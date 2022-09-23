/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
)

var (
	// maximum number of lines loaded from the apiserver
	lineReadLimit int64 = 5000
	// maximum number of bytes loaded from the apiserver
	byteReadLimit int64 = 500000
	// TaskGVK gvk for task
	TaskGVK = k8s.PodGVK
)

// KubeRuntimeClient for kubernetes client
type KubeRuntimeClient struct {
	Client kubernetes.Interface
	// DynamicClient dynamic client
	DynamicClient  dynamic.Interface
	DynamicFactory dynamicinformer.DynamicSharedInformerFactory
	// DiscoveryClient client for discovery
	DiscoveryClient discovery.DiscoveryInterface
	// TODO: adjust field Config
	Config      *rest.Config
	ClusterInfo *pfschema.Cluster
	// GVKToGVR contains GroupVersionKind map to GroupVersionResource
	GVKToGVR sync.Map

	// JobInformerMap contains GroupVersionKind and informer for different kubernetes job
	JobInformerMap map[schema.GroupVersionKind]cache.SharedIndexInformer
	// podInformer contains the informer of task
	podInformer cache.SharedIndexInformer
	taskClient  framework.JobInterface
	// QueueInformerMap
	QueueInformerMap map[schema.GroupVersionKind]cache.SharedIndexInformer
}

func CreateKubeRuntimeClient(config *rest.Config, cluster *pfschema.Cluster) (framework.RuntimeClientInterface, error) {
	// new kubernetes typed client
	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Errorf("create kubernetes client failed, err: %v", err)
		return nil, err
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Errorf("init dynamic client failed. error:%s", err)
		return nil, err
	}
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0)
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		log.Errorf("create discovery client failed: %v", err)
		return nil, err
	}
	if cluster == nil {
		log.Errorf("cluster info is nil")
		return nil, fmt.Errorf("cluster info is nil")
	}
	return &KubeRuntimeClient{
		Client:           k8sClient,
		DynamicClient:    dynamicClient,
		DynamicFactory:   factory,
		DiscoveryClient:  discoveryClient,
		Config:           config,
		ClusterInfo:      cluster,
		JobInformerMap:   make(map[schema.GroupVersionKind]cache.SharedIndexInformer),
		QueueInformerMap: make(map[schema.GroupVersionKind]cache.SharedIndexInformer),
	}, nil
}

func frameworkVersionToGVK(fv pfschema.FrameworkVersion) schema.GroupVersionKind {
	return schema.FromAPIVersionAndKind(fv.APIVersion, fv.Framework)
}

func KubeFrameworkVersion(gvk schema.GroupVersionKind) pfschema.FrameworkVersion {
	return pfschema.NewFrameworkVersion(gvk.Kind, gvk.GroupVersion().String())
}

func (krc *KubeRuntimeClient) JobFrameworkVersion(jobType pfschema.JobType, fw pfschema.Framework) pfschema.FrameworkVersion {
	frameworkVersion := k8s.GetJobFrameworkVersion(jobType, fw)
	log.Infof("on %s, FrameworkVesion for job type %s framework %s, is %s", krc.Cluster(), jobType, fw, frameworkVersion)
	return frameworkVersion
}

func (krc *KubeRuntimeClient) GetJobTypeFramework(fv pfschema.FrameworkVersion) (pfschema.JobType, pfschema.Framework) {
	gvk := frameworkVersionToGVK(fv)
	jobType, framework := k8s.GetJobTypeAndFramework(gvk)
	return jobType, framework
}

func (krc *KubeRuntimeClient) RegisterListener(listenerType string, workQueue workqueue.RateLimitingInterface) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeJob:
		err = krc.registerJobListener(workQueue)
	case pfschema.ListenerTypeTask:
		err = krc.registerTaskListener(workQueue)
	case pfschema.ListenerTypeQueue:
		err = krc.registerQueueListener(workQueue)
	default:
		err = fmt.Errorf("listener type %s is not supported", listenerType)
	}
	return err
}

func (krc *KubeRuntimeClient) registerJobListener(workQueue workqueue.RateLimitingInterface) error {
	jobPlugins := framework.ListJobPlugins(pfschema.KubernetesType)
	if len(jobPlugins) == 0 {
		return fmt.Errorf("register job Listener failed, err: job plugins is nil")
	}
	for fv, jobPlugin := range jobPlugins {
		gvk := frameworkVersionToGVK(fv)
		gvrMap, err := krc.GetGVR(gvk)
		if err != nil {
			log.Warnf("on %s, cann't find GroupVersionKind %s, err: %v", krc.Cluster(), gvk.String(), err)
		} else {
			// Register job event listener
			log.Infof("on %s, register job event listener for %s", krc.Cluster(), gvk.String())
			krc.JobInformerMap[gvk] = krc.DynamicFactory.ForResource(gvrMap.Resource).Informer()
			jobClient := jobPlugin(krc)
			err = jobClient.AddEventListener(context.TODO(), pfschema.ListenerTypeJob, workQueue, krc.JobInformerMap[gvk])
			if err != nil {
				log.Warnf("on %s, add event lister for job %s failed, err: %v", krc.Cluster(), gvk.String(), err)
				continue
			}
			// Register task event listener
			if gvk == TaskGVK {
				krc.taskClient = jobClient
			}
		}
	}
	return nil
}

func (krc *KubeRuntimeClient) registerTaskListener(workQueue workqueue.RateLimitingInterface) error {
	gvrMap, err := krc.GetGVR(TaskGVK)
	if err != nil {
		log.Warnf("on %s, cann't find task GroupVersionKind %s, err: %v", krc.Cluster(), TaskGVK.String(), err)
		return err
	}
	krc.podInformer = krc.DynamicFactory.ForResource(gvrMap.Resource).Informer()
	taskInformer, find := krc.JobInformerMap[TaskGVK]
	if !find {
		err = fmt.Errorf("register task listener failed, taskClient is nil")
		log.Errorf("on %s, %s", krc.Cluster(), err)
		return err
	}
	if krc.taskClient == nil {
		err = fmt.Errorf("register task listener failed, taskClient is nil")
		log.Errorf("on %s, %s", krc.Cluster(), err)
		return err
	}
	// Register task event listener
	err = krc.taskClient.AddEventListener(context.TODO(), pfschema.ListenerTypeTask, workQueue, taskInformer)
	if err != nil {
		log.Errorf("on %s, add event lister for task failed, err: %v", krc.Cluster(), err)
		return err
	}
	return nil
}

func (krc *KubeRuntimeClient) registerQueueListener(workQueue workqueue.RateLimitingInterface) error {
	queuePlugins := framework.ListQueuePlugins(pfschema.KubernetesType)
	if len(queuePlugins) == 0 {
		return fmt.Errorf("on %s, register queue listener failed, err: plugins is nil", krc.Cluster())
	}
	for fv, plugin := range queuePlugins {
		gvk := frameworkVersionToGVK(fv)
		gvrMap, err := krc.GetGVR(gvk)
		if err != nil {
			log.Warnf("on %s, cann't find GroupVersionKind %s, err: %v", krc.Cluster(), gvk.String(), err)
		} else {
			// Register queue event listener
			log.Infof("on %s, register queue event listener for %s", krc.Cluster(), gvk.String())
			krc.QueueInformerMap[gvk] = krc.DynamicFactory.ForResource(gvrMap.Resource).Informer()
			queueClient := plugin(krc)
			err = queueClient.AddEventListener(context.TODO(), pfschema.ListenerTypeQueue, workQueue, krc.QueueInformerMap[gvk])
			if err != nil {
				log.Warnf("on %s, add event lister for queue %s failed, err: %v", krc.Cluster(), gvk.String(), err)
				continue
			}
		}
	}
	return nil
}

func (krc *KubeRuntimeClient) StartListener(listenerType string, stopCh <-chan struct{}) error {
	var err error
	var informerMap = make(map[schema.GroupVersionKind]cache.SharedIndexInformer)
	switch listenerType {
	case pfschema.ListenerTypeJob:
		informerMap = krc.JobInformerMap
	case pfschema.ListenerTypeTask:
		informerMap[TaskGVK] = krc.podInformer
	case pfschema.ListenerTypeQueue:
		informerMap = krc.QueueInformerMap
	default:
		err = fmt.Errorf("listener type %s is not supported", listenerType)
	}
	if err != nil {
		log.Errorf("on %s, start %s listener failed, err: %v", krc.Cluster(), listenerType, err)
		return err
	}
	// start dynamic factory and wait for cache sync
	go krc.DynamicFactory.Start(stopCh)
	for _, informer := range informerMap {
		if !cache.WaitForCacheSync(stopCh, informer.HasSynced) {
			err = fmt.Errorf("timed out waiting for caches to %s", krc.Cluster())
			log.Errorf("on %s, start %s listener failed, err: %v", krc.Cluster(), listenerType, err)
			break
		}
	}
	return err
}

func (krc *KubeRuntimeClient) Cluster() string {
	msg := ""
	if krc.ClusterInfo != nil {
		msg = fmt.Sprintf("cluster %s with type %s", krc.ClusterInfo.Name, krc.ClusterInfo.Type)
	}
	return msg
}

func (krc *KubeRuntimeClient) ClusterID() string {
	clusterID := ""
	if krc.ClusterInfo != nil {
		clusterID = krc.ClusterInfo.ID
	}
	return clusterID
}

func (krc *KubeRuntimeClient) ListNodeQuota(ctx context.Context) (pfschema.QuotaSummary, []pfschema.NodeQuotaInfo, error) {
	// TODO: add ListNodeQuota logic
	return pfschema.QuotaSummary{}, []pfschema.NodeQuotaInfo{}, nil
}

func (krc *KubeRuntimeClient) GetGVR(gvk schema.GroupVersionKind) (meta.RESTMapping, error) {
	gvr, ok := krc.GVKToGVR.Load(gvk.String())
	if ok {
		return gvr.(meta.RESTMapping), nil
	}
	return krc.findGVR(&gvk)
}

func (krc *KubeRuntimeClient) findGVR(gvk *schema.GroupVersionKind) (meta.RESTMapping, error) {
	// DiscoveryClient queries API server about the resources
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(krc.DiscoveryClient))
	// Find GVR
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		log.Warningf("find GVR with restMapping failed: %v", err)
		return meta.RESTMapping{}, err
	}
	// Store GVR
	log.Debugf("The GVR of GVK[%s] is [%s]", gvk.String(), mapping.Resource.String())
	krc.GVKToGVR.Store(gvk.String(), *mapping)
	return *mapping, nil
}

func (krc *KubeRuntimeClient) Get(namespace string, name string, fv pfschema.FrameworkVersion) (interface{}, error) {
	gvk := frameworkVersionToGVK(fv)
	log.Debugf("executor begin to get kubernetes resource[%s]. ns:[%s] name:[%s]", gvk.String(), namespace, name)
	if krc == nil {
		return nil, fmt.Errorf("dynamic client is nil")
	}
	gvrMap, err := krc.GetGVR(gvk)
	if err != nil {
		return nil, err
	}
	var obj *unstructured.Unstructured
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		obj, err = krc.DynamicClient.Resource(gvrMap.Resource).Namespace(namespace).Get(context.TODO(), name, v1.GetOptions{})
	} else {
		obj, err = krc.DynamicClient.Resource(gvrMap.Resource).Get(context.TODO(), name, v1.GetOptions{})
	}
	if err != nil {
		log.Errorf("get kubernetes %s resource[%s/%s] failed. error:[%s]", gvk.String(), namespace, name, err.Error())
	}
	return obj, err
}

func (krc *KubeRuntimeClient) Create(resource interface{}, fv pfschema.FrameworkVersion) error {
	gvk := frameworkVersionToGVK(fv)
	log.Debugf("executor begin to create kuberentes resource[%s]", gvk.String())
	if krc == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	gvrMap, err := krc.GetGVR(gvk)
	if err != nil {
		return err
	}

	newResource, err := runtime.DefaultUnstructuredConverter.ToUnstructured(resource)
	if err != nil {
		return err
	}

	obj := &unstructured.Unstructured{
		Object: newResource,
	}
	obj.SetKind(gvk.Kind)
	obj.SetAPIVersion(gvk.GroupVersion().String())
	// Create the object with dynamic client
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		_, err = krc.DynamicClient.Resource(gvrMap.Resource).Namespace(obj.GetNamespace()).Create(context.TODO(), obj, v1.CreateOptions{})
	} else {
		_, err = krc.DynamicClient.Resource(gvrMap.Resource).Create(context.TODO(), obj, v1.CreateOptions{})
	}
	if err != nil {
		log.Errorf("create kuberentes resource[%s] failed. error:[%s]", gvk.String(), err.Error())
	}
	return err
}

func (krc *KubeRuntimeClient) Delete(namespace string, name string, fv pfschema.FrameworkVersion) error {
	gvk := frameworkVersionToGVK(fv)
	log.Debugf("executor begin to delete kubernetes resource[%s]. ns:[%s] name:[%s]", gvk.String(), namespace, name)
	if krc == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	propagationPolicy := v1.DeletePropagationBackground
	deleteOptions := v1.DeleteOptions{
		PropagationPolicy: &propagationPolicy,
	}
	gvrMap, err := krc.GetGVR(gvk)
	if err != nil {
		return err
	}
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		err = krc.DynamicClient.Resource(gvrMap.Resource).Namespace(namespace).Delete(context.TODO(), name, deleteOptions)
	} else {
		err = krc.DynamicClient.Resource(gvrMap.Resource).Delete(context.TODO(), name, deleteOptions)
	}
	if err != nil {
		log.Errorf("delete kubernetes  resource[%s] failed. error:[%s]", gvk.String(), err.Error())
	}
	return err
}

func (krc *KubeRuntimeClient) Patch(namespace, name string, fv pfschema.FrameworkVersion, data []byte) error {
	gvk := frameworkVersionToGVK(fv)
	log.Debugf("executor begin to patch kubernetes resource[%s]. ns:[%s] name:[%s]", gvk.String(), namespace, name)
	if krc == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	patchType := types.StrategicMergePatchType
	patchOptions := v1.PatchOptions{}
	gvrMap, err := krc.GetGVR(gvk)
	if err != nil {
		return err
	}
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		_, err = krc.DynamicClient.Resource(gvrMap.Resource).Namespace(namespace).Patch(context.TODO(), name, patchType, data, patchOptions)
	} else {
		_, err = krc.DynamicClient.Resource(gvrMap.Resource).Patch(context.TODO(), name, patchType, data, patchOptions)
	}
	if err != nil {
		log.Errorf("patch kubernetes resource: %s failed. error: %s", gvk.String(), err.Error())
	}
	return err
}

func (krc *KubeRuntimeClient) Update(resource interface{}, fv pfschema.FrameworkVersion) error {
	gvk := frameworkVersionToGVK(fv)
	log.Debugf("executor begin to update kubernetes resource[%s]", gvk.String())
	if krc == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	gvrMap, err := krc.GetGVR(gvk)
	if err != nil {
		return err
	}

	newResource, err := runtime.DefaultUnstructuredConverter.ToUnstructured(resource)
	if err != nil {
		log.Errorf("convert to unstructured failed, err: %v", err)
		return err
	}

	obj := &unstructured.Unstructured{
		Object: newResource,
	}
	obj.SetKind(gvk.Kind)
	obj.SetAPIVersion(gvk.GroupVersion().String())
	// Create the object with dynamic client
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		_, err = krc.DynamicClient.Resource(gvrMap.Resource).Namespace(obj.GetNamespace()).Update(context.TODO(), obj, v1.UpdateOptions{})
	} else {
		_, err = krc.DynamicClient.Resource(gvrMap.Resource).Update(context.TODO(), obj, v1.UpdateOptions{})
	}
	if err != nil {
		log.Errorf("update kuberentes resource[%s] failed. error:[%s]", gvk.String(), err.Error())
	}
	return err
}

func (krc *KubeRuntimeClient) GetTaskLog(namespace, name, logFilePosition string, pageSize, pageNo int) ([]pfschema.TaskLogInfo, error) {
	taskLogInfoList := make([]pfschema.TaskLogInfo, 0)
	pod, err := krc.Client.CoreV1().Pods(namespace).Get(context.TODO(), name, v1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return []pfschema.TaskLogInfo{}, nil
	} else if err != nil {
		return []pfschema.TaskLogInfo{}, err
	}
	for _, c := range pod.Spec.Containers {
		podLogOptions := mapToLogOptions(c.Name, logFilePosition)
		logContent, length, err := krc.getContainerLog(namespace, name, podLogOptions)
		if err != nil {
			return []pfschema.TaskLogInfo{}, err
		}
		startIndex := -1
		endIndex := -1
		hasNextPage := false
		truncated := false
		limitFlag := isReadLimitReached(int64(len(logContent)), int64(length), logFilePosition)
		overFlag := false
		// 判断开始位置是否已超过日志总行数，若超过overFlag为true；
		// 如果是logFilePPosition为end，则看下startIndex是否已经超过0，若超过则置startIndex为-1（从最开始获取），并检查日志是否被截断
		// 如果是logFilePPosition为begin，则判断末尾index是否超过总长度，若超过endIndex为-1（直到末尾），并检查日志是否被截断
		if (pageNo-1)*pageSize+1 <= length {
			switch logFilePosition {
			case common.EndFilePosition:
				startIndex = length - pageSize*pageNo
				endIndex = length - (pageNo-1)*pageSize
				if startIndex <= 0 {
					startIndex = -1
					truncated = limitFlag
				} else {
					hasNextPage = true
				}
				if endIndex == length {
					endIndex = -1
				}
			case common.BeginFilePosition:
				startIndex = (pageNo - 1) * pageSize
				if pageNo*pageSize < length {
					endIndex = pageNo * pageSize
					hasNextPage = true
				} else {
					truncated = limitFlag
				}
			}
		} else {
			overFlag = true
		}

		taskLogInfo := pfschema.TaskLogInfo{
			TaskID: fmt.Sprintf("%s_%s", pod.GetUID(), c.Name),
			Info: pfschema.LogInfo{
				LogContent:  splitLog(logContent, startIndex, endIndex, overFlag),
				HasNextPage: hasNextPage,
				Truncated:   truncated,
			},
		}
		taskLogInfoList = append(taskLogInfoList, taskLogInfo)
	}
	return taskLogInfoList, nil

}

func (krc *KubeRuntimeClient) getContainerLog(namespace, name string, logOptions *corev1.PodLogOptions) (string, int, error) {
	readCloser, err := krc.Client.CoreV1().RESTClient().Get().
		Namespace(namespace).
		Name(name).
		Resource("pods").
		SubResource("log").
		VersionedParams(logOptions, scheme.ParameterCodec).Stream(context.TODO())
	if err != nil {
		log.Errorf("pod[%s] get log stream failed. error: %s", name, err.Error())
		return err.Error(), 0, nil
	}

	defer readCloser.Close()

	// logOptions: begin LimitBytes 500000; end TailLines 5000
	result, err := io.ReadAll(readCloser)
	if err != nil {
		log.Errorf("pod[%s] read content failed; error: %s", name, err.Error())
		return "", 0, err
	}

	return string(result), len(strings.Split(strings.TrimRight(string(result), "\n"), "\n")), nil
}

func mapToLogOptions(container, logFilePosition string) *corev1.PodLogOptions {
	logOptions := &corev1.PodLogOptions{
		Container:  container,
		Follow:     false,
		Timestamps: true,
	}

	if logFilePosition == common.BeginFilePosition {
		logOptions.LimitBytes = &byteReadLimit
	} else {
		logOptions.TailLines = &lineReadLimit
	}

	return logOptions
}

func splitLog(logContent string, startIndex, endIndex int, overFlag bool) string {
	if overFlag || logContent == "" {
		return ""
	}
	logContent = strings.TrimRight(logContent, "\n")
	var logLines []string
	if startIndex == -1 && endIndex == -1 {
		logLines = strings.Split(logContent, "\n")[:]
	} else if startIndex == -1 {
		logLines = strings.Split(logContent, "\n")[:endIndex]
	} else if endIndex == -1 {
		logLines = strings.Split(logContent, "\n")[startIndex:]
	} else {
		logLines = strings.Split(logContent, "\n")[startIndex:endIndex]
	}
	return strings.Join(logLines, "\n") + "\n"
}

func isReadLimitReached(bytesLoaded int64, linesLoaded int64, logFilePosition string) bool {
	return (logFilePosition == common.BeginFilePosition && bytesLoaded >= byteReadLimit) ||
		(logFilePosition == common.EndFilePosition && linesLoaded >= lineReadLimit)
}
