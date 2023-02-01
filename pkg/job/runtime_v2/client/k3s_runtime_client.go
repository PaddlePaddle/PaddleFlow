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

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	infov1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
)

type K3SRuntimeClient struct {
	Client          kubernetes.Interface
	InformerFactory informers.SharedInformerFactory
	ClusterInfo     *pfschema.Cluster
	// DynamicClient dynamic client
	DynamicClient  dynamic.Interface
	DynamicFactory dynamicinformer.DynamicSharedInformerFactory
	Config         *rest.Config
	// nodeInformer contains the informer of node
	nodeInformer infov1.NodeInformer
	// nodeTaskInformer contains the informer of node task
	nodeTaskInformer infov1.PodInformer
	// JobInformerMap contains GroupVersionKind and informer for different kubernetes job
	JobInformerMap map[schema.GroupVersionResource]cache.SharedIndexInformer
	// podInformer contains the informer of task
	podInformer cache.SharedIndexInformer
	taskClient  framework.JobInterface
}

func (k3s *K3SRuntimeClient) Cluster() string {
	if k3s.ClusterInfo != nil {
		return fmt.Sprintf("cluster %s with type %s", k3s.ClusterInfo.Name, k3s.ClusterInfo.Type)
	}
	// default values
	return fmt.Sprintf("cluster %s with type %s", "K3S", "SingleNode")
}
func (k3s *K3SRuntimeClient) ClusterID() string {
	if k3s.ClusterInfo != nil {
		return k3s.ClusterInfo.ID
	}
	return "defaultK3SID"
}

func (k3s *K3SRuntimeClient) ClusterName() string {
	if k3s.ClusterInfo != nil {
		return k3s.ClusterInfo.Name
	}
	// default return the only node name
	nodes, err := k3s.Client.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil || len(nodes.Items) != 1 {
		return fmt.Sprintf("K3S Get Node Err %v, nodeLen %v ", err, len(nodes.Items))
	}
	return nodes.Items[0].Name
}

func (k3s *K3SRuntimeClient) Get(namespace string, name string, fv pfschema.FrameworkVersion) (interface{}, error) {
	gvr := k8s.GetJobGVR(pfschema.Framework(fv.Framework))
	var obj *unstructured.Unstructured
	// 操作pod相关资源默认用namespaced scope
	obj, err := k3s.DynamicClient.Resource(gvr).Namespace(namespace).Get(context.TODO(), name, v1.GetOptions{})

	if err != nil {
		log.Errorf("get k3s %s resource[%s/%s] failed. error:[%s]", gvr.String(), namespace, name, err.Error())
	}
	return obj, err
}

func (k3s *K3SRuntimeClient) Create(resource interface{}, fv pfschema.FrameworkVersion) error {
	gvr := k8s.GetJobGVR(pfschema.Framework(fv.Framework))
	log.Debugf("executor begin to create k3s resource[%s]", gvr.String())
	if k3s == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	newResource, err := runtime.DefaultUnstructuredConverter.ToUnstructured(resource)
	if err != nil {
		log.Errorf("k3s unstruct resource err: %v", err)
		return err
	}
	obj := &unstructured.Unstructured{
		Object: newResource,
	}
	// Create the object with dynamic client
	_, err = k3s.DynamicClient.Resource(gvr).Namespace(obj.GetNamespace()).Create(context.TODO(), obj, v1.CreateOptions{})
	if err != nil {
		log.Errorf("create k3s resource[%s] failed. error:[%s]", gvr.String(), err.Error())
	}
	return err
}

func (k3s *K3SRuntimeClient) Delete(namespace string, name string, fv pfschema.FrameworkVersion) error {
	gvr := k8s.GetJobGVR(pfschema.Framework(fv.Framework))
	log.Debugf("executor begin to delete k3s resource[%s]. ns:[%s] name:[%s]", gvr.String(), namespace, name)
	if k3s == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	propagationPolicy := v1.DeletePropagationBackground
	deleteOptions := v1.DeleteOptions{
		PropagationPolicy: &propagationPolicy,
	}

	err := k3s.DynamicClient.Resource(gvr).Namespace(namespace).Delete(context.TODO(), name, deleteOptions)

	if err != nil {
		log.Errorf("delete k3s resource[%s] failed. error:[%s]", gvr.String(), err.Error())
	}
	return err
}

func (k3s *K3SRuntimeClient) Patch(namespace, name string, fv pfschema.FrameworkVersion, data []byte) error {
	gvr := k8s.GetJobGVR(pfschema.Framework(fv.Framework))
	log.Debugf("executor begin to patch k3s resource[%s]. ns:[%s] name:[%s]", gvr.String(), namespace, name)
	if k3s == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	patchType := types.StrategicMergePatchType
	patchOptions := v1.PatchOptions{}
	_, err := k3s.DynamicClient.Resource(gvr).Namespace(namespace).Patch(context.TODO(), name, patchType, data, patchOptions)

	if err != nil {
		log.Errorf("patch k3s resource: %s failed. error: %s", gvr.String(), err.Error())
	}
	return err
}

func (k3s *K3SRuntimeClient) Update(resource interface{}, fv pfschema.FrameworkVersion) error {
	gvr := k8s.GetJobGVR(pfschema.Framework(fv.Framework))
	log.Debugf("executor begin to update k3s resource[%s]", gvr.String())
	if k3s.DynamicClient == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	newResource, err := runtime.DefaultUnstructuredConverter.ToUnstructured(resource)
	if err != nil {
		log.Errorf("convert to unstructured failed, err: %v", err)
		return err
	}

	obj := &unstructured.Unstructured{
		Object: newResource,
	}
	// Create the object with dynamic client
	_, err = k3s.DynamicClient.Resource(gvr).Namespace(obj.GetNamespace()).Update(context.TODO(), obj, v1.UpdateOptions{})

	if err != nil {
		log.Errorf("update kuberentes resource[%s] failed. error:[%s]", gvr.String(), err.Error())
	}
	return err
}

// RegisterListener register single job(task) listener
func (k3s *K3SRuntimeClient) RegisterListener(listenerType string, workQueue workqueue.RateLimitingInterface) error {
	log.Infof("k3s listener type is %v", listenerType)
	var err error
	switch listenerType {
	case pfschema.ListenerTypeJob:
		err = k3s.registerJobListener(workQueue)
	case pfschema.ListenerTypeTask:
		err = k3s.registerTaskListener(workQueue)
	case pfschema.ListenerTypeNode:
		err = k3s.registerNodeListener(workQueue)
	case pfschema.ListenerTypeNodeTask:
		err = k3s.registerNodeTaskListener(workQueue)
	default:
		err = fmt.Errorf("listener type %s is not supported", listenerType)
	}
	return err
}

func (k3s *K3SRuntimeClient) registerJobListener(workQueue workqueue.RateLimitingInterface) error {
	log.Infof("Register single job and task listener")
	//  add job informer
	taskGvr := k8s.PodGVR
	k3s.JobInformerMap[taskGvr] = k3s.DynamicFactory.ForResource(taskGvr).Informer()
	gvk := k8s.PodGVK
	jobPlugin, _ := framework.GetJobPlugin(pfschema.K3SType, KubeFrameworkVersion(gvk))
	jobClient := jobPlugin(k3s)
	err := jobClient.AddEventListener(context.TODO(), pfschema.ListenerTypeJob, workQueue, k3s.JobInformerMap[taskGvr])
	if err != nil {
		log.Errorf("k3s job on %s, add event lister for job %s failed, err: %v", k3s.Cluster(), gvk.String(), err)
		return err
	}
	k3s.taskClient = jobClient
	return nil
}

func (k3s *K3SRuntimeClient) registerTaskListener(workQueue workqueue.RateLimitingInterface) error {
	// add pod informer
	taskGvr := k8s.PodGVR
	k3s.podInformer = k3s.DynamicFactory.ForResource(taskGvr).Informer()
	// register single job plugin and reuse the job plugin instance for task
	// only support single job(pods resource)
	// Register task event listener
	err := k3s.taskClient.AddEventListener(context.TODO(), pfschema.ListenerTypeTask, workQueue, k3s.JobInformerMap[taskGvr])
	if err != nil {
		log.Errorf("k3s task on %s, add event lister for task failed, err: %v", k3s.Cluster(), err)
		return err
	}
	return nil
}

func (k3s *K3SRuntimeClient) registerNodeListener(workQueue workqueue.RateLimitingInterface) error {
	k3s.nodeInformer = k3s.InformerFactory.Core().V1().Nodes()
	// 这部分看起来先复用就行，因此k3s只有一个节点，监听变化的意义不是很大
	nodeHandler := NewNodeHandler(workQueue, k3s.Cluster())
	k3s.nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    nodeHandler.AddNode,
		UpdateFunc: nodeHandler.UpdateNode,
		DeleteFunc: nodeHandler.DeleteNode,
	})
	return nil
}

func (k3s *K3SRuntimeClient) registerNodeTaskListener(workQueue workqueue.RateLimitingInterface) error {
	k3s.nodeTaskInformer = k3s.InformerFactory.Core().V1().Pods()

	taskHandler := NewNodeTaskHandler(workQueue, k3s.Cluster())
	k3s.nodeTaskInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			// TODO: evaluate the time of filter
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return false
			}
			// check weather pod resources is empty or not
			podResource := GetPodResource(pod)
			if podResource.IsZero() {
				log.Debugf("node task %s/%s request resources is empty, ignore it", pod.Namespace, pod.Name)
				return false
			}
			return true
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    taskHandler.AddPod,
			UpdateFunc: taskHandler.UpdatePod,
			DeleteFunc: taskHandler.DeletePod,
		},
	})
	return nil
}

func (k3s *K3SRuntimeClient) StartListener(listenerType string, stopCh <-chan struct{}) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeNode, pfschema.ListenerTypeNodeTask:
		k3s.InformerFactory.Start(stopCh)
		for _, synced := range k3s.InformerFactory.WaitForCacheSync(stopCh) {
			if !synced {
				err = fmt.Errorf("timed out waiting for caches to %s", listenerType)
				log.Errorf("on %s, start %s listener failed, err: %v", k3s.Cluster(), listenerType, err)
				break
			}
		}
	default:
		err = k3s.startDynamicListener(listenerType, stopCh)
	}
	return err
}

func (k3s *K3SRuntimeClient) startDynamicListener(listenerType string, stopCh <-chan struct{}) error {
	var err error
	var informerMap = make(map[schema.GroupVersionResource]cache.SharedIndexInformer)
	switch listenerType {
	case pfschema.ListenerTypeJob, pfschema.ListenerTypeTask:
		informerMap = k3s.JobInformerMap
		informerMap[k8s.PodGVR] = k3s.podInformer
	default:
		err = fmt.Errorf("listener type %s is not supported", listenerType)
	}
	if err != nil {
		log.Errorf("on %s, start %s listener failed, err: %v", k3s.Cluster(), listenerType, err)
		return err
	}
	// start dynamic factory and wait for cache sync
	k3s.DynamicFactory.Start(stopCh)
	for _, informer := range informerMap {
		if !cache.WaitForCacheSync(stopCh, informer.HasSynced) {
			err = fmt.Errorf("timed out waiting for caches to %s", k3s.Cluster())
			log.Errorf("on %s, start %s listener failed, err: %v", k3s.Cluster(), listenerType, err)
			break
		}
	}
	return err
}

// ListNodeQuota resource api for cluster nodes
func (k3s *K3SRuntimeClient) ListNodeQuota(ctx context.Context) (pfschema.QuotaSummary, []pfschema.NodeQuotaInfo, error) {
	return pfschema.QuotaSummary{}, nil, nil
}

func (k3s *K3SRuntimeClient) GetJobTypeFramework(fv pfschema.FrameworkVersion) (pfschema.JobType, pfschema.Framework) {
	gvk := frameworkVersionToGVK(fv)
	return k8s.GetJobTypeAndFramework(gvk)
}

func (k3s *K3SRuntimeClient) JobFrameworkVersion(jobType pfschema.JobType, fw pfschema.Framework) pfschema.FrameworkVersion {
	frameworkVersion := k8s.GetJobFrameworkVersion(jobType, fw)
	log.Infof("on %s, FrameworkVesion for job type %s framework %s, is %s", k3s.Cluster(), jobType, fw, frameworkVersion)
	return frameworkVersion
}

func (k3s *K3SRuntimeClient) GetTaskLogV2(namespace, name string, logPage utils.LogPage) ([]pfschema.TaskLogInfo, error) {
	return getTaskLogV2(k3s.Client, namespace, name, logPage)
}

func (k3s *K3SRuntimeClient) GetTaskLog(namespace, name, logFilePosition string, pageSize, pageNo int) ([]pfschema.TaskLogInfo, error) {
	return getTaskLog(k3s.Client, namespace, name, logFilePosition, pageSize, pageNo)
}
