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
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

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
	"k8s.io/client-go/informers"
	infov1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

var (
	// maximum number of lines loaded from the apiserver
	lineReadLimit int64 = 5000
	// maximum number of bytes loaded from the apiserver
	byteReadLimit int64 = 500000
	// SyncJobPluginsPeriod defines how often to sync jobPlugins
	SyncJobPluginsPeriod int = 5
	// TaskGVK gvk for task
	TaskGVK = k8s.PodGVK
)

// KubeRuntimeClient for kubernetes client
type KubeRuntimeClient struct {
	Client          kubernetes.Interface
	InformerFactory informers.SharedInformerFactory
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

	// nodeInformer contains the informer of node
	nodeInformer infov1.NodeInformer
	// nodeTaskInformer contains the informer of node task
	nodeTaskInformer infov1.PodInformer
	// JobInformerMap contains GroupVersionKind and informer for different kubernetes job
	JobInformerMap map[schema.GroupVersionKind]cache.SharedIndexInformer
	// UnRegisteredMap record unregistered GroupVersionKind
	UnRegisteredMap map[schema.GroupVersionKind]bool
	// podInformer contains the informer of task
	podInformer     cache.SharedIndexInformer
	taskClient      framework.JobInterface
	taskClientReady chan int
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
		InformerFactory:  informers.NewSharedInformerFactory(k8sClient, 0),
		DynamicClient:    dynamicClient,
		DynamicFactory:   factory,
		DiscoveryClient:  discoveryClient,
		Config:           config,
		ClusterInfo:      cluster,
		taskClientReady:  make(chan int),
		JobInformerMap:   make(map[schema.GroupVersionKind]cache.SharedIndexInformer),
		UnRegisteredMap:  make(map[schema.GroupVersionKind]bool),
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
	case pfschema.ListenerTypeNode:
		err = krc.registerNodeListener(workQueue)
	case pfschema.ListenerTypeNodeTask:
		err = krc.registerNodeTaskListener(workQueue)
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
	gvkPlugins := make(map[schema.GroupVersionKind]framework.JobPlugin)
	for fv, jobPlugin := range jobPlugins {
		gvk := frameworkVersionToGVK(fv)
		krc.UnRegisteredMap[gvk] = true
		gvkPlugins[gvk] = jobPlugin
	}
	go krc.AddJobInformerMaps(gvkPlugins, workQueue)
	return nil
}

func (krc *KubeRuntimeClient) AddJobInformerMaps(gvkPlugins map[schema.GroupVersionKind]framework.JobPlugin, workQueue workqueue.RateLimitingInterface) {
	for len(krc.UnRegisteredMap) != 0 {
		for gvk, _ := range krc.UnRegisteredMap {
			gvrMap, err := krc.GetGVR(gvk)
			if err != nil {
				continue
			} else {
				// Register job event listener
				log.Infof("on %s, register job event listener for %s", krc.Cluster(), gvk.String())
				krc.JobInformerMap[gvk] = krc.DynamicFactory.ForResource(gvrMap.Resource).Informer()
				jobPlugin := gvkPlugins[gvk]
				jobClient := jobPlugin(krc)
				err = jobClient.AddEventListener(context.TODO(), pfschema.ListenerTypeJob, workQueue, krc.JobInformerMap[gvk])
				if err != nil {
					log.Warnf("on %s, add event lister for job %s failed, err: %v", krc.Cluster(), gvk.String(), err)
					continue
				}
				// Register task event listener
				if gvk == TaskGVK {
					krc.taskClient = jobClient
					krc.taskClientReady <- 0
				}
				delete(krc.UnRegisteredMap, gvk)
			}
		}
		time.Sleep(time.Duration(SyncJobPluginsPeriod) * time.Second)
	}
}

func (krc *KubeRuntimeClient) registerTaskListener(workQueue workqueue.RateLimitingInterface) error {
	<-krc.taskClientReady
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

func (krc *KubeRuntimeClient) registerNodeListener(workQueue workqueue.RateLimitingInterface) error {
	krc.nodeInformer = krc.InformerFactory.Core().V1().Nodes()
	nodeHandler := NewNodeHandler(workQueue, krc.Cluster())
	krc.nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    nodeHandler.AddNode,
		UpdateFunc: nodeHandler.UpdateNode,
		DeleteFunc: nodeHandler.DeleteNode,
	})
	return nil
}

func (krc *KubeRuntimeClient) registerNodeTaskListener(workQueue workqueue.RateLimitingInterface) error {
	krc.nodeTaskInformer = krc.InformerFactory.Core().V1().Pods()

	taskHandler := NewNodeTaskHandler(workQueue, krc.Cluster())
	krc.nodeTaskInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
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

func GetPodResource(pod *corev1.Pod) *resources.Resource {
	result := resources.EmptyResource()
	for _, container := range pod.Spec.Containers {
		result.Add(k8s.NewResource(container.Resources.Requests))
	}
	return result
}

type NodeHandler struct {
	workQueue      workqueue.RateLimitingInterface
	labelKeys      []string
	cluster        string
	resourceFilter []string
}

func NewNodeHandler(q workqueue.RateLimitingInterface, cluster string) *NodeHandler {
	var labelKeys []string
	nodeLabels := strings.TrimSpace(os.Getenv(pfschema.EnvPFNodeLabels))
	if len(nodeLabels) > 0 {
		labelKeys = strings.Split(nodeLabels, ",")
	} else {
		labelKeys = []string{pfschema.PFNodeLabels}
	}
	var rFilter = []string{
		"pods",
	}
	resourceFilters := strings.TrimSpace(os.Getenv(pfschema.EnvPFResourceFilter))
	if len(resourceFilters) > 0 {
		filters := strings.Split(resourceFilters, ",")
		rFilter = append(rFilter, filters...)
	}
	return &NodeHandler{
		workQueue:      q,
		labelKeys:      labelKeys,
		cluster:        cluster,
		resourceFilter: rFilter,
	}
}

func (n *NodeHandler) isExpectedResources(rName string) bool {
	var isExpected = true
	for _, filter := range n.resourceFilter {
		if strings.Contains(rName, filter) {
			isExpected = false
			break
		}
	}
	return isExpected
}

func (n *NodeHandler) addQueue(node *corev1.Node, action pfschema.ActionType, labels map[string]string) {
	capacity := make(map[string]string)
	for rName, rValue := range node.Status.Allocatable {
		resourceName := string(rName)
		if n.isExpectedResources(resourceName) {
			capacity[resourceName] = rValue.String()
		}
	}
	nodeSync := &api.NodeSyncInfo{
		Name:     node.Name,
		Status:   getNodeStatus(node),
		Capacity: capacity,
		Labels:   labels,
		Action:   action,
	}
	log.Infof("WatchNodeSync: %s, watch %s event for node %s with status %s", n.cluster, action, node.Name, nodeSync.Status)
	n.workQueue.Add(nodeSync)
}

func (n *NodeHandler) AddNode(obj interface{}) {
	node := obj.(*corev1.Node)
	n.addQueue(node, pfschema.Create, getLabels(n.labelKeys, node.GetLabels()))
}

func (n *NodeHandler) UpdateNode(old, new interface{}) {
	oldNode := old.(*corev1.Node)
	newNode := new.(*corev1.Node)

	oldStatus := getNodeStatus(oldNode)
	newStatus := getNodeStatus(newNode)

	oldLabels := getLabels(n.labelKeys, oldNode.Labels)
	newLabels := getLabels(n.labelKeys, newNode.Labels)

	if oldStatus == newStatus &&
		reflect.DeepEqual(oldNode.Status.Allocatable, newNode.Status.Allocatable) &&
		reflect.DeepEqual(oldLabels, newLabels) {
		return
	}
	if reflect.DeepEqual(oldLabels, newLabels) {
		// In order to skip label update, set newLabels to nil
		newLabels = nil
	}
	n.addQueue(newNode, pfschema.Update, newLabels)
}

func (n *NodeHandler) DeleteNode(obj interface{}) {
	node := obj.(*corev1.Node)
	n.addQueue(node, pfschema.Delete, nil)
}

func getNodeStatus(node *corev1.Node) string {
	if node.Spec.Unschedulable {
		return "Unschedulable"
	}
	nodeStatus := "NotReady"
	condLen := len(node.Status.Conditions)
	if condLen > 0 {
		if node.Status.Conditions[condLen-1].Type == corev1.NodeReady {
			nodeStatus = "Ready"
		}
	}
	return nodeStatus
}

func getLabels(labelKeys []string, totalLabels map[string]string) map[string]string {
	labels := make(map[string]string)

	for _, key := range labelKeys {
		if value, find := totalLabels[key]; find {
			labels[key] = value
		}
	}
	return labels
}

type NodeTaskHandler struct {
	workQueue workqueue.RateLimitingInterface
	labelKeys []string
	cluster   string
}

func NewNodeTaskHandler(q workqueue.RateLimitingInterface, cluster string) *NodeTaskHandler {
	var labelKeys []string
	nodeLabels := strings.TrimSpace(os.Getenv(pfschema.EnvPFTaskLabels))
	if len(nodeLabels) > 0 {
		labelKeys = strings.Split(nodeLabels, ",")
	} else {
		labelKeys = []string{pfschema.QueueLabelKey}
	}
	return &NodeTaskHandler{
		workQueue: q,
		labelKeys: labelKeys,
		cluster:   cluster,
	}
}

func convertPodResources(pod *corev1.Pod) map[string]int64 {
	result := resources.EmptyResource()
	for _, container := range pod.Spec.Containers {
		result.Add(k8s.NewResource(container.Resources.Requests))
	}
	deviceIDX := k8s.SharedGPUIDX(pod)
	if deviceIDX > 0 {
		result.SetResources(k8s.GPUIndexResources, deviceIDX)
	}

	podResources := make(map[string]int64)
	for rName, rValue := range result.Resource() {
		switch rName {
		case resources.ResMemory:
			podResources[string(corev1.ResourceMemory)] = int64(rValue)
		default:
			podResources[rName] = int64(rValue)
		}
	}
	return podResources
}

func (n *NodeTaskHandler) addQueue(pod *corev1.Pod, action pfschema.ActionType, status model.TaskAllocateStatus, labels map[string]string) {
	// TODO: use multi workQueues
	nodeTaskSync := &api.NodeTaskSyncInfo{
		ID:        string(pod.UID),
		Name:      pod.Name,
		NodeName:  pod.Spec.NodeName,
		Status:    status,
		Resources: convertPodResources(pod),
		Labels:    labels,
		Action:    action,
	}
	log.Infof("WatchTaskSync: %s, watch %s event for task %s/%s with status %v", n.cluster, action, pod.Namespace, pod.Name, nodeTaskSync.Status)
	n.workQueue.Add(nodeTaskSync)
}

func isAllocatedPod(pod *corev1.Pod) bool {
	if pod.Spec.NodeName != "" &&
		(pod.Status.Phase == corev1.PodPending || pod.Status.Phase == corev1.PodRunning) {
		return true
	}
	return false
}

func (n *NodeTaskHandler) AddPod(obj interface{}) {
	pod := obj.(*corev1.Pod)

	// TODO: check weather pod is exist or not
	if isAllocatedPod(pod) {
		n.addQueue(pod, pfschema.Create, model.TaskRunning, getLabels(n.labelKeys, pod.Labels))
	}
}

func (n *NodeTaskHandler) UpdatePod(old, new interface{}) {
	oldPod := old.(*corev1.Pod)
	newPod := new.(*corev1.Pod)
	var deletionGracePeriodSeconds int64 = -1
	if newPod.DeletionGracePeriodSeconds != nil {
		deletionGracePeriodSeconds = *newPod.DeletionGracePeriodSeconds
	}
	log.Debugf("TaskSync: %s, update task %s/%s, deletionGracePeriodSeconds: %v, status: %v,  nodeName: %s",
		n.cluster, newPod.Namespace, newPod.Name, deletionGracePeriodSeconds, newPod.Status.Phase, newPod.Spec.NodeName)

	// 1. weather the allocated status of pod is changed or not
	oldPodAllocated := isAllocatedPod(oldPod)
	newPodAllocated := isAllocatedPod(newPod)
	if oldPodAllocated != newPodAllocated {
		if newPodAllocated {
			n.addQueue(newPod, pfschema.Create, model.TaskRunning, getLabels(n.labelKeys, newPod.Labels))
		} else {
			n.addQueue(newPod, pfschema.Delete, model.TaskDeleted, nil)
		}
		return
	}
	// 2. pod is allocated and is deleted
	if newPodAllocated && oldPod.DeletionGracePeriodSeconds == nil && newPod.DeletionGracePeriodSeconds != nil {
		if *newPod.DeletionGracePeriodSeconds == 0 {
			n.addQueue(newPod, pfschema.Delete, model.TaskDeleted, nil)
		} else {
			n.addQueue(newPod, pfschema.Update, model.TaskTerminating, nil)
		}
	}
}

func (n *NodeTaskHandler) DeletePod(obj interface{}) {
	pod := obj.(*corev1.Pod)
	n.addQueue(pod, pfschema.Delete, model.TaskDeleted, nil)
}

func (krc *KubeRuntimeClient) StartListener(listenerType string, stopCh <-chan struct{}) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeNode, pfschema.ListenerTypeNodeTask:
		krc.InformerFactory.Start(stopCh)
		for _, synced := range krc.InformerFactory.WaitForCacheSync(stopCh) {
			if !synced {
				err = fmt.Errorf("timed out waiting for caches to %s", listenerType)
				log.Errorf("on %s, start %s listener failed, err: %v", krc.Cluster(), listenerType, err)
				break
			}
		}
	default:
		err = krc.startDynamicListener(listenerType, stopCh)
	}
	return err
}

func (krc *KubeRuntimeClient) startDynamicListener(listenerType string, stopCh <-chan struct{}) error {
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
	krc.DynamicFactory.Start(stopCh)
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

func (krc *KubeRuntimeClient) ClusterName() string {
	name := ""
	if krc.ClusterInfo != nil {
		name = krc.ClusterInfo.Name
	}
	return name
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

// GetTaskLog using pageSize and pageNo to paging logs
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
		limitFlag := utils.IsReadLimitReached(int64(len(logContent)), int64(length), logFilePosition)
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
				LogContent:  utils.SplitLog(logContent, startIndex, endIndex, overFlag),
				HasNextPage: hasNextPage,
				Truncated:   truncated,
			},
		}
		taskLogInfoList = append(taskLogInfoList, taskLogInfo)
	}
	return taskLogInfoList, nil

}

// GetTaskLogV2 using lineLimit and sizeLimit to paging logs
func (krc *KubeRuntimeClient) GetTaskLogV2(namespace, name string, logpage utils.LogPage) ([]pfschema.TaskLogInfo, error) {
	log.Infof("Get mixed logs for %s/%s, paging info: %#v", namespace, name, logpage)
	taskLogInfoList := make([]pfschema.TaskLogInfo, 0)
	pod, err := krc.Client.CoreV1().Pods(namespace).Get(context.TODO(), name, v1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return []pfschema.TaskLogInfo{}, nil
	} else if err != nil {
		return []pfschema.TaskLogInfo{}, err
	}

	// traverse Containers
	for _, c := range pod.Spec.Containers {
		log.Debugf("traverse pod %s-container %s", name, c.Name)
		podLogOptions := mapToLogOptions(c.Name, logpage.LogFilePosition)
		logContent, logContentLineNum, err := krc.getContainerLog(namespace, name, podLogOptions)
		if err != nil {
			return []pfschema.TaskLogInfo{}, err
		}
		finalContent := logpage.Paging(logContent, logContentLineNum)
		log.Debugf("get log of pod/container: %s/%s, content length: %d", name, c.Name, len(finalContent))
		taskLogInfo := pfschema.TaskLogInfo{
			TaskID: fmt.Sprintf("%s_%s/%s", pod.Name, pod.GetUID(), c.Name),
			Info: pfschema.LogInfo{
				LogContent:  finalContent,
				HasNextPage: logpage.HasNextPage,
				Truncated:   logpage.Truncated,
			},
		}
		taskLogInfoList = append(taskLogInfoList, taskLogInfo)
	}
	return taskLogInfoList, nil
}

func (krc *KubeRuntimeClient) getContainerLog(namespace, name string, logOptions *corev1.PodLogOptions) (string, int, error) {
	readCloser, err := krc.Client.CoreV1().Pods(namespace).GetLogs(name, logOptions).Stream(context.TODO())
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
