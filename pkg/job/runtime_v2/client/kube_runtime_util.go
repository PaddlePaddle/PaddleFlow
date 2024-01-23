/*
Copyright (c) 2024 PaddlePaddle Authors. All Rights Reserve.

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
	"encoding/json"
	"os"
	"reflect"
	"strings"

	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type NodeHandler struct {
	workQueue      workqueue.RateLimitingInterface
	labelKeys      []string
	cluster        string
	resourceFilter []string
	cardTypeAnno   string
}

func NewNodeHandler(q workqueue.RateLimitingInterface, cluster string) *NodeHandler {
	var labelKeys []string
	nodeLabels := strings.TrimSpace(os.Getenv(schema.EnvPFNodeLabels))
	if len(nodeLabels) > 0 {
		labelKeys = strings.Split(nodeLabels, ",")
	} else {
		labelKeys = []string{schema.PFNodeLabels}
	}
	var rFilter = []string{
		"pods",
	}
	resourceFilters := strings.TrimSpace(os.Getenv(schema.EnvPFResourceFilter))
	if len(resourceFilters) > 0 {
		filters := strings.Split(resourceFilters, ",")
		rFilter = append(rFilter, filters...)
	}
	cardTypeAnno := strings.TrimSpace(os.Getenv(schema.EnvPFNodeCardType))
	if cardTypeAnno == "" {
		cardTypeAnno = "gpu.topo"
	}
	return &NodeHandler{
		workQueue:      q,
		labelKeys:      labelKeys,
		cluster:        cluster,
		resourceFilter: rFilter,
		cardTypeAnno:   cardTypeAnno,
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

func (n *NodeHandler) addQueue(node *v1.Node, action schema.ActionType, labels map[string]string) {
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
		IP:       getNodeIP(node),
		Capacity: capacity,
		Labels:   labels,
		Action:   action,
	}
	log.Debugf("WatchNodeSync: %s, watch %s event for node %s with status %s, card %s", n.cluster, action,
		node.Name, nodeSync.Status, labels[schema.PFNodeCardTypeAnno])
	n.workQueue.Add(nodeSync)
}

func (n *NodeHandler) AddNode(obj interface{}) {
	node := obj.(*v1.Node)
	n.addQueue(node, schema.Create, n.getNodeLabels(node))
}

func (n *NodeHandler) UpdateNode(old, new interface{}) {
	oldNode := old.(*v1.Node)
	newNode := new.(*v1.Node)

	oldStatus := getNodeStatus(oldNode)
	newStatus := getNodeStatus(newNode)

	oldLabels := n.getNodeLabels(oldNode)
	newLabels := n.getNodeLabels(newNode)

	if oldStatus == newStatus &&
		reflect.DeepEqual(oldNode.Status.Allocatable, newNode.Status.Allocatable) &&
		reflect.DeepEqual(oldLabels, newLabels) {
		return
	}
	if reflect.DeepEqual(oldLabels, newLabels) {
		// In order to skip label update, set newLabels to nil
		newLabels = nil
	}
	n.addQueue(newNode, schema.Update, newLabels)
}

func (n *NodeHandler) DeleteNode(obj interface{}) {
	node := obj.(*v1.Node)
	n.addQueue(node, schema.Delete, nil)
}

// getNodeLabels 函数用于获取 NodeHandler 中给定 node 的标签。
// 如果 node 为 nil，则返回 nil。
// 参数 node 是一个 corev1.Node 指针，表示需要获取标签的节点。
// 返回一个 map[string]string 类型的标签，表示从给定节点中获取到的标签。
func (n *NodeHandler) getNodeLabels(node *v1.Node) map[string]string {
	if node == nil {
		return nil
	}
	nodeLabels := getLabels(n.labelKeys, node.Labels)
	nodeLabels[schema.PFNodeCardTypeAnno] = n.getNodeCardType(node)
	return nodeLabels
}

func (n *NodeHandler) getNodeCardType(node *v1.Node) string {
	cardType := ""
	// 1. get card type from node gpu-topo annotations
	gpuTopoAnno := node.Annotations[n.cardTypeAnno]
	if gpuTopoAnno != "" {
		gpuTopo := []struct {
			UUID                  string
			Path                  string
			Model                 string
			Power                 int64
			Memory                int64
			CPUAffinity           int64
			PCI                   interface{}
			Clocks                interface{}
			Topology              interface{}
			CudaComputeCapability interface{}
		}{{}}
		err := json.Unmarshal([]byte(gpuTopoAnno), &gpuTopo)
		if err == nil && len(gpuTopo) > 0 {
			cardType = gpuTopo[0].Model
		} else {
			log.Warnf("Failed to unmarshal gpu-topo annotation, err: %v", err)
		}
	}
	// 2. get card type from node PaddleFlow annotations
	if cardType == "" {
		cardType = node.Annotations[schema.PFNodeCardTypeAnno]
	}
	return cardType
}

func getNodeStatus(node *v1.Node) string {
	if node.Spec.Unschedulable {
		return schema.StatusNodeUnsched
	}
	nodeStatus := schema.StatusNodeNotReady
	condLen := len(node.Status.Conditions)
	if condLen > 0 {
		if node.Status.Conditions[condLen-1].Type == v1.NodeReady {
			nodeStatus = schema.StatusNodeReady
		}
	}
	return nodeStatus
}

func getNodeIP(node *v1.Node) string {
	var nodeIP string
	for _, address := range node.Status.Addresses {
		if address.Type == v1.NodeInternalIP {
			nodeIP = address.Address
		}
	}
	return nodeIP
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
	nodeLabels := strings.TrimSpace(os.Getenv(schema.EnvPFTaskLabels))
	if len(nodeLabels) > 0 {
		labelKeys = strings.Split(nodeLabels, ",")
	} else {
		labelKeys = []string{schema.QueueLabelKey}
	}
	return &NodeTaskHandler{
		workQueue: q,
		labelKeys: labelKeys,
		cluster:   cluster,
	}
}

func convertPodResources(pod *v1.Pod) map[string]int64 {
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
			podResources[string(v1.ResourceMemory)] = int64(rValue)
		default:
			podResources[rName] = int64(rValue)
		}
	}
	return podResources
}

func (n *NodeTaskHandler) addQueue(pod *v1.Pod, action schema.ActionType, status model.TaskAllocateStatus, labels map[string]string) {
	// TODO: use multi workQueues
	nodeTaskSync := &api.NodeTaskSyncInfo{
		ID:        string(pod.UID),
		Name:      pod.Name,
		NodeName:  pod.Spec.NodeName,
		Namespace: pod.Namespace,
		Status:    status,
		Resources: convertPodResources(pod),
		Labels:    labels,
		Action:    action,
	}
	log.Infof("WatchTaskSync: %s, watch %s event for task %s/%s with status %v, annotations: %v", n.cluster, action, pod.Namespace, pod.Name, nodeTaskSync.Status, pod.Annotations)
	n.workQueue.Add(nodeTaskSync)
}

func isAllocatedPod(pod *v1.Pod) bool {
	if pod.Spec.NodeName != "" &&
		(pod.Status.Phase == v1.PodPending || pod.Status.Phase == v1.PodRunning) {
		return true
	}
	return false
}

func isResourcesChanged(oldPod, newPod *v1.Pod) bool {
	if oldPod == nil || newPod == nil ||
		len(oldPod.Spec.Containers) != len(newPod.Spec.Containers) {
		return false
	}
	for idx := range newPod.Spec.Containers {
		oldContainerReq := oldPod.Spec.Containers[idx].Resources.Requests
		if !reflect.DeepEqual(oldContainerReq, newPod.Spec.Containers[idx].Resources.Requests) {
			return true
		}
	}
	// check weather gpu is changed
	hasChanged := false
	if oldPod.Annotations != nil && newPod.Annotations != nil {
		if oldPod.Annotations[k8s.GPUCorePodKey] != newPod.Annotations[k8s.GPUCorePodKey] ||
			oldPod.Annotations[k8s.GPUMemPodKey] != newPod.Annotations[k8s.GPUMemPodKey] {
			hasChanged = true
		}
	}
	return hasChanged
}

func (n *NodeTaskHandler) AddPod(obj interface{}) {
	pod := obj.(*v1.Pod)

	// TODO: check weather pod is exist or not
	if isAllocatedPod(pod) {
		n.addQueue(pod, schema.Create, model.TaskRunning, getLabels(n.labelKeys, pod.Labels))
	}
}

func (n *NodeTaskHandler) UpdatePod(old, new interface{}) {
	oldPod := old.(*v1.Pod)
	newPod := new.(*v1.Pod)
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
			n.addQueue(newPod, schema.Create, model.TaskRunning, getLabels(n.labelKeys, newPod.Labels))
		} else {
			n.addQueue(newPod, schema.Delete, model.TaskDeleted, nil)
		}
		return
	}
	// 2. pod is allocated and is deleted
	if newPodAllocated && oldPod.DeletionGracePeriodSeconds == nil && newPod.DeletionGracePeriodSeconds != nil {
		if *newPod.DeletionGracePeriodSeconds == 0 {
			n.addQueue(newPod, schema.Delete, model.TaskDeleted, nil)
		} else {
			n.addQueue(newPod, schema.Update, model.TaskTerminating, nil)
		}
	}
	// 3. pod is allocated and pod resource is updated
	if newPodAllocated && isResourcesChanged(oldPod, newPod) {
		// update pod resources
		n.addQueue(newPod, schema.Update, model.TaskRunning, nil)
	}
}

func (n *NodeTaskHandler) DeletePod(obj interface{}) {
	pod := obj.(*v1.Pod)
	n.addQueue(pod, schema.Delete, model.TaskDeleted, nil)
}
