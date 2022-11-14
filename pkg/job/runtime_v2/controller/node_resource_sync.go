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

package controller

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	NodeResourceControllerName = "NodeResourceSync"
)

type NodeResourceSync struct {
	runtimeClient framework.RuntimeClientInterface
	// nodeQueue contains node add/update/delete event
	nodeQueue workqueue.RateLimitingInterface
	// taskQueue contains task add/update/delete event
	taskQueue workqueue.RateLimitingInterface
}

func NewNodeResourceSync() *NodeResourceSync {
	return &NodeResourceSync{}
}

func (nr *NodeResourceSync) Name() string {
	return fmt.Sprintf("%s controller for %s", NodeResourceControllerName, nr.runtimeClient.Cluster())
}

func (nr *NodeResourceSync) Initialize(runtimeClient framework.RuntimeClientInterface) error {
	if runtimeClient == nil {
		return fmt.Errorf("init %s controller failed", NodeResourceControllerName)
	}
	nr.runtimeClient = runtimeClient
	log.Infof("initialize %s!", nr.Name())
	// TODO: adjust rateLimiter parameters for node queue
	nr.nodeQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	// TODO: adjust rateLimiter parameters for task queue
	nr.taskQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// Register node listener
	err := nr.runtimeClient.RegisterListener(schema.ListenerTypeNode, nr.nodeQueue)
	if err != nil {
		log.Errorf("register node event listener for %s failed, err: %v", nr.Name(), err)
		return err
	}
	// Register task listener
	err = nr.runtimeClient.RegisterListener(schema.ListenerTypeNodeTask, nr.taskQueue)
	if err != nil {
		log.Errorf("register node task event listener for %s failed, err: %v", nr.Name(), err)
		return err
	}
	return nil
}

func (nr *NodeResourceSync) Run(stopCh <-chan struct{}) {
	log.Infof("start %s successfully!", nr.Name())
	err := nr.runtimeClient.StartListener(schema.ListenerTypeNode, stopCh)
	if err != nil {
		log.Errorf("start node listener failed, err: %v", err)
		return
	}
	err = nr.runtimeClient.StartListener(schema.ListenerTypeNodeTask, stopCh)
	if err != nil {
		log.Errorf("start node task listener failed, err: %v", err)
		return
	}

	go wait.Until(nr.runNodeWorker, 0, stopCh)
	go wait.Until(nr.runNodeTaskWorker, 0, stopCh)
}

func (nr *NodeResourceSync) runNodeWorker() {
	for nr.processNode() {
	}
}

func (nr *NodeResourceSync) processNode() bool {
	obj, shutdown := nr.nodeQueue.Get()
	if shutdown {
		log.Errorf("FailedNodeSync: %s, fail to pop node sync item from queue", nr.runtimeClient.Cluster())
		return false
	}

	nodeSync := obj.(*api.NodeSyncInfo)
	defer nr.nodeQueue.Done(nodeSync)

	log.Infof("ProcessNodeSync: %s, try to handle node sync: %v", nr.runtimeClient.Cluster(), nodeSync)
	var err error
	nodeInfo := &model.NodeInfo{
		ID:          nr.generateNodeID(nodeSync.Name),
		Name:        nodeSync.Name,
		ClusterID:   nr.runtimeClient.ClusterID(),
		ClusterName: nr.runtimeClient.ClusterName(),
		Status:      nodeSync.Status,
		Labels:      nodeSync.Labels,
		Capacity:    nodeSync.Capacity,
	}
	switch nodeSync.Action {
	case schema.Create:
		err = storage.NodeCache.AddNode(nodeInfo)
	case schema.Update:
		err = storage.NodeCache.UpdateNode(nodeInfo.ID, nodeInfo)
	case schema.Delete:
		err = storage.NodeCache.DeleteNode(nodeInfo.ID)
	default:
		err = fmt.Errorf("action %s for node is not supported", nodeSync.Action)
	}
	if err != nil {
		log.Errorf("FailedNodeSync: %s, update node cache failed, err: %v", nr.runtimeClient.Cluster(), err)
		if nodeSync.RetryTimes < DefaultSyncRetryTimes {
			nodeSync.RetryTimes += 1
			nr.taskQueue.AddRateLimited(nodeSync)
			return true
		}
	}
	// If no error, forget it
	nr.nodeQueue.Forget(nodeSync)
	return true
}

func (nr *NodeResourceSync) runNodeTaskWorker() {
	for nr.processNodeTask() {
	}
}

func (nr *NodeResourceSync) processNodeTask() bool {
	obj, shutdown := nr.taskQueue.Get()
	if shutdown {
		log.Errorf("FailedTaskSync: %s, fail to pop node task sync item from queue", nr.runtimeClient.Cluster())
		return false
	}

	taskSync := obj.(*api.NodeTaskSyncInfo)
	defer nr.taskQueue.Done(taskSync)

	log.Infof("ProcessTaskSync: %s, try to handle node task sync: %v", nr.runtimeClient.Cluster(), taskSync)

	var err error
	taskInfo := &model.PodInfo{
		ID:        taskSync.ID,
		Name:      taskSync.Name,
		NodeID:    nr.generateNodeID(taskSync.NodeName),
		NodeName:  taskSync.NodeName,
		Status:    int(taskSync.Status),
		Labels:    taskSync.Labels,
		Resources: taskSync.Resources,
	}
	switch taskSync.Action {
	case schema.Create:
		err = storage.PodCache.AddPod(taskInfo)
	case schema.Update:
		err = storage.PodCache.UpdatePod(taskSync.ID, taskInfo)
	case schema.Delete:
		err = storage.PodCache.DeletePod(taskSync.ID)
	default:
		err = fmt.Errorf("action %s for task is not supported", taskSync.Action)
	}
	if err != nil {
		log.Errorf("FailedTaskSync: %s, update task cache failed, err: %v", nr.runtimeClient.Cluster(), err)
		if taskSync.RetryTimes < DefaultSyncRetryTimes {
			taskSync.RetryTimes += 1
			nr.taskQueue.AddRateLimited(taskSync)
			return true
		}
	}
	// If no error, forget it
	nr.taskQueue.Forget(taskSync)
	return true
}

func (nr *NodeResourceSync) generateNodeID(name string) string {
	return fmt.Sprintf("%s-%s", nr.runtimeClient.ClusterID(), name)
}
