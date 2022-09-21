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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	_ "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/queue"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	QueueSyncControllerName = "QueueSync"
)

type QueueSync struct {
	runtimeClient framework.RuntimeClientInterface
	workQueue     workqueue.RateLimitingInterface
}

func NewQueueSync() *QueueSync {
	return &QueueSync{}
}

func (qs *QueueSync) Name() string {
	return fmt.Sprintf("%s controller for %s", QueueSyncControllerName, qs.runtimeClient.Cluster())
}

func (qs *QueueSync) Initialize(runtimeClient framework.RuntimeClientInterface) error {
	if runtimeClient == nil {
		return fmt.Errorf("init %s failed, err: runtimeClient is nil", qs.Name())
	}
	qs.runtimeClient = runtimeClient
	qs.workQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// Register queue listeners
	err := qs.runtimeClient.RegisterListener(pfschema.ListenerTypeQueue, qs.workQueue)
	if err != nil {
		log.Errorf("register event listener for %s failed, err: %v", qs.Name(), err)
		return err
	}
	log.Infof("initialize %s successfully!", qs.Name())
	return nil
}

func (qs *QueueSync) Run(stopCh <-chan struct{}) {
	log.Infof("Start %s ...", qs.Name())
	err := qs.runtimeClient.StartListener(pfschema.ListenerTypeQueue, stopCh)
	if err != nil {
		log.Errorf("start queue listener failed, err: %v", err)
		return
	}
	go wait.Until(qs.runWorker, 0, stopCh)
}

func (qs *QueueSync) runWorker() {
	for qs.processWorkItem() {
	}
}

func (qs *QueueSync) processWorkItem() bool {
	obj, shutdown := qs.workQueue.Get()
	if shutdown {
		return false
	}
	queueSyncInfo := obj.(*api.QueueSyncInfo)
	log.Debugf("%s, queueName is %s", qs.Name(), queueSyncInfo.Name)
	defer qs.workQueue.Done(queueSyncInfo)

	if config.GlobalServerConfig.Job.SyncClusterQueue {
		if err := qs.syncQueueInfo(queueSyncInfo); err != nil {
			log.Errorf("sync queue %s failed. err: %s", queueSyncInfo.Name, err)
			if queueSyncInfo.RetryTimes < DefaultSyncRetryTimes {
				queueSyncInfo.RetryTimes += 1
				qs.workQueue.AddRateLimited(queueSyncInfo)
			}
			qs.workQueue.Forget(queueSyncInfo)
			return true
		}
	}
	qs.workQueue.Forget(queueSyncInfo)
	return true
}

func (qs *QueueSync) syncQueueInfo(qsInfo *api.QueueSyncInfo) error {
	log.Infof("%s, sync queue %s with action %s", qs.Name(), qsInfo.Name, qsInfo.Action)
	var err error
	switch qsInfo.Action {
	case pfschema.Create:
		queue := &model.Queue{
			Name:         qsInfo.Name,
			Namespace:    qsInfo.Namespace,
			ClusterId:    qs.runtimeClient.ClusterID(),
			Status:       qsInfo.Status,
			QuotaType:    qsInfo.QuotaType,
			MaxResources: qsInfo.MaxResource,
			Location:     qsInfo.Labels,
		}
		if qsInfo.MinResource != nil {
			queue.MinResources = qsInfo.MinResource
		}
		err = storage.Queue.CreateOrUpdateQueue(queue)
	case pfschema.Update, pfschema.Delete:
		err = storage.Queue.UpdateQueueInfo(qsInfo.Name, qsInfo.Status, qsInfo.MaxResource, qsInfo.MinResource)
	default:
		err = fmt.Errorf("%s, the sync action of queue %s is not supported", qs.Name(), qsInfo.Action)
	}
	return err
}
