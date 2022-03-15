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
	"reflect"
	"sync"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	vcqueue "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/common/schema"
)

const (
	QueueSyncControllerName = "QueueSync"
)

type QueueSyncInfo struct {
	Name       string
	Status     string
	Message    string
	RetryTimes int
}

type QueueSync struct {
	sync.Mutex
	opt             *k8s.DynamicClientOption
	jobQueue        workqueue.RateLimitingInterface
	vcQueueInformer cache.SharedIndexInformer
}

func NewQueueSync() Controller {
	return &QueueSync{}
}

func (qs *QueueSync) Name() string {
	return QueueSyncControllerName
}

func (qs *QueueSync) Initialize(opt *k8s.DynamicClientOption) error {
	log.Infof("Initialize %s controller!", qs.Name())
	qs.opt = opt
	qs.jobQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	vcQueueGVRMap, err := qs.opt.GetGVR(k8s.VCQueueGVK)
	if err != nil {
		log.Warnf("cann't find GroupVersionKind [%s]", k8s.VCQueueGVK)
	} else {
		qs.vcQueueInformer = qs.opt.DynamicFactory.ForResource(vcQueueGVRMap.Resource).Informer()
		qs.vcQueueInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			UpdateFunc: qs.updateQueue,
			DeleteFunc: qs.deleteQueue,
		})
	}
	return nil
}

func (qs *QueueSync) Run(stopCh <-chan struct{}) {
	if qs.vcQueueInformer == nil {
		log.Infof("Cluster hasn't vc queue, skip %s controller!", qs.Name())
		return
	}
	go qs.opt.DynamicFactory.Start(stopCh)

	if qs.vcQueueInformer != nil {
		if !cache.WaitForCacheSync(stopCh, qs.vcQueueInformer.HasSynced) {
			log.Errorf("timed out waiting for caches to %s", qs.Name())
			return
		}
	}
	log.Infof("Start %s controller successfully!", qs.Name())
	go wait.Until(qs.runWorker, 0, stopCh)
}

func (qs *QueueSync) runWorker() {
	for qs.processWorkItem() {
	}
}

func (qs *QueueSync) processWorkItem() bool {
	obj, shutdown := qs.jobQueue.Get()
	if shutdown {
		return false
	}
	queueSyncInfo := obj.(*QueueSyncInfo)
	log.Debugf("process queue sync. queue name:[%s]", queueSyncInfo.Name)
	defer qs.jobQueue.Done(queueSyncInfo)

	// update queue status to unavailable
	err := models.UpdateQueueStatus(queueSyncInfo.Name, queueSyncInfo.Status)
	if err != nil {
		log.Errorf("queueInformer update queue status failed. queueName:[%s] error:[%s]", queueSyncInfo.Name, err.Error())
		if queueSyncInfo.RetryTimes < DefaultSyncRetryTimes {
			queueSyncInfo.RetryTimes += 1
			qs.jobQueue.AddRateLimited(queueSyncInfo)
		}
	}
	qs.jobQueue.Forget(queueSyncInfo)
	return true
}

// updateQueue for queue update event
func (qs *QueueSync) updateQueue(oldObj, newObj interface{}) {
	oldQueue, err := unstructuredToVCQueue(oldObj)
	if err != nil {
		return
	}
	queue, err := unstructuredToVCQueue(newObj)
	if err != nil {
		return
	}
	log.Debugf("vcQueueInformer update begin. oldQueue:%v newQueue:%v", oldQueue, queue)
	if reflect.DeepEqual(oldQueue.Spec, queue.Spec) && oldQueue.Status == queue.Status {
		return
	}
	status := string(queue.Status.State)
	if !reflect.DeepEqual(oldQueue.Spec, queue.Spec) {
		status = schema.StatusQueueUnavailable
	}
	queueInfo := &QueueSyncInfo{
		Name:    queue.Name,
		Status:  status,
		Message: fmt.Sprintf("queue[%s] is update from cluster", queue.Name),
	}
	qs.jobQueue.Add(queueInfo)
}

// deleteQueue for queue delete event
func (qs *QueueSync) deleteQueue(obj interface{}) {
	queue, err := unstructuredToVCQueue(obj)
	if err != nil {
		return
	}
	log.Debugf("vcQueueInformer DeleteFunc. queueName:%s", queue.Name)
	queueInfo := &QueueSyncInfo{
		Name:    queue.Name,
		Status:  schema.StatusQueueUnavailable,
		Message: fmt.Sprintf("queue[%s] is deleted from cluster", queue.Name),
	}
	qs.jobQueue.Add(queueInfo)
}

func unstructuredToVCQueue(obj interface{}) (*vcqueue.Queue, error) {
	queueObj := obj.(*unstructured.Unstructured)
	queue := &vcqueue.Queue{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(queueObj.Object, queue); err != nil {
		log.Errorf("convert unstructured object[%+v] to sparkApp failed: %v", obj, err)
		return nil, err
	}
	return queue, nil
}
