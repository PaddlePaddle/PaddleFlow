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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/k8s"
	queueschema "paddleflow/pkg/common/schema"
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
	opt      *k8s.DynamicClientOption
	jobQueue workqueue.RateLimitingInterface
	// informerMap contains GroupVersionKind and informer for queue, and ElasticResourceQuota
	informerMap map[schema.GroupVersionKind]cache.SharedIndexInformer
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
	qs.informerMap = make(map[schema.GroupVersionKind]cache.SharedIndexInformer)

	for _, gvk := range k8s.GVKToQuotaType {
		gvrMap, err := qs.opt.GetGVR(gvk)
		if err != nil {
			log.Warnf("cann't find GroupVersionKind [%s]", gvk)
		} else {
			qs.informerMap[gvk] = qs.opt.DynamicFactory.ForResource(gvrMap.Resource).Informer()
			qs.informerMap[gvk].AddEventHandler(cache.ResourceEventHandlerFuncs{
				UpdateFunc: qs.updateQueue,
				DeleteFunc: qs.deleteQueue,
			})
		}
	}
	return nil
}

func (qs *QueueSync) Run(stopCh <-chan struct{}) {
	if len(qs.informerMap) == 0 {
		log.Infof("Cluster hasn't needed GroupVersionKind, skip %s controller!", qs.Name())
		return
	}
	go qs.opt.DynamicFactory.Start(stopCh)

	for _, informer := range qs.informerMap {
		if !cache.WaitForCacheSync(stopCh, informer.HasSynced) {
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
	err := models.UpdateQueueStatus(database.DB, queueSyncInfo.Name, queueSyncInfo.Status)
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
	oldQueue := oldObj.(*unstructured.Unstructured)
	queue := newObj.(*unstructured.Unstructured)

	oldSpec, err := getResourceSpec(oldQueue)
	if err != nil || oldSpec == nil {
		log.Errorf("get spec from old resource object %s failed", oldQueue.GetName())
		return
	}
	spec, err := getResourceSpec(queue)
	if err != nil || spec == nil {
		log.Errorf("get spec from new resource object %s failed", queue.GetName())
		return
	}

	log.Debugf("%s queue resource is updated. old:%v new:%v", queue.GroupVersionKind(), oldQueue, queue)
	if reflect.DeepEqual(oldSpec, spec) {
		return
	}

	log.Infof("watch %s resource is updated, name is %s", queue.GroupVersionKind(), queue.GetName())
	var status = queueschema.StatusQueueUnavailable
	queueInfo := &QueueSyncInfo{
		Name:    queue.GetName(),
		Status:  status,
		Message: fmt.Sprintf("queue[%s] is update from cluster", queue.GetName()),
	}
	qs.jobQueue.Add(queueInfo)
}

// deleteQueue for queue resource delete event
func (qs *QueueSync) deleteQueue(obj interface{}) {
	queueObj := obj.(*unstructured.Unstructured)
	log.Infof("watch %s resource is deleted, name is %s", queueObj.GroupVersionKind(), queueObj.GetName())

	queueInfo := &QueueSyncInfo{
		Name:    queueObj.GetName(),
		Status:  queueschema.StatusQueueUnavailable,
		Message: fmt.Sprintf("queue resource[%s] is deleted from cluster", queueObj.GetName()),
	}
	qs.jobQueue.Add(queueInfo)
}

func getResourceSpec(queueObj *unstructured.Unstructured) (interface{}, error) {
	spec, ok, unerr := unstructured.NestedFieldCopy(queueObj.Object, "spec")
	if !ok {
		if unerr != nil {
			log.Error(unerr, "NestedFieldCopy unstructured to spec error")
			return nil, unerr
		}
		log.Info("NestedFieldCopy unstructured to spec error: Spec is not found in resource")
		return nil, fmt.Errorf("get spec from unstructured object failed")
	}
	return spec, nil
}
