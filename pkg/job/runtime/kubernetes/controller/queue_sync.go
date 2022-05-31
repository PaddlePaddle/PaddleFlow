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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	commomschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const (
	QueueSyncControllerName = "QueueSync"
)

type QueueSyncInfo struct {
	Name        string
	Status      string
	MaxResource *commomschema.ResourceInfo
	MinResource *commomschema.ResourceInfo
	Type        string
	Message     string
	RetryTimes  int
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
	log.Debugf("process queue sync. queueName is %s", queueSyncInfo.Name)
	defer qs.jobQueue.Done(queueSyncInfo)
	return true
}

// updateQueue for queue update event
func (qs *QueueSync) updateQueue(old, new interface{}) {
	oldObj := old.(*unstructured.Unstructured)
	newObj := new.(*unstructured.Unstructured)

	gvk := newObj.GroupVersionKind()
	name := newObj.GetName()

	oldQueue, err := convertUnstructuredResource(oldObj, gvk)
	if err != nil || oldObj == nil {
		log.Errorf("get spec from old resource object %s failed", name)
		return
	}
	queue, err := convertUnstructuredResource(newObj, gvk)
	if err != nil || newObj == nil {
		log.Errorf("get spec from new resource object %s failed", name)
		return
	}

	switch gvk {
	case k8s.VCQueueGVK:
		qs.updateVCQueue(oldQueue, queue)
	case k8s.EQuotaGVK:
		qs.updateEQuota(oldQueue, queue)
	default:
		log.Warnf("quota type %s for queue is not supported", gvk.String())
		return
	}
}

func (qs *QueueSync) updateEQuota(oldObj, newObj interface{}) {
	if oldObj == nil || newObj == nil {
		return
	}

	oldEquota := oldObj.(*v1beta1.ElasticResourceQuota)
	newEquota := newObj.(*v1beta1.ElasticResourceQuota)

	if reflect.DeepEqual(oldEquota.Spec, newEquota.Spec) {
		return
	}
	log.Infof("%s queue resource is updated. old:%v new:%v", newEquota.GroupVersionKind(), oldEquota.Spec, newEquota.Spec)
}

func (qs *QueueSync) updateVCQueue(oldObj, newObj interface{}) {
	if oldObj == nil || newObj == nil {
		return
	}
	oldQueue := oldObj.(*v1beta1.Queue)
	newQueue := newObj.(*v1beta1.Queue)

	if reflect.DeepEqual(oldQueue.Spec, newQueue.Spec) {
		return
	}
	log.Infof("%s queue resource is updated. old:%v new:%v", newQueue.GroupVersionKind(), oldQueue.Spec, newQueue.Spec)
}

// deleteQueue for queue resource delete event
func (qs *QueueSync) deleteQueue(obj interface{}) {
	queueObj := obj.(*unstructured.Unstructured)
	log.Infof("watch %s resource is deleted, name is %s", queueObj.GroupVersionKind(), queueObj.GetName())
}

func convertUnstructuredResource(queueObj *unstructured.Unstructured, gvk schema.GroupVersionKind) (interface{}, error) {
	var realQueue interface{}
	switch gvk {
	case k8s.VCQueueGVK:
		realQueue = &v1beta1.Queue{}
	case k8s.EQuotaGVK:
		realQueue = &v1beta1.ElasticResourceQuota{}
	default:
		return nil, fmt.Errorf("the group version kind %s for queue is not supported", gvk)
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(queueObj.Object, realQueue); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s status failed. error: %s", queueObj, gvk.String(), err.Error())
		return nil, err
	}
	return realQueue, nil
}
