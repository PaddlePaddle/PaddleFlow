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
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	commonschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	QueueSyncControllerName = "QueueSync"
)

type QueueSyncInfo struct {
	Name        string
	Namespace   string
	Labels      map[string]string
	Status      string
	QuotaType   string
	MaxResource *resources.Resource
	MinResource *resources.Resource
	Action      commonschema.ActionType
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
	if opt == nil || opt.ClusterInfo == nil {
		return fmt.Errorf("init %s controller failed", qs.Name())
	}
	log.Infof("Initialize %s controller for cluster %s!", qs.Name(), opt.ClusterInfo.Name)
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
				AddFunc:    qs.add,
				UpdateFunc: qs.update,
				DeleteFunc: qs.delete,
			})
		}
	}
	return nil
}

func (qs *QueueSync) Run(stopCh <-chan struct{}) {
	if len(qs.informerMap) == 0 {
		log.Infof("cluster hasn't needed GroupVersionKind, skip %s controller!", qs.Name())
		return
	}
	go qs.opt.DynamicFactory.Start(stopCh)

	for _, informer := range qs.informerMap {
		if !cache.WaitForCacheSync(stopCh, informer.HasSynced) {
			log.Errorf("timed out waiting for caches to %s", qs.Name())
			return
		}
	}
	log.Infof("Start %s controller for cluster %s successfully!", qs.Name(), qs.opt.ClusterInfo.Name)
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
	log.Debugf("process queue sync. queueName is %s", queueSyncInfo.Name)
	defer qs.jobQueue.Done(queueSyncInfo)

	if config.GlobalServerConfig.Job.SyncClusterQueue {
		if err := qs.syncQueueInfo(queueSyncInfo); err != nil {
			log.Errorf("sync queue %s failed. err: %s", queueSyncInfo.Name, err)
			if queueSyncInfo.RetryTimes < DefaultSyncRetryTimes {
				queueSyncInfo.RetryTimes += 1
				qs.jobQueue.AddRateLimited(queueSyncInfo)
			}
			qs.jobQueue.Forget(queueSyncInfo)
			return true
		}
	}
	qs.jobQueue.Forget(queueSyncInfo)
	return true
}

func (qs *QueueSync) syncQueueInfo(qsInfo *QueueSyncInfo) error {
	log.Debugf("sync queue %s with action %s", qsInfo.Name, qsInfo.Action)
	var err error
	switch qsInfo.Action {
	case commonschema.Create:
		queue := &model.Queue{
			Name:         qsInfo.Name,
			Namespace:    qsInfo.Namespace,
			ClusterId:    qs.opt.ClusterInfo.ID,
			Status:       qsInfo.Status,
			QuotaType:    qsInfo.QuotaType,
			MaxResources: qsInfo.MaxResource,
			Location:     qsInfo.Labels,
		}
		if qsInfo.MinResource != nil {
			queue.MinResources = qsInfo.MinResource
		}
		err = storage.Queue.CreateOrUpdateQueue(queue)
	case commonschema.Update, commonschema.Delete:
		err = storage.Queue.UpdateQueueInfo(qsInfo.Name, qsInfo.Status, qsInfo.MaxResource, qsInfo.MinResource)
	default:
		err = fmt.Errorf("the sync action of queue %s is not supported", qsInfo.Action)
	}
	return err
}

// add for queue resource add event
func (qs *QueueSync) add(obj interface{}) {
	newObj := obj.(*unstructured.Unstructured)

	gvk := newObj.GroupVersionKind()
	name := newObj.GetName()
	queue, err := convertUnstructuredResource(newObj, gvk)
	if err != nil || queue == nil {
		log.Errorf("get spec from resource object %s failed, err: %v", name, err)
		return
	}

	qSyncInfo := &QueueSyncInfo{
		Name:   name,
		Action: commonschema.Create,
		Labels: newObj.GetLabels(),
	}
	switch gvk {
	case k8s.VCQueueGVK:
		vcQueue := queue.(*v1beta1.Queue)
		qSyncInfo.MaxResource = k8s.NewResource(vcQueue.Spec.Capability)
		// set queue status
		qSyncInfo.Status = getVCQueueStatus(vcQueue.Status.State)
		qSyncInfo.QuotaType = commonschema.TypeVolcanoCapabilityQuota
		qSyncInfo.Namespace = "default"
	case k8s.EQuotaGVK:
		eQuota := queue.(*v1beta1.ElasticResourceQuota)
		qSyncInfo.MaxResource = k8s.NewResource(eQuota.Spec.Max)
		qSyncInfo.MinResource = k8s.NewResource(eQuota.Spec.Min)
		// set queue status
		qSyncInfo.Status = getEQuotaStatus(eQuota.Status)
		qSyncInfo.QuotaType = commonschema.TypeElasticQuota
		qSyncInfo.Namespace = eQuota.Spec.Namespace
	default:
		log.Warnf("quota type %s for queue is not supported", gvk.String())
		return
	}

	qs.jobQueue.Add(qSyncInfo)
	log.Infof("watch queue %s is added, type is %s", name, gvk.String())
}

// update for queue update event
func (qs *QueueSync) update(old, new interface{}) {
	oldObj := old.(*unstructured.Unstructured)
	newObj := new.(*unstructured.Unstructured)

	gvk := newObj.GroupVersionKind()
	name := newObj.GetName()
	oldQueue, err := convertUnstructuredResource(oldObj, gvk)
	if err != nil || oldQueue == nil {
		log.Errorf("get spec from old resource object %s failed", name)
		return
	}
	queue, err := convertUnstructuredResource(newObj, gvk)
	if err != nil || queue == nil {
		log.Errorf("get spec from new resource object %s failed", name)
		return
	}

	qSyncInfo := &QueueSyncInfo{
		Name:   name,
		Action: commonschema.Update,
	}
	msg := ""
	switch gvk {
	case k8s.VCQueueGVK:
		oldQ := oldQueue.(*v1beta1.Queue)
		newQ := queue.(*v1beta1.Queue)
		if reflect.DeepEqual(oldQ.Spec, newQ.Spec) && oldQ.Status.State == newQ.Status.State {
			return
		}
		msg = fmt.Sprintf("old queue: %v, new queue: %v", oldQ.Spec, newQ.Spec)
		qSyncInfo.MaxResource = k8s.NewResource(newQ.Spec.Capability)
		// set queue status
		qSyncInfo.Status = getVCQueueStatus(newQ.Status.State)
	case k8s.EQuotaGVK:
		oldEquota := oldQueue.(*v1beta1.ElasticResourceQuota)
		newEquota := queue.(*v1beta1.ElasticResourceQuota)
		if reflect.DeepEqual(oldEquota.Spec, newEquota.Spec) && oldEquota.Status.IsLeaf == newEquota.Status.IsLeaf {
			return
		}
		msg = fmt.Sprintf("old queue: %v, new queue: %v", oldEquota.Spec, newEquota.Spec)
		qSyncInfo.MaxResource = k8s.NewResource(newEquota.Spec.Max)
		qSyncInfo.MinResource = k8s.NewResource(newEquota.Spec.Min)
		qSyncInfo.Status = getEQuotaStatus(newEquota.Status)
	default:
		log.Warnf("quota type %s for queue is not supported", gvk.String())
		return
	}
	qs.jobQueue.Add(qSyncInfo)
	log.Infof("watch queue %s is updated, type is %s. message: %s", name, gvk.String(), msg)
}

// delete for queue resource delete event
func (qs *QueueSync) delete(obj interface{}) {
	queueObj := obj.(*unstructured.Unstructured)
	qSyncInfo := &QueueSyncInfo{
		Name:   queueObj.GetName(),
		Action: commonschema.Delete,
		Status: commonschema.StatusQueueUnavailable,
	}
	qs.jobQueue.Add(qSyncInfo)
	log.Infof("watch queue %s is deleted, type is %s", queueObj.GetName(), queueObj.GroupVersionKind())
}

func getVCQueueStatus(state v1beta1.QueueState) string {
	status := commonschema.StatusQueueOpen
	switch state {
	case "", v1beta1.QueueStateOpen:
		status = commonschema.StatusQueueOpen
	case v1beta1.QueueStateClosing:
		status = commonschema.StatusQueueClosing
	case v1beta1.QueueStateClosed:
		status = commonschema.StatusQueueClosed
	case v1beta1.QueueStateUnknown:
		status = commonschema.StatusQueueUnavailable
	}
	return status
}

func getEQuotaStatus(state v1beta1.ElasticResourceQuotaStatus) string {
	status := commonschema.StatusQueueOpen
	if !state.IsLeaf {
		status = commonschema.StatusQueueClosed
	}
	return status
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
