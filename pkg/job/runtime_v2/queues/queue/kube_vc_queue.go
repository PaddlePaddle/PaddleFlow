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

package queue

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/queues/util/kuberuntime"
)

var (
	QueueGVK             = k8s.VCQueueGVK
	KubeVCQueueQuotaType = client.KubeFrameworkVersion(QueueGVK)
)

// KubeElasticQuota is an executor struct that runs a single pod
type KubeElasticQuota struct {
	GVK           schema.GroupVersionKind
	runtimeClient framework.RuntimeClientInterface
	workQueue     workqueue.RateLimitingInterface
}

func New(client framework.RuntimeClientInterface) framework.QueueInterface {
	return &KubeElasticQuota{
		runtimeClient: client,
		GVK:           QueueGVK,
	}
}

func (eq *KubeElasticQuota) String(name string) string {
	return fmt.Sprintf("%s queue %s on %s", eq.GVK.String(), name, eq.runtimeClient.Cluster())
}

func (eq *KubeElasticQuota) Create(ctx context.Context, q *api.QueueInfo) error {
	if q == nil {
		return fmt.Errorf("queue is nil")
	}
	// construct vc queue
	vcQueue := &v1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: q.Name,
		},
		Spec: v1beta1.QueueSpec{
			Capability: k8s.NewResourceList(q.MaxResources),
		},
		Status: v1beta1.QueueStatus{
			State: v1beta1.QueueStateOpen,
		},
	}
	log.Debugf("Create %s, info: %#v", eq.String(q.Name), vcQueue)
	err := eq.runtimeClient.Create(vcQueue, KubeVCQueueQuotaType)
	if err != nil {
		log.Errorf("Create %s falied, err: %s", eq.String(q.Name), err)
		return err
	}
	return nil
}

func (eq *KubeElasticQuota) Update(ctx context.Context, q *api.QueueInfo) error {
	capability := k8s.NewResourceList(q.MaxResources)
	log.Debugf("UpdateQueue resourceList[%v]", capability)

	obj, err := eq.runtimeClient.Get(q.Namespace, q.Name, KubeVCQueueQuotaType)
	if err != nil {
		log.Errorf("get %s failed, err: %s", eq.String(q.Name), err)
		return err
	}
	unObj := obj.(*unstructured.Unstructured)

	var vcQueue = &v1beta1.Queue{}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, vcQueue)
	if err != nil {
		log.Errorf("convert unstructured object %v to %s failed, err: %s", unObj.Object, eq.String(q.Name), err)
		return err
	}
	vcQueue.Spec.Capability = k8s.NewResourceList(q.MaxResources)
	vcQueue.Status.State = v1beta1.QueueState(q.Status)

	log.Infof("begin to update %s, info: %#v", eq.String(q.Name), vcQueue)
	if err = eq.runtimeClient.Update(vcQueue, KubeVCQueueQuotaType); err != nil {
		log.Errorf("update %s falied. err: %s", eq.String(q.Name), err)
		return err
	}
	return nil
}

func (eq *KubeElasticQuota) Delete(ctx context.Context, queue *api.QueueInfo) error {
	if queue == nil {
		return fmt.Errorf("queue is nil")
	}
	log.Infof("begin to delete %s ", eq.String(queue.Name))
	if err := eq.runtimeClient.Delete(queue.Namespace, queue.Name, KubeVCQueueQuotaType); err != nil {
		log.Errorf("delete %s failed, err %v", eq.String(queue.Name), err)
		return err
	}
	return nil
}

func (eq *KubeElasticQuota) AddEventListener(ctx context.Context, listenerType string,
	eventQ workqueue.RateLimitingInterface, listener interface{}) error {
	if eventQ == nil || listener == nil {
		return fmt.Errorf("add event listener failed, err: listener is nil")
	}
	eq.workQueue = eventQ
	var err error
	switch listenerType {
	case pfschema.ListenerTypeQueue:
		informer := listener.(cache.SharedIndexInformer)
		informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    eq.add,
			UpdateFunc: eq.update,
			DeleteFunc: eq.delete,
		})
	default:
		err = fmt.Errorf("listenerType %s is not supported", listenerType)
	}
	return err
}

func (eq *KubeElasticQuota) add(obj interface{}) {
	newObj := obj.(*unstructured.Unstructured)
	name := newObj.GetName()
	// convert to ElasticResourceQuota struct
	vcQueue := &v1beta1.Queue{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(newObj.Object, vcQueue); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s failed. err: %s", obj, eq.String(name), err)
		return
	}
	qSyncInfo := &api.QueueSyncInfo{
		Name:        name,
		Action:      pfschema.Create,
		Labels:      newObj.GetLabels(),
		MaxResource: k8s.NewResource(vcQueue.Spec.Capability),
		// set vc queue status
		Status:    getVCQueueStatus(vcQueue.Status.State),
		QuotaType: pfschema.TypeVolcanoCapabilityQuota,
		Namespace: newObj.GetNamespace(),
	}
	eq.workQueue.Add(qSyncInfo)
	log.Infof("watch %s is added", eq.String(name))
}

func (eq *KubeElasticQuota) update(old, new interface{}) {
	oldObj := old.(*unstructured.Unstructured)
	newObj := new.(*unstructured.Unstructured)

	name := newObj.GetName()
	oldQ := &v1beta1.Queue{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(oldObj.Object, oldQ); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s failed. err: %s", oldObj, eq.String(name), err)
		return
	}
	newQ := &v1beta1.Queue{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(newObj.Object, newQ); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s failed. error: %s", newObj, eq.String(name), err)
		return
	}
	// Check whether vc queue is updated or not
	if reflect.DeepEqual(oldQ.Spec, newQ.Spec) && oldQ.Status.State == newQ.Status.State {
		return
	}
	qSyncInfo := &api.QueueSyncInfo{
		Name:        name,
		Action:      pfschema.Update,
		MaxResource: k8s.NewResource(newQ.Spec.Capability),
		Status:      getVCQueueStatus(newQ.Status.State),
	}
	msg := fmt.Sprintf("old queue spec: %v, new queue spec: %v", oldQ.Spec, newQ.Spec)
	eq.workQueue.Add(qSyncInfo)
	log.Infof("watch %s is updated, message: %s", eq.String(name), msg)
}

func getVCQueueStatus(state v1beta1.QueueState) string {
	status := pfschema.StatusQueueOpen
	switch state {
	case "", v1beta1.QueueStateOpen:
		status = pfschema.StatusQueueOpen
	case v1beta1.QueueStateClosing:
		status = pfschema.StatusQueueClosing
	case v1beta1.QueueStateClosed:
		status = pfschema.StatusQueueClosed
	case v1beta1.QueueStateUnknown:
		status = pfschema.StatusQueueUnavailable
	}
	return status
}

func (eq *KubeElasticQuota) delete(obj interface{}) {
	kuberuntime.QueueDeleteFunc(obj, eq.workQueue)
}
