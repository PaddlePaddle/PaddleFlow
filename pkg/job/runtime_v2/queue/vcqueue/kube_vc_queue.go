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

package vcqueue

import (
	"context"
	"fmt"
	"reflect"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/queue/util/kuberuntime"
)

// KubeVCQueue is a struct that contains client to operate volcano queue on cluster
type KubeVCQueue struct {
	resourceVersion pfschema.KindGroupVersion
	runtimeClient   framework.RuntimeClientInterface
	workQueue       workqueue.RateLimitingInterface
}

func New(client framework.RuntimeClientInterface) framework.QueueInterface {
	return &KubeVCQueue{
		resourceVersion: pfschema.VCQueueKindGroupVersion,
		runtimeClient:   client,
	}
}

func (vcq *KubeVCQueue) String(name string) string {
	return fmt.Sprintf("%s queue %s on %s", vcq.resourceVersion, name, vcq.runtimeClient.Cluster())
}

func (vcq *KubeVCQueue) Create(ctx context.Context, q *api.QueueInfo) error {
	if q == nil {
		return fmt.Errorf("queue is nil")
	}
	// construct vc queue
	vcQueue := &v1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: q.Name,
			Annotations: map[string]string{
				pfschema.QueueNamespaceAnnotation: q.Namespace,
			},
		},
		Spec: v1beta1.QueueSpec{
			Capability: k8s.NewResourceList(q.MaxResources),
			Weight:     1,
		},
		Status: v1beta1.QueueStatus{
			State: v1beta1.QueueStateOpen,
		},
	}
	log.Debugf("Create %s, info: %#v", vcq.String(q.Name), vcQueue)
	err := vcq.runtimeClient.Create(vcQueue, vcq.resourceVersion)
	if err != nil {
		log.Errorf("Create %s falied, err: %s", vcq.String(q.Name), err)
		return err
	}
	return nil
}

func (vcq *KubeVCQueue) Update(ctx context.Context, q *api.QueueInfo) error {
	capability := k8s.NewResourceList(q.MaxResources)
	log.Debugf("UpdateQueue resourceList[%v]", capability)

	obj, err := vcq.runtimeClient.Get(q.Namespace, q.Name, vcq.resourceVersion)
	if err != nil {
		log.Errorf("get %s failed, err: %s", vcq.String(q.Name), err)
		return err
	}
	unObj := obj.(*unstructured.Unstructured)

	var vcQueue = &v1beta1.Queue{}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, vcQueue)
	if err != nil {
		log.Errorf("convert unstructured object %v to %s failed, err: %s", unObj.Object, vcq.String(q.Name), err)
		return err
	}
	vcQueue.Spec.Capability = k8s.NewResourceList(q.MaxResources)
	vcQueue.Status.State = statusToVCQueueState(q.Status)
	if vcQueue.Spec.Weight < 1 {
		vcQueue.Spec.Weight = 1
	}

	log.Infof("begin to update %s, info: %#v", vcq.String(q.Name), vcQueue)
	if err = vcq.runtimeClient.Update(vcQueue, vcq.resourceVersion); err != nil {
		log.Errorf("update %s falied. err: %s", vcq.String(q.Name), err)
		return err
	}
	return nil
}

func (vcq *KubeVCQueue) Delete(ctx context.Context, q *api.QueueInfo) error {
	if q == nil {
		return fmt.Errorf("queue is nil")
	}
	log.Infof("begin to delete %s ", vcq.String(q.Name))
	if err := vcq.runtimeClient.Delete(q.Namespace, q.Name, vcq.resourceVersion); err != nil {
		log.Errorf("delete %s failed, err %v", vcq.String(q.Name), err)
		return err
	}
	return nil
}

func (vcq *KubeVCQueue) AddEventListener(ctx context.Context, listenerType string,
	eventQ workqueue.RateLimitingInterface, listener interface{}) error {
	if eventQ == nil || listener == nil {
		return fmt.Errorf("add event listener failed, err: listener is nil")
	}
	vcq.workQueue = eventQ
	var err error
	switch listenerType {
	case pfschema.ListenerTypeQueue:
		informer := listener.(cache.SharedIndexInformer)
		informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    vcq.add,
			UpdateFunc: vcq.update,
			DeleteFunc: vcq.delete,
		})
	default:
		err = fmt.Errorf("listenerType %s is not supported", listenerType)
	}
	return err
}

func (vcq *KubeVCQueue) add(obj interface{}) {
	newObj := obj.(*unstructured.Unstructured)
	name := newObj.GetName()
	namespace := newObj.GetAnnotations()[pfschema.QueueNamespaceAnnotation]
	if namespace == "" {
		namespace = "default"
	}
	// convert to vc queue struct
	vcQueue := &v1beta1.Queue{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(newObj.Object, vcQueue); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s failed. err: %s", obj, vcq.String(name), err)
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
		Namespace: namespace,
	}
	vcq.workQueue.Add(qSyncInfo)
	log.Infof("watch %s is added", vcq.String(name))
}

func (vcq *KubeVCQueue) update(old, new interface{}) {
	oldObj := old.(*unstructured.Unstructured)
	newObj := new.(*unstructured.Unstructured)

	name := newObj.GetName()
	oldQ := &v1beta1.Queue{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(oldObj.Object, oldQ); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s failed. err: %s", oldObj, vcq.String(name), err)
		return
	}
	newQ := &v1beta1.Queue{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(newObj.Object, newQ); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s failed. error: %s", newObj, vcq.String(name), err)
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
	vcq.workQueue.Add(qSyncInfo)
	log.Infof("watch %s is updated, message: %s", vcq.String(name), msg)
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

// statusToVCQueueState converts vc queue status to volcano queue state
func statusToVCQueueState(status string) v1beta1.QueueState {
	state := v1beta1.QueueStateOpen
	switch status {
	case pfschema.StatusQueueOpen:
		state = v1beta1.QueueStateOpen
	case pfschema.StatusQueueClosing:
		state = v1beta1.QueueStateClosing
	case pfschema.StatusQueueClosed:
		state = v1beta1.QueueStateClosed
	case pfschema.StatusQueueUnavailable:
		state = v1beta1.QueueStateUnknown
	}
	return state
}

func (vcq *KubeVCQueue) delete(obj interface{}) {
	kuberuntime.QueueDeleteFunc(obj, vcq.workQueue)
}
