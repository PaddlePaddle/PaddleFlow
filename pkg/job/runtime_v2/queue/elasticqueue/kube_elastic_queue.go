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

package elasticqueue

import (
	"context"
	"fmt"
	"reflect"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/queue/util/kuberuntime"
)

var (
	QueueGVK             = k8s.EQuotaGVK
	KubeElasticQuotaType = client.KubeFrameworkVersion(QueueGVK)
)

// KubeElasticQuota is an executor struct that runs a single pod
type KubeElasticQuota struct {
	GVK             schema.GroupVersionKind
	resourceVersion pfschema.FrameworkVersion
	runtimeClient   framework.RuntimeClientInterface
	workQueue       workqueue.RateLimitingInterface
}

func New(client framework.RuntimeClientInterface) framework.QueueInterface {
	return &KubeElasticQuota{
		resourceVersion: KubeElasticQuotaType,
		runtimeClient:   client,
		GVK:             QueueGVK,
	}
}

func (eq *KubeElasticQuota) String(name string) string {
	return fmt.Sprintf("%s queue %s on %s", eq.GVK.String(), name, eq.runtimeClient.Cluster())
}

func (eq *KubeElasticQuota) Create(ctx context.Context, q *api.QueueInfo) error {
	if q == nil {
		return fmt.Errorf("queue is nil")
	}
	// construct elastic resource quota
	equota := &v1beta1.ElasticResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:   q.Name,
			Labels: q.Location,
		},
		Spec: v1beta1.ElasticResourceQuotaSpec{
			Max:         k8s.NewResourceList(q.MaxResources),
			Min:         k8s.NewResourceList(q.MinResources),
			Namespace:   q.Namespace,
			Reclaimable: true,
		},
	}
	log.Debugf("Create %s, info: %#v", eq.String(q.Name), equota)
	err := eq.runtimeClient.Create(equota, eq.resourceVersion)
	if err != nil {
		log.Errorf("Create %s falied, err: %s", eq.String(q.Name), err)
		return err
	}
	return nil
}

func (eq *KubeElasticQuota) Update(ctx context.Context, q *api.QueueInfo) error {
	if q == nil {
		return fmt.Errorf("queue is nil")
	}
	// get elastic quota from cluster
	obj, err := eq.runtimeClient.Get(q.Namespace, q.Name, eq.resourceVersion)
	if err != nil {
		log.Errorf("get %s failed, err: %s", eq.String(q.Name), err)
		return err
	}
	unObj := obj.(*unstructured.Unstructured)

	equota := &v1beta1.ElasticResourceQuota{}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, equota)
	if err != nil {
		log.Errorf("convert unstructured object %v to %s failed, err: %s", unObj.Object, eq.String(q.Name), err)
		return err
	}

	equota.Spec.Max = k8s.NewResourceList(q.MaxResources)
	equota.Spec.Min = k8s.NewResourceList(q.MinResources)
	equota.Spec.Namespace = q.Namespace
	// update labels
	if equota.Labels == nil {
		equota.Labels = make(map[string]string)
	}
	newLabels := make(map[string]string)
	for key, v := range q.Location {
		newLabels[key] = v
	}
	equota.Labels = newLabels

	log.Infof("begin to update %s, info: %#v", eq.String(q.Name), equota)
	if err = eq.runtimeClient.Update(equota, eq.resourceVersion); err != nil {
		log.Errorf("update %s falied. err: %s", eq.String(q.Name), err)
		return err
	}
	return nil
}

func (eq *KubeElasticQuota) Delete(ctx context.Context, q *api.QueueInfo) error {
	if q == nil {
		return fmt.Errorf("queue is nil")
	}
	log.Infof("begin to delete %s ", eq.String(q.Name))
	if err := eq.runtimeClient.Delete(q.Namespace, q.Name, eq.resourceVersion); err != nil {
		log.Errorf("delete %s failed, err %v", eq.String(q.Name), err)
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
	eQuota := &v1beta1.ElasticResourceQuota{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(newObj.Object, eQuota); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s failed. err: %s", obj, eq.String(name), err)
		return
	}
	qSyncInfo := &api.QueueSyncInfo{
		Name:        name,
		Action:      pfschema.Create,
		Labels:      newObj.GetLabels(),
		MaxResource: k8s.NewResource(eQuota.Spec.Max),
		MinResource: k8s.NewResource(eQuota.Spec.Min),
		// set elastic resource quota status
		Status:    getEQuotaStatus(eQuota.Status),
		QuotaType: pfschema.TypeElasticQuota,
		Namespace: eQuota.Spec.Namespace,
	}
	eq.workQueue.Add(qSyncInfo)
	log.Infof("watch %s is added", eq.String(name))
}

func (eq *KubeElasticQuota) update(old, new interface{}) {
	oldObj := old.(*unstructured.Unstructured)
	newObj := new.(*unstructured.Unstructured)

	name := newObj.GetName()
	oldEQuota := &v1beta1.ElasticResourceQuota{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(oldObj.Object, oldEQuota); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s failed. err: %s", oldObj, eq.String(name), err)
		return
	}
	newEQuota := &v1beta1.ElasticResourceQuota{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(newObj.Object, newEQuota); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s failed. err: %s", newObj, eq.String(name), err)
		return
	}
	// Check whether elastic quota is updated or not
	if reflect.DeepEqual(oldEQuota.Spec, newEQuota.Spec) && oldEQuota.Status.IsLeaf == newEQuota.Status.IsLeaf {
		return
	}
	qSyncInfo := &api.QueueSyncInfo{
		Name:        name,
		Action:      pfschema.Update,
		MaxResource: k8s.NewResource(newEQuota.Spec.Max),
		MinResource: k8s.NewResource(newEQuota.Spec.Min),
		Status:      getEQuotaStatus(newEQuota.Status),
	}
	msg := fmt.Sprintf("old queue spec: %v, new queue spec: %v", oldEQuota.Spec, newEQuota.Spec)
	eq.workQueue.Add(qSyncInfo)
	log.Infof("watch %s is updated, message: %s", eq.String(name), msg)
}

func getEQuotaStatus(state v1beta1.ElasticResourceQuotaStatus) string {
	status := pfschema.StatusQueueOpen
	if !state.IsLeaf {
		status = pfschema.StatusQueueClosed
	}
	return status
}

func (eq *KubeElasticQuota) delete(obj interface{}) {
	kuberuntime.QueueDeleteFunc(obj, eq.workQueue)
}
