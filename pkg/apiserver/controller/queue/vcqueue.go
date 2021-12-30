/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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
	"strings"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"
	busv1alpha1 "volcano.sh/apis/pkg/apis/bus/v1alpha1"
	"volcano.sh/apis/pkg/apis/helpers"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"

	"paddleflow/pkg/apiserver/models"
)

type VCQueue struct {
	client *vcclientset.Clientset
	cache  *Cache
}

func NewVCQueue(client *vcclientset.Clientset) *VCQueue {
	qc, err := NewQueueCache(client)
	if err != nil {
		log.Errorf("new vc queue failed. ")
		return nil
	}
	q := VCQueue{
		client: client,
		cache:  qc,
	}
	return &q
}

func (vcq *VCQueue) Run(stopCh <-chan struct{}) {
	log.Debugf("VCQueue begin to run")
	go vcq.cache.vcQueueInformer.Informer().Run(stopCh)
	if !toolscache.WaitForNamedCacheSync("VCQueue", stopCh,
		vcq.cache.vcQueueInformer.Informer().HasSynced) {
		log.Errorln("VCQueue WaitForNamedCacheSync failed")
		return
	}
	log.Debugf("VCQueue sync succeed")
}

func (vcq *VCQueue) CreateQueue(q *models.Queue) error {
	resourceList := v1.ResourceList{}
	resourceList[v1.ResourceCPU] = resource.MustParse(q.Cpu)
	resourceList[v1.ResourceMemory] = resource.MustParse(q.Mem)
	for k, v := range q.ScalarResources {
		resourceList[k] = resource.MustParse(v)
	}
	log.Debugf("VCQueue CreateQueue resourceList[%v]", resourceList)

	queue := &schedulingv1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: q.Name,
		},
		Spec: schedulingv1beta1.QueueSpec{
			Capability: resourceList,
		},
		Status: schedulingv1beta1.QueueStatus{
			State: schedulingv1beta1.QueueStateOpen,
		},
	}
	log.Debugf("CreateQueue queue info:%#v", queue)
	if _, err := vcq.client.SchedulingV1beta1().Queues().Create(context.TODO(), queue, metav1.CreateOptions{}); err != nil {
		log.Errorf("CreateQueue error. queueName:[%s], error:[%s]",
			q.Name, err.Error())
		return err
	}
	return nil
}

func (vcq *VCQueue) DeleteQueue(q *models.Queue) error {
	err := vcq.client.SchedulingV1beta1().Queues().Delete(context.TODO(), q.Name, metav1.DeleteOptions{})
	if err != nil {
		log.Errorf("DeleteQueue error. queueName:[%s], error:[%s]", q.Name, err.Error())
		return err
	}
	return nil
}

func (vcq *VCQueue) executeQueueAction(q *models.Queue, action busv1alpha1.Action) error {
	queue, err := vcq.client.SchedulingV1beta1().Queues().Get(context.TODO(),
		q.Name, metav1.GetOptions{})
	if err != nil {
		log.Errorf("execute queue action get queue failed. queueName:[%s]", q.Name)
		return err
	}

	ctrlRef := metav1.NewControllerRef(queue, helpers.V1beta1QueueKind)
	cmd := &busv1alpha1.Command{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-%s-",
				queue.Name, strings.ToLower(string(action))),
			OwnerReferences: []metav1.OwnerReference{
				*ctrlRef,
			},
		},
		TargetObject: ctrlRef,
		Action:       string(action),
	}

	if _, err := vcq.client.BusV1alpha1().Commands("default").Create(context.TODO(), cmd,
		metav1.CreateOptions{}); err != nil {
		log.Errorf("execute queue action failed. queueName:[%s] err:[%s]", q.Name, err.Error())
		return err
	}
	return nil
}

func (vcq *VCQueue) CloseQueue(q *models.Queue) error {
	return vcq.executeQueueAction(q, busv1alpha1.CloseQueueAction)
}

func (vcq *VCQueue) OpenQueue(q *models.Queue) error {
	return vcq.executeQueueAction(q, busv1alpha1.OpenQueueAction)
}
