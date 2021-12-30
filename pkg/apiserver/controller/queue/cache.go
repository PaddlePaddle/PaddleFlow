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
	log "github.com/sirupsen/logrus"
	toolscache "k8s.io/client-go/tools/cache"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	vcinformer "volcano.sh/apis/pkg/client/informers/externalversions"
	vcqueueinformer "volcano.sh/apis/pkg/client/informers/externalversions/scheduling/v1beta1"
)

type Cache struct {
	vcQueueInformer vcqueueinformer.QueueInformer
	client          *vcclientset.Clientset
}

func (qc *Cache) Initialize(client *vcclientset.Clientset) error {
	qc.client = client
	qc.vcQueueInformer = vcinformer.NewSharedInformerFactory(client, 0).Scheduling().V1beta1().Queues()
	qc.vcQueueInformer.Informer().AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    qc.addQueueCache,
		UpdateFunc: qc.updateQueueCache,
		DeleteFunc: qc.deleteQueueCache,
	})
	return nil
}

func NewQueueCache(client *vcclientset.Clientset) (*Cache, error) {
	qc := Cache{}
	err := qc.Initialize(client)
	if err != nil {
		log.Errorf("new queue cache initialize failed. err:[%s]", err.Error())
		return nil, err
	}
	return &qc, nil
}
