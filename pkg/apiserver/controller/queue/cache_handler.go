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
	vcqueue "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"paddleflow/pkg/apiserver/models"
)

func (qc *Cache) addQueueCache(obj interface{}) {
	queue := obj.(*vcqueue.Queue)
	log.Debugf("vcQueueInformer AddFunc. queue:%s", queue.Name)
}

func (qc *Cache) updateQueueCache(oldObj, newObj interface{}) {
	oldQueue := oldObj.(*vcqueue.Queue)
	queue := newObj.(*vcqueue.Queue)
	log.Debugf("vcQueueInformer update begin. oldQueue:%v newQueue:%v", oldQueue, queue)
	if oldQueue.Status == queue.Status {
		return
	}
	err := models.UpdateQueueStatus(queue.Name, string(queue.Status.State))
	if err != nil {
		log.Errorf("queueInformer update queue status failed. queueName:[%s] error:[%s]", queue.Name, err.Error())
	}
}

func (qc *Cache) deleteQueueCache(obj interface{}) {
	queue := obj.(*vcqueue.Queue)
	log.Debugf("vcQueueInformer DeleteFunc. queueName:%s", queue.Name)
}
