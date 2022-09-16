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

package kuberuntime

import (
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/workqueue"

	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
)

func QueueDeleteFunc(obj interface{}, workQueue workqueue.RateLimitingInterface) {
	if obj == nil || workQueue == nil {
		log.Warnf("queue obj or workQueue is nil ")
		return
	}
	unObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		log.Warnf("convert obj to unstructed failed")
		return
	}
	qSyncInfo := &api.QueueSyncInfo{
		Name:   unObj.GetName(),
		Action: pfschema.Delete,
		Status: pfschema.StatusQueueUnavailable,
	}
	workQueue.Add(qSyncInfo)
	log.Infof("watch queue %s is deleted, type is %s", unObj.GetName(), unObj.GroupVersionKind())
}
