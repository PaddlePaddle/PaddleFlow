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

	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/rest"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

// Controller interface for kubernetes job sync, job gc, and queue sync
type Controller interface {
	Name() string
	Initialize(opt *k8s.DynamicClientOption) error
	Run(stopCh <-chan struct{})
}

func New(name string, conf *rest.Config, cluster *schema.Cluster) (Controller, error) {
	var ctrl Controller
	switch name {
	case JobSyncControllerName:
		ctrl = NewJobSync()
	case JobGCControllerName:
		ctrl = NewJobGC()
	case QueueSyncControllerName:
		ctrl = NewQueueSync()
	default:
		log.Errorf("job controller[%s] is not find", name)
		return nil, fmt.Errorf("job controller[%s] is not find", name)
	}

	opt, err := k8s.CreateDynamicClientOpt(conf, cluster)
	if err != nil {
		log.Errorf("init dynamic client failed. error: %v", err)
		return nil, err
	}
	if err = ctrl.Initialize(opt); err != nil {
		log.Errorf("init controller[%s] failed, err: %v", name, err)
		return nil, err
	}
	return ctrl, nil
}
