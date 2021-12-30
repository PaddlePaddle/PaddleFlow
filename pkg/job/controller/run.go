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

package controller

import (
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	framework2 "paddleflow/pkg/job/controller/framework"
	_ "paddleflow/pkg/job/controller/job_gc"
	_ "paddleflow/pkg/job/controller/job_sync"
)

func Run(config *rest.Config, stopCh <-chan struct{}) error {
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Errorf("Init dynamic client failed. error:%s", err.Error())
		return err
	}
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0)
	controllerOpt := framework2.ControllerOption{DynamicClient: dynamicClient, DynamicFactory: factory}
	framework2.ForeachController(func(c framework2.Controller) {
		if err := c.Initialize(&controllerOpt); err != nil {
			log.Errorf("Failed to initialize controller <%s>: %v", c.Name(), err)
			return
		}
		go c.Run(stopCh)
	})
	return nil
}
