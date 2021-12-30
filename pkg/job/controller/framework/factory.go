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

package framework

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

var controllers = map[string]Controller{}

func RegisterController(ctrl Controller) error {
	if ctrl == nil {
		return fmt.Errorf("register controller input controller is nil")
	}
	if _, found := controllers[ctrl.Name()]; found {
		log.Infof("job controller[%s] has alread registered", ctrl.Name())
	} else {
		controllers[ctrl.Name()] = ctrl
	}
	log.Infof("register job controller[%s] succeed.", ctrl.Name())
	return nil
}

func ForeachController(fn func(controller Controller)) {
	for _, ctrl := range controllers {
		log.Infof("begin to init controller[%s]", ctrl.Name())
		fn(ctrl)
	}
}
