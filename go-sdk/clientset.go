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

package go_sdk

import (
	"github.com/PaddlePaddle/PaddleFlow/go-sdk/service"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
)

type Interface interface {
	PF() service.PFInterface
}

type ClientSet struct {
	client *service.PaddleFlowClient
}

func (c ClientSet) PF() service.PFInterface {
	return c.client
}

var _ Interface = &ClientSet{}

func NewForConfig(config *core.PaddleFlowClientConfiguration) (*ClientSet, error) {
	var cs ClientSet
	var err error
	cs.client, err = service.NewForClient(config)
	if err != nil {
		return nil, err
	}
	return &cs, nil
}
