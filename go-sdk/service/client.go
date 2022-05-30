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

package service

import (
	v1 "github.com/PaddlePaddle/PaddleFlow/go-sdk/service/apiserver/v1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
)

type PFInterface interface {
	APIV1() v1.APIV1Interface
}

type PaddleFlowClient struct {
	apiV1 *v1.APIV1Client
}

// APIV1 retrieves the APIV1Client.
func (c *PaddleFlowClient) APIV1() v1.APIV1Interface {
	return c.apiV1
}

func NewForClient(config *core.PaddleFlowClientConfiguration) (*PaddleFlowClient, error) {
	var pc PaddleFlowClient
	var err error
	pc.apiV1, err = v1.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &pc, nil
}
