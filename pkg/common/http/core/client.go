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

package core

import (
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
)

// Client is the general interface which can perform sending request. Different service
// will define its own client in case of specific extension.
type Client interface {
	SendRequest(*PFRequest) (*PFResponse, error)
}

type PaddleFlowClient struct {
	Config *PaddleFlowClientConfiguration
}

func (c *PaddleFlowClient) buildHttpRequest(request *PFRequest) {
	request.BuildHttpRequest()
	request.SetHost(c.Config.Host)
	request.SetPort(c.Config.Port)
	request.SetTimeout(c.Config.ConnectionTimeoutInSeconds)
	// TODO : add signer
}

func (c *PaddleFlowClient) SendRequest(req *PFRequest) (*PFResponse, error) {
	// Build the http request and prepare to send
	c.buildHttpRequest(req)
	log.Debugf("send http request: %v", req)

	httpResp, err := http.Execute(&req.Request)
	if err != nil {
		return nil, err
	}

	resp := &PFResponse{}
	resp.SetHttpResponse(httpResp)
	resp.ParseResponse()
	return resp, nil
}

func NewPaddleFlowClient(conf *PaddleFlowClientConfiguration) *PaddleFlowClient {
	http.InitClient()
	return &PaddleFlowClient{conf}
}
