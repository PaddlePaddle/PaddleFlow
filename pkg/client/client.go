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

package client

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
)

var Client *core.PaddleFlowClient

const (
	DefaultTimeOut = 200
)

func NewHttpClient(server string, timeout int) (*core.PaddleFlowClient, error) {
	server = strings.TrimPrefix(server, "http://")
	arr := strings.Split(server, ":")
	if len(arr) != 2 {
		log.Errorf("NewHttpClient: malformat server(ip:port) [%s]", server)
		return nil, fmt.Errorf("NewHttpClient: malformat server(ip:port) [%s]", server)
	}
	port, _ := strconv.Atoi(arr[1])
	if Client == nil {
		Client = core.NewPaddleFlowClient(&core.PaddleFlowClientConfiguration{
			Host:                       arr[0],
			Port:                       port,
			ConnectionTimeoutInSeconds: timeout,
		})
	}
	return Client, nil
}
