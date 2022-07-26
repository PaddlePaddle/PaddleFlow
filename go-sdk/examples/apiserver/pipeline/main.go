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

package main

import (
	"context"
	"fmt"

	"github.com/PaddlePaddle/PaddleFlow/go-sdk/service"
	v1 "github.com/PaddlePaddle/PaddleFlow/go-sdk/service/apiserver/v1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
)

func getToken(pfClient *service.PaddleFlowClient) string {
	data, err := pfClient.APIV1().User().Login(context.TODO(), &v1.LoginInfo{
		UserName: "root",       // debug: test
		Password: "paddleflow", // debug: test
	})
	if err != nil {
		panic(err)
	}
	token := data.Authorization

	return token
}

func create(pfClient *service.PaddleFlowClient, token string) (createResult *v1.CreatePipelineResponse) {
	request := &v1.CreatePipelineRequest{
		FsName:   "",
		YamlPath: "",
		Name:     "",
	}

	createResult, err := pfClient.APIV1().Pipeline().Create(context.TODO(), request, token)

	if err != nil {
		panic(err)
	}

	fmt.Println(createResult.ID, createResult.Name)
	return
}

func Get(pfClient *service.PaddleFlowClient, token, pipelineID string) (result *v1.GetPipelineResponse) {
	result, err := pfClient.APIV1().Pipeline().Get(context.TODO(), pipelineID, token)
	if err != nil {
		panic(err)
	}

	fmt.Println(result.Name, result.UserName, result.FsName)
	fmt.Println(result.FsName, result.PipelineYaml)
	return result
}

func List(pfClient *service.PaddleFlowClient, token string) (result *v1.ListPipelineResponse) {
	request := &v1.ListPipelineRequest{
		MaxKeys:    10,
		UserFilter: []string{""},
		FsFilter:   []string{"", ""},
	}

	result, err := pfClient.APIV1().Pipeline().List(context.TODO(), request, token)
	if err != nil {
		panic(err)
	}

	for _, p := range result.PipelineList {
		fmt.Println(p.ID, p.UserName, p.FsName, p.Name)
	}

	fmt.Println(result.MarkerInfo.NextMarker)
	return result
}

func Delete(pfClient *service.PaddleFlowClient, pipelineID, token string) {
	err := pfClient.APIV1().Pipeline().Delete(context.TODO(), pipelineID, token)
	if err != nil {
		panic(err)
	}
}

func main() {
	config := &core.PaddleFlowClientConfiguration{
		Host:                       "http://gzbh-bos-aries-r104-178546850.gzbh.baidu.com", // debug: test
		Port:                       8999,
		ConnectionTimeoutInSeconds: 1,
	}
	pfClient, err := service.NewForClient(config)

	if err != nil {
		panic(err)
	}

	token := getToken(pfClient)
	// create(pfClient, token)

	// Get(pfClient, token, "")

	// List(pfClient, token)
	Delete(pfClient, "", token)

	Get(pfClient, token, "")
}
