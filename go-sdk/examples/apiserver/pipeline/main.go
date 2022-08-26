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
	"encoding/json"
	"fmt"

	"github.com/PaddlePaddle/PaddleFlow/go-sdk/service"
	v1 "github.com/PaddlePaddle/PaddleFlow/go-sdk/service/apiserver/v1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
)

func getToken(pfClient *service.PaddleFlowClient) string {
	data, err := pfClient.APIV1().User().Login(context.TODO(), &v1.LoginInfo{
		UserName: "",
		Password: "",
	})
	if err != nil {
		panic(err)
	}
	token := data.Authorization

	return token
}

func create(pfClient *service.PaddleFlowClient, request *v1.CreatePipelineRequest, token string) (createResult *v1.CreatePipelineResponse) {
	createResult, err := pfClient.APIV1().Pipeline().Create(context.TODO(), request, token)

	if err != nil {
		panic(err)
	}
	fmt.Println("result of create pipeline: ", createResult.PipelineID, createResult.PipelineVersionID)
	return
}

func Get(pfClient *service.PaddleFlowClient, token string, request *v1.GetPipelineRequest) (result *v1.GetPipelineResponse) {
	result, err := pfClient.APIV1().Pipeline().Get(context.TODO(), request, token)
	if err != nil {
		panic(err)
	}
	return result
}

func List(pfClient *service.PaddleFlowClient, request *v1.ListPipelineRequest, token string) (result *v1.ListPipelineResponse) {
	result, err := pfClient.APIV1().Pipeline().List(context.TODO(), request, token)
	if err != nil {
		panic(err)
	}

	fmt.Println("IDs in result of list pipeline: ")
	for _, p := range result.PipelineList {
		fmt.Println(p.ID)
	}

	return result
}

func Delete(pfClient *service.PaddleFlowClient, pipelineID, token string) {
	err := pfClient.APIV1().Pipeline().Delete(context.TODO(), pipelineID, token)
	if err != nil {
		panic(err)
	}
}

func Update(pfClient *service.PaddleFlowClient, pipelineID string, request *v1.UpdatePipelineRequest, token string) (result *v1.UpdatePipelineResponse) {
	result, err := pfClient.APIV1().Pipeline().Update(context.TODO(), pipelineID, request, token)
	if err != nil {
		panic(err)
	}
	return
}

func GetVersion(pfClient *service.PaddleFlowClient, pipelineID, pipelineVersionID,
	token string) (result *v1.GetPipelineVersionResponse) {
	result, err := pfClient.APIV1().Pipeline().GetVersion(context.TODO(), pipelineID, pipelineVersionID, token)
	if err != nil {
		panic(err)
	}
	return
}

func DeleteVersion(pfClient *service.PaddleFlowClient, pipelineID, pipelineVersionID, token string) (err error) {
	err = pfClient.APIV1().Pipeline().DeleteVersion(context.TODO(), pipelineID, pipelineVersionID, token)
	if err != nil {
		panic(err)
	}
	return
}

func main() {
	config := &core.PaddleFlowClientConfiguration{
		Host:                       "",
		Port:                       8999,
		ConnectionTimeoutInSeconds: 1,
	}
	pfClient, err := service.NewForClient(config)

	if err != nil {
		panic(err)
	}

	token := getToken(pfClient)
	reqCreate := &v1.CreatePipelineRequest{
		FsName:   "",
		YamlPath: "",
	}
	create(pfClient, reqCreate, token)

	reqGet := &v1.GetPipelineRequest{
		PipelineID: "ppl-000004",
		MaxKeys:    1,
	}

	resGet := Get(pfClient, token, reqGet)
	resJson, _ := json.Marshal(resGet)
	fmt.Println(string(resJson))

	reqUpdate := &v1.UpdatePipelineRequest{
		FsName:   "",
		YamlPath: "",
	}
	resUpdate := Update(pfClient, "ppl-000001", reqUpdate, token)
	fmt.Println(resUpdate.PipelineVersionID)

	res := GetVersion(pfClient, "ppl-000001", "1", token)
	fmt.Println(res.PipelineVersion.PipelineYaml)

	DeleteVersion(pfClient, "ppl-000001", "2", token)
	resDeleteVer := Get(pfClient, token, reqGet)
	resJson, _ = json.Marshal(resDeleteVer.PipelineVersions)
	fmt.Println(string(resJson))

	reqList := &v1.ListPipelineRequest{}
	List(pfClient, reqList, token)
	Delete(pfClient, "ppl-000001", token)
	List(pfClient, reqList, token)
}
