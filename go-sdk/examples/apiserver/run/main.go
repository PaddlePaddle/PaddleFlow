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
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func getToken(pfClient *service.PaddleFlowClient) string {
	data, err := pfClient.APIV1().User().Login(context.TODO(), &v1.LoginInfo{
		UserName: "root",       //debug: test
		Password: "paddleflow", // debug: test
	})
	if err != nil {
		panic(err)
	}
	token := data.Authorization

	return token
}

func create(pfClient *service.PaddleFlowClient, token string) (createResult *v1.CreateRunResponse) {
	request := &v1.CreateRunRequest{
		FsName:      "cyang14", //debug: test
		Description: "",
		RunYamlPath: "143/runDag.yaml", //debug: test
		UserName:    "",
	}

	createResult, err := pfClient.APIV1().Run().Create(context.TODO(), request, token)

	if err != nil {
		panic(err)
	}
	fmt.Printf("create Run result %v\n", createResult)
	return
}

func createByJson(pfClient *service.PaddleFlowClient, token string) (createResult *v1.CreateRunResponse) {
	main := &v1.RunStep{
		Command:   "",
		DockerEnv: "centos:centos7",
		Env: map[string]string{
			"PF_JOB_MODE": "Pod",
		},
	}

	main2 := &v1.RunStep{
		DockerEnv: "centos:centos7",
		Deps:      "main",
		Command:   "echo 111",
		Env: map[string]string{
			"PF_JOB_MODE": "Pod",
		},
	}

	request := map[string]interface{}{
		"name":    "test",
		"jobType": "vcjob",
		"queue":   "wf-queue",
		"flavour": "flavour1",
		"entryPoints": map[string]*v1.RunStep{ //这里有问题需要修改
			"main":  main,
			"main2": main2,
		},
		"failureOptions": schema.FailureOptions{
			Strategy: "continue",
		},
	}

	createResult, err := pfClient.APIV1().Run().CreateByJson(context.TODO(), request, token)
	if err != nil {
		panic(err)
	}

	return createResult
}

func Get(pfClient *service.PaddleFlowClient, token, runID string) (result *v1.GetRunResponse) {
	result, err := pfClient.APIV1().Run().Get(context.TODO(), runID, token)
	if err != nil {
		panic(err)
	}

	fmt.Println(result.Status)
	fmt.Println(result.FsName, result.Source)
	return result
}

func List(pfClient *service.PaddleFlowClient, token string) (result *v1.ListRunResponse) {
	request := &v1.ListRunRequest{
		MaxKeys:    10,
		UserFilter: []string{"root"},
		FsFilter:   []string{"cyang14"},
	}

	result, err := pfClient.APIV1().Run().List(context.TODO(), request, token)
	if err != nil {
		panic(err)
	}

	for _, r := range result.RunList {
		fmt.Println(r.ID, r.UserName, r.FsName, r.Name)
	}

	fmt.Println(result.MarkerInfo.NextMarker)
	return result
}

func Stop(pfClient *service.PaddleFlowClient, token, runID string) {
	err := pfClient.APIV1().Run().Stop(context.TODO(), false, runID, token)
	if err != nil {
		panic(err)
	}
}

func Retry(pfClient *service.PaddleFlowClient, token, runID string) *v1.RetryRunResponse {
	result, err := pfClient.APIV1().Run().Retry(context.TODO(), runID, token)
	if err != nil {
		panic(err)
	}
	return result
}

func Delete(pfClient *service.PaddleFlowClient, token, runID string) {
	err := pfClient.APIV1().Run().Delete(context.TODO(), runID, token)
	if err != nil {
		panic(err)
	}
}

func main() {
	config := &core.PaddleFlowClientConfiguration{
		Host:                       "gzbh-bos-aries-r104-178546850.gzbh.baidu.com", // debug: test
		Port:                       8999,
		ConnectionTimeoutInSeconds: 1,
	}
	pfClient, err := service.NewForClient(config)

	if err != nil {
		panic(err)
	}

	token := getToken(pfClient)
	// createResult := create(pfClient, token)
	// fmt.Println(createResult)
	// Stop(pfClient, token, createResult.RunID)
	// time.Sleep(time.Duration(20) * time.Second)
	res := Retry(pfClient, token, "run-000024")

	fmt.Println(res.RunID)

	// createResult := createByJson(pfClient, token)
	// getResult := Get(pfClient, token, "run-000014")
	// resJson, _ := json.Marshal(getResult.Runtime)
	// fmt.Println("runtime: ", string(resJson))
	// Delete(pfClient, token, "")
	// Get(pfClient, token, "")

	// List(pfClient, token)
}
