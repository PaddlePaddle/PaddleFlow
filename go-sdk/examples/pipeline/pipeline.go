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

	"github.com/PaddlePaddle/PaddleFlow/go-sdk/pipeline"
	"github.com/PaddlePaddle/PaddleFlow/go-sdk/service"
	v1 "github.com/PaddlePaddle/PaddleFlow/go-sdk/service/apiserver/v1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func GetPipelineFromFile(filepath string) (p *pipeline.Pipeline) {
	p, err := pipeline.NewPipelineFromYamlFile(filepath)
	if err != nil {
		panic(err)
	}
	fmt.Println(p.Cache)

	for name, step := range p.EntryPoints.EntryPoints {
		fmt.Println(name)
		fmt.Println("input", step.GetArtifacts().Input)
		fmt.Println("output", step.GetArtifacts().Output)
	}

	return p
}

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

func createRun(pfClient *service.PaddleFlowClient, token string, request *v1.CreateRunRequest) (createResult *v1.CreateRunResponse) {
	createResult, err := pfClient.APIV1().Run().Create(context.TODO(), request, token)

	if err != nil {
		panic(err)
	}
	fmt.Printf("create Run result %v\n", createResult)
	return
}

func CreateRunByRunYamlRaw(filepath string) (runID string) {
	p, err := pipeline.NewPipelineFromYamlFile(filepath)
	if err != nil {
		panic(err)
	}

	runYamlRaw, err := p.TransToRunYamlRaw()
	if err != nil {
		panic(err)
	}

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

	request := &v1.CreateRunRequest{
		FsName:     "ppl",
		RunYamlRaw: runYamlRaw,
	}

	result := createRun(pfClient, token, request)

	fmt.Println(result)
	return result.RunID
}

func CreateRunSpecifyFailureOptions(filepath string, failStrategy string) (runID string) {
	p, err := pipeline.NewPipelineFromYamlFile(filepath)
	if err != nil {
		panic(err)
	}

	runYamlRaw, err := p.TransToRunYamlRaw()
	if err != nil {
		panic(err)
	}

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

	request := &v1.CreateRunRequest{
		FsName:         "ppl",
		RunYamlRaw:     runYamlRaw,
		FailureOptions: &schema.FailureOptions{Strategy: failStrategy},
	}

	result := createRun(pfClient, token, request)

	fmt.Println(result)
	return result.RunID
}

func createPipeline(pfClient *service.PaddleFlowClient, token string, request *v1.CreatePipelineRequest) (pplID string) {
	createResult, err := pfClient.APIV1().Pipeline().Create(context.TODO(), request, token)

	if err != nil {
		panic(err)
	}
	fmt.Printf("create Run result %v\n", createResult)
	return
}

func CreatePipelineByRaw(filepath string) (pplID string) {
	p, err := pipeline.NewPipelineFromYamlFile(filepath)
	if err != nil {
		panic(err)
	}

	YamlRaw, err := p.TransToRunYamlRaw()
	if err != nil {
		panic(err)
	}

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

	request := &v1.CreatePipelineRequest{
		YamlRaw: YamlRaw,
	}

	result := createPipeline(pfClient, token, request)

	fmt.Println(result)
	return result
}

func updatePipeline(pfClient *service.PaddleFlowClient, pipelineID, token string, request *v1.CreatePipelineRequest) (pplID, pplVerID string) {
	updateResult, err := pfClient.APIV1().Pipeline().Update(context.TODO(), pipelineID, request, token)

	if err != nil {
		panic(err)
	}
	fmt.Printf("create Run result %v\n", updateResult)
	return updateResult.PipelineID, updateResult.PipelineVersionID
}

func UpdatePipelineByRaw(filepath string, pplID string) (string, string) {
	p, err := pipeline.NewPipelineFromYamlFile(filepath)
	if err != nil {
		panic(err)
	}

	YamlRaw, err := p.TransToRunYamlRaw()
	if err != nil {
		panic(err)
	}

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

	request := &v1.CreatePipelineRequest{
		YamlRaw: YamlRaw,
	}

	pplID, pplVerID := updatePipeline(pfClient, pplID, token, request)

	fmt.Println(pplID, pplVerID)
	return pplID, pplVerID
}

func main() {
	GetPipelineFromFile("")
	CreateRunByRunYamlRaw("")
	CreatePipelineByRaw("")
	UpdatePipelineByRaw("", "ppl-000096")
	CreateRunSpecifyFailureOptions("", schema.FailureStrategyFailFast)
}
