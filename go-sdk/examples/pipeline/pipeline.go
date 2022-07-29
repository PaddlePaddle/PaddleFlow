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

func create(pfClient *service.PaddleFlowClient, runYamlRaw, token string) (createResult *v1.CreateRunResponse) {
	request := &v1.CreateRunRequest{
		FsName:     "",
		RunYamlRaw: runYamlRaw,
	}

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

	result := create(pfClient, runYamlRaw, token)

	fmt.Println(result)
	return result.RunID
}

func main() {
	GetPipelineFromFile("")
	CreateRunByRunYamlRaw("")
}
