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
	"github.com/PaddlePaddle/PaddleFlow/go-sdk/service/apiserver/v1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

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
	data, err := pfClient.APIV1().User().Login(context.TODO(), &v1.LoginInfo{
		UserName: "",
		Password: "",
	})
	if err != nil {
		panic(err)
	}
	token := data.Authorization

	flavourName := "test-flavour"
	createResult, err := pfClient.APIV1().Flavour().Create(context.TODO(), &v1.CreateFlavourRequest{
		Name: flavourName,
		CPU:  "4",
		Mem:  "8Gi",
		ScalarResources: map[schema.ResourceName]string{
			"nvidia.com/gpu": "1",
		},
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("create flavour result %v\n", createResult)

	getResult, err := pfClient.APIV1().Flavour().Get(context.TODO(), flavourName, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("get flavour result %v\n", getResult)

	listResult, err := pfClient.APIV1().Flavour().List(context.TODO(), &v1.ListFlavourRequest{
		MaxKeys: 100,
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("list flavour result %v\n", listResult)

	updateResult, err := pfClient.APIV1().Flavour().Update(context.TODO(), &v1.UpdateFlavourRequest{
		Name: flavourName,
		CPU:  "8",
		Mem:  "10Gi",
		ScalarResources: map[schema.ResourceName]string{
			"nvidia.com/gpu": "2",
		},
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("update flavour result %v\n", updateResult)

	err = pfClient.APIV1().Flavour().Delete(context.TODO(), flavourName, token)
	if err != nil {
		panic(err)
	}
	fmt.Println("delete flavour ok")
}
