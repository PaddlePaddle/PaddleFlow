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
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/queue"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/user"
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
	data, err := pfClient.APIV1().User().Login(context.TODO(), &user.LoginInfo{
		UserName: "",
		Password: "",
	})
	if err != nil {
		panic(err)
	}
	token := data.Authorization

	queueName := "test-queue"
	clusterName := "test-cluster"
	createResult, err := pfClient.APIV1().Queue().Create(context.TODO(), &queue.CreateQueueRequest{
		Name:        queueName,
		Namespace:   "default",
		ClusterName: clusterName,
		MaxResources: schema.ResourceInfo{
			CPU: "30",
			Mem: "50Gi",
		},
		MinResources: schema.ResourceInfo{
			CPU: "10",
			Mem: "20Gi",
		},
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("create queue result %v\n", createResult)

	getResult, err := pfClient.APIV1().Queue().Get(context.TODO(), queueName, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("get queue result %v\n", getResult)

	listResult, err := pfClient.APIV1().Queue().List(context.TODO(), &queue.ListQueueRequest{
		MaxKeys: 100,
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("list queue result %v\n", listResult)

	updateResult, err := pfClient.APIV1().Queue().Update(context.TODO(), queueName, &queue.UpdateQueueRequest{
		Location:         map[string]string{},
		SchedulingPolicy: []string{"priority"},
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("update queue result %v\n", updateResult)

	err = pfClient.APIV1().Queue().Delete(context.TODO(), queueName, token)
	if err != nil {
		panic(err)
	}
	fmt.Println("delete queue ok")
}
