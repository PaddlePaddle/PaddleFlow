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
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/cluster"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/user"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
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

	clusterName := "test-cluster"
	createResult, err := pfClient.APIV1().Cluster().Create(context.TODO(), &cluster.CreateClusterRequest{
		ClusterCommonInfo: cluster.ClusterCommonInfo{
			Endpoint:    "http://127.0.0.1:6443",
			ClusterType: "Kubernetes",
			Version:     "v1.16.3",
			// base64 encoding for cluster credential
			Credential: "xxx",
		},
		Name: clusterName,
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("create cluster result %v\n", createResult)

	getResult, err := pfClient.APIV1().Cluster().Get(context.TODO(), clusterName, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("get cluster result %v\n", getResult)

	listResult, err := pfClient.APIV1().Cluster().List(context.TODO(), &cluster.ListClusterRequest{
		MaxKeys:         100,
		ClusterStatus:   "online",
		ClusterNameList: []string{clusterName, "xx", "xx2"},
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("list cluster result %v\n", listResult)

	updateResult, err := pfClient.APIV1().Cluster().Update(context.TODO(), clusterName, &cluster.UpdateClusterRequest{
		ClusterCommonInfo: cluster.ClusterCommonInfo{
			Endpoint:    "http://127.0.0.1:6443",
			ClusterType: "Kubernetes",
			Version:     "v1.16.9",
			// base64 encoding for cluster credential
			Credential: "xxx",
		},
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("update cluster result %v\n", updateResult)

	err = pfClient.APIV1().Cluster().Delete(context.TODO(), clusterName, token)
	if err != nil {
		panic(err)
	}
	fmt.Println("delete cluster ok")
}
