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
	name := "pfstest"
	createResult, err := pfClient.APIV1().FileSystem().Create(context.TODO(), &v1.CreateFileSystemRequest{
		Name:       name,
		Url:        "",
		Properties: map[string]string{},
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("create fs result %v \n", createResult)

	getResult, err := pfClient.APIV1().FileSystem().Get(context.TODO(), &v1.GetFileSystemRequest{FsName: name}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("get fs result %v \n", getResult)

	stsResult, err := pfClient.APIV1().Sts().GetSts(context.TODO(), &v1.GetStsRequest{FsName: name}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("get sts result %v", stsResult)

	err = pfClient.APIV1().FileSystem().Delete(context.TODO(), &v1.DeleteFileSystemRequest{FsName: name}, token)
	if err != nil {
		panic(err)
	}
}
