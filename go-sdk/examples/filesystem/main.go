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
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/fs"
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
	name := "pfstest"
	createResult, err := pfClient.APIV1().FileSystem().Create(context.TODO(), &fs.CreateFileSystemRequest{
		Name: name,
		Url:  "glusterfs://10.190.170.12:ape-default-volume",
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("create fs result %v", createResult)

	getResult, err := pfClient.APIV1().FileSystem().Get(context.TODO(), &fs.GetFileSystemRequest{FsName: name}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("get fs result %v", getResult)

	err = pfClient.APIV1().FileSystem().Delete(context.TODO(), &fs.DeleteFileSystemRequest{FsName: name}, token)
	if err != nil {
		panic(err)
	}
}
