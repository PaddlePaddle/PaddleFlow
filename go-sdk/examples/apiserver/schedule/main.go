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

func Create(pfClient *service.PaddleFlowClient, token string, request *v1.CreateScheduleRequest) (createResult *v1.CreateScheduleResponse) {
	createResult, err := pfClient.APIV1().Schedule().Create(context.TODO(), request, token)

	if err != nil {
		panic(err)
	}
	return
}

func List(pfClient *service.PaddleFlowClient, token string, request *v1.ListScheduleRequest) (result *v1.ListScheduleResponse) {
	result, err := pfClient.APIV1().Schedule().List(context.TODO(), request, token)
	if err != nil {
		panic(err)
	}
	return
}

func Get(pfClient *service.PaddleFlowClient, token string, request *v1.GetScheduleRequest) (result *v1.GetScheduleResponse) {
	result, err := pfClient.APIV1().Schedule().Get(context.TODO(), request, token)

	if err != nil {
		panic(err)
	}
	return
}

func Stop(pfClient *service.PaddleFlowClient, token string, scheduleID string) {
	err := pfClient.APIV1().Schedule().Stop(context.TODO(), scheduleID, token)
	if err != nil {
		panic(err)
	}
}

func Delete(pfClient *service.PaddleFlowClient, token string, scheduleID string) {
	err := pfClient.APIV1().Schedule().Delete(context.TODO(), scheduleID, token)
	if err != nil {
		panic(err)
	}
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

	resCreate := Create(pfClient, token, &v1.CreateScheduleRequest{
		Name:              "test",
		PipelineID:        "ppl-000001",
		PipelineVersionID: "1",
		Crontab:           "*/5 * * * *",
	})
	jsonCreate, _ := json.Marshal(resCreate)
	fmt.Println(string(jsonCreate))

	resGet := Get(pfClient, token, &v1.GetScheduleRequest{
		ScheduleID: "schedule-000001",
		MaxKeys:    10,
	})
	jsonGet, _ := json.Marshal(resGet)
	fmt.Println(string(jsonGet))

	Stop(pfClient, token, "schedule-000001")
	resGet = Get(pfClient, token, &v1.GetScheduleRequest{
		ScheduleID: "schedule-000001",
		MaxKeys:    10,
	})
	jsonGet, _ = json.Marshal(resGet)
	fmt.Println(string(jsonGet))

	Delete(pfClient, token, "schedule-000001")

	resList := List(pfClient, token, &v1.ListScheduleRequest{
		MaxKeys: 10,
	})
	jsonList, _ := json.Marshal(resList)
	fmt.Println(string(jsonList))
}
