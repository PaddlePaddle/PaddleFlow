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

func Create(pfClient *service.PaddleFlowClient, request *v1.CreateRunRequest, token string) (createResult *v1.CreateRunResponse) {
	createResult, err := pfClient.APIV1().Run().Create(context.TODO(), request, token)

	if err != nil {
		panic(err)
	}
	fmt.Printf("create Run result %v\n", createResult)
	return
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

func List(pfClient *service.PaddleFlowClient, request *v1.ListRunRequest, token string) (result *v1.ListRunResponse) {
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

func Stop(pfClient *service.PaddleFlowClient, token, runID string, stopForce bool) {
	err := pfClient.APIV1().Run().Stop(context.TODO(), stopForce, runID, token)
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

func ListCache(pfClient *service.PaddleFlowClient, token string, request *v1.ListRunCacheRequest) *v1.ListRunCacheResponse {
	result, err := pfClient.APIV1().Run().ListRunCache(context.TODO(), request, token)
	if err != nil {
		panic(err)
	}
	return result
}

func GetCache(pfClient *service.PaddleFlowClient, token string, cacheID string) *v1.GetRunCacheResponse {
	result, err := pfClient.APIV1().Run().GetRunCache(context.TODO(), cacheID, token)
	if err != nil {
		panic(err)
	}
	return result
}

func DeleteCache(pfClient *service.PaddleFlowClient, token string, cacheID string) {
	err := pfClient.APIV1().Run().DeleteRunCache(context.TODO(), cacheID, token)
	if err != nil {
		panic(err)
	}
}

func ListArtifact(pfClient *service.PaddleFlowClient, token string, request *v1.ListArtifactRequest) *v1.ListArtifactResponse {
	result, err := pfClient.APIV1().Run().ListArtifact(context.TODO(), request, token)
	if err != nil {
		panic(err)
	}
	return result
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

	reqCreate := &v1.CreateRunRequest{
		FsName:      "",
		Description: "",
		RunYamlPath: "",
		UserName:    "",
	}

	createResult := Create(pfClient, reqCreate, token)
	fmt.Println(createResult)

	Stop(pfClient, token, createResult.RunID, false)

	res := Retry(pfClient, token, "run-000001")
	fmt.Println(res.RunID)

	getResult := Get(pfClient, token, "run-000001")
	resJson, _ := json.Marshal(getResult)
	fmt.Println(string(resJson))

	Delete(pfClient, token, "run-000002")
	Get(pfClient, token, "run-000002")

	reqList := &v1.ListRunRequest{
		MaxKeys:    10,
		UserFilter: []string{},
		FsFilter:   []string{},
	}
	List(pfClient, reqList, token)

	reqListCache := &v1.ListRunCacheRequest{
		UserFilter: []string{},
		FSFilter:   []string{},
		RunFilter:  []string{},
		MaxKeys:    30,
		Marker:     "",
	}
	resListCache := ListCache(pfClient, token, reqListCache)
	jsonListCache, _ := json.Marshal(resListCache)
	fmt.Println(string(jsonListCache))

	resGetCache := GetCache(pfClient, token, "cch-000002")
	jsonGetCache, _ := json.Marshal(resGetCache)
	fmt.Println(string(jsonGetCache))

	DeleteCache(pfClient, token, "cch-000002")
	resListCache = ListCache(pfClient, token, reqListCache)
	jsonListCache, _ = json.Marshal(resListCache)
	fmt.Println(string(jsonListCache))

	resListArtifact := ListArtifact(pfClient, token, &v1.ListArtifactRequest{
		MaxKeys: 10,
	})
	jsonListAtf, _ := json.Marshal(resListArtifact)
	fmt.Println(string(jsonListAtf))
}
