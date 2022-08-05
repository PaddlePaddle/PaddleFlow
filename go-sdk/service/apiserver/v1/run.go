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
package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const (
	runApi = Prefix + "/run"
)

type CreateRunRequest struct {
	FsName      string                 `json:"fsName"`
	UserName    string                 `json:"username,omitempty"`   // optional, only for root user
	Name        string                 `json:"name,omitempty"`       // optional
	Description string                 `json:"desc,omitempty"`       // optional
	Parameters  map[string]interface{} `json:"parameters,omitempty"` // optional
	DockerEnv   string                 `json:"dockerEnv,omitempty"`  // optional
	// run workflow source. priority: RunYamlRaw > PipelineID + PipelineVersionID > RunYamlPath
	// 为了防止字符串或者不同的http客户端对run.yaml
	// 格式中的特殊字符串做特殊过滤处理导致yaml文件不正确，因此采用runYamlRaw采用base64编码传输
	Disabled          string `json:"disabled,omitempty"`          // optional
	RunYamlRaw        string `json:"runYamlRaw,omitempty"`        // optional. one of 3 sources of run. high priority
	PipelineID        string `json:"pipelineID,omitempty"`        // optional. one of 3 sources of run. medium priority
	PipelineVersionID string `json:"pipelineVersionID,omitempty"` // optional. one of 3 sources of run. medium priority
	RunYamlPath       string `json:"runYamlPath,omitempty"`       // optional. one of 3 sources of run. low priority
}

type UpdateRunRequest struct {
	StopForce bool `json:"stopForce"`
}

type DeleteRunRequest struct {
	CheckCache bool `json:"checkCache"`
}

type CreateRunResponse struct {
	RunID string `json:"runID"`
}

type ListRunRequest struct {
	Marker           string
	MaxKeys          int
	UserFilter       []string
	FsFilter         []string
	RunFilter        []string
	NameFilter       []string
	StatusFilter     []string
	ScheduleIDFilter []string
}

type RunBrief struct {
	ID            string `json:"runID"`
	Name          string `json:"name"`
	Source        string `json:"source"` // pipelineID or yamlPath
	UserName      string `json:"username"`
	FsName        string `json:"fsName"`
	Description   string `json:"description"`
	ScheduleID    string `json:"scheduleID"`
	Message       string `json:"message"`
	Status        string `json:"status"`
	ScheduledTime string `json:"scheduledTime"`
	CreateTime    string `json:"createTime"`
	ActivateTime  string `json:"activateTime"`
	UpdateTime    string `json:"updateTime"`
}

type ListRunResponse struct {
	common.MarkerInfo
	RunList []RunBrief `json:"runList"`
}

type GetRunResponse struct {
	ID             string                 `json:"runID"`
	Name           string                 `json:"name"`
	Source         string                 `json:"source"` // pipelineID or yamlPath
	UserName       string                 `json:"username"`
	FsName         string                 `json:"fsname"`
	FsOptions      schema.FsOptions       `json:"fsOptions"`
	Description    string                 `json:"description"`
	Parameters     map[string]interface{} `json:"parameters"`
	RunYaml        string                 `json:"runYaml"`
	Runtime        schema.RuntimeView     `json:"runtime"`
	PostProcess    schema.PostProcessView `json:"postProcess"`
	FailureOptions schema.FailureOptions  `json:"failureOptions"`
	DockerEnv      string                 `json:"dockerEnv"`
	Entry          string                 `json:"entry"`
	Disabled       string                 `json:"disabled"`
	ScheduleID     string                 `json:"scheduleID"`
	Message        string                 `json:"message"`
	Status         string                 `json:"status"` // StatusRun%%%
	RunCachedIDs   string                 `json:"runCachedIDs"`
	CreateTime     string                 `json:"createTime"`
	ActivateTime   string                 `json:"activateTime"`
	UpdateTime     string                 `json:"updateTime,omitempty"`
}

type StopRequest struct {
	StopForce bool `json:"stopForce"`
}

type RetryRunResponse struct {
	RunID string `json:"runID"`
}

type run struct {
	client *core.PaddleFlowClient
}

// newrun returns a run.
func newRun(c *APIV1Client) *run {
	return &run{
		client: c.RESTClient(),
	}
}

func newRequestBuilderWithTokenHeader(cli *core.PaddleFlowClient, token string) *core.RequestBuilder {
	builder := core.NewRequestBuilder(cli)
	builder.WithHeader(common.HeaderKeyAuthorization, token)
	return builder
}

func (r *run) Create(ctx context.Context, request *CreateRunRequest,
	token string) (result *CreateRunResponse, err error) {
	result = &CreateRunResponse{}
	err = newRequestBuilderWithTokenHeader(r.client, token).
		WithURL(runApi).
		WithMethod(http.POST).
		WithBody(request).
		WithResult(result).
		Do()

	if err != nil {
		return nil, err
	}

	return
}

func (r *run) Get(ctx context.Context, runID string, token string) (result *GetRunResponse, err error) {
	// 由于GetResponse中的Runtime类型为接口，不能在直接传给WithResult，因此先用一个临时Map接收Response信息
	rspMap := map[string]interface{}{}
	result = &GetRunResponse{}
	err = newRequestBuilderWithTokenHeader(r.client, token).
		WithMethod(http.GET).
		WithURL(runApi + "/" + runID).
		WithResult(&rspMap).
		Do()
	if err != nil {
		return nil, err
	}

	// 1. 先单独处理runtime
	if runtime, ok := rspMap["runtime"]; ok {
		runtimeMap, ok := runtime.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("init Runtime of response failed: value of runtime is not Map type")
		}
		result.Runtime, err = initRuntime(runtimeMap)
		if err != nil {
			return nil, err
		}
	}

	// 2. 处理完runtime后，将runtime剔除，然后将Map转换成GetResponse结构体
	delete(rspMap, "runtime")
	tempRsp, err := json.Marshal(rspMap)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(tempRsp, result)

	return
}

func initRuntime(compMap map[string]interface{}) (map[string][]schema.ComponentView, error) {
	resMap := map[string][]schema.ComponentView{}
	// 遍历compMap中的每个compList
	for name, comps := range compMap {
		compList, ok := comps.([]interface{})
		if !ok {
			return nil, fmt.Errorf("init Runtime of response failed: value of comp is not list")
		}

		resList := []schema.ComponentView{}
		for _, comp := range compList {
			// 遍历compList中的每个comp
			compMap, ok := comp.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("init Runtime of response failed: comp should be map type")
			}

			// 通过是否有entryPoints判断是否是Dag
			if entryPoints, ok := compMap["entryPoints"]; ok {
				// 如果是Dag
				subCompMap, ok := entryPoints.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("init Runtime of response failed: entryPoints in dag[%s] should be map type", name)
				}

				// 先Marshal再Unmarshal回davView
				// 由于entryPoints中的内容无法直接Unmarshal到davView中，这里先删除，后续单独初始化
				delete(compMap, "entryPoints")
				compByte, err := json.Marshal(compMap)
				if err != nil {
					return nil, err
				}
				dagView := schema.DagView{}
				if err := json.Unmarshal(compByte, &dagView); err != nil {
					return nil, err
				}

				// 递归调用initRuntime来初始化entryPoints子节点
				subComps, err := initRuntime(subCompMap)
				if err != nil {
					return nil, err
				}

				dagView.EntryPoints = subComps
				resList = append(resList, &dagView)
			} else {
				// 如果不是Dag，而是Job
				compByte, err := json.Marshal(compMap)
				if err != nil {
					return nil, err
				}
				jobView := schema.JobView{}
				if err := json.Unmarshal(compByte, &jobView); err != nil {
					return nil, err
				}
				resList = append(resList, &jobView)
			}
		}
		resMap[name] = resList
	}
	return resMap, nil
}

func (r *run) List(ctx context.Context, request *ListRunRequest, token string) (result *ListRunResponse, err error) {
	result = &ListRunResponse{}

	err = newRequestBuilderWithTokenHeader(r.client, token).
		WithMethod(http.GET).
		WithResult(result).
		WithURL(runApi).
		WithQueryParam("fsFilter", strings.Join(request.FsFilter, ",")).
		WithQueryParam("userFilter", strings.Join(request.UserFilter, ",")).
		WithQueryParam("nameFilter", strings.Join(request.NameFilter, ",")).
		WithQueryParam("runFilter", strings.Join(request.RunFilter, ",")).
		WithQueryParam("statusFilter", strings.Join(request.StatusFilter, ",")).
		WithQueryParam("scheduleIDFilter", strings.Join(request.ScheduleIDFilter, ",")).
		WithQueryParam("marker", request.Marker).
		WithQueryParam("maxKeys", strconv.Itoa(request.MaxKeys)).
		Do()

	if err != nil {
		return nil, err
	}

	return
}

func (r *run) Stop(ctx context.Context, StopForce bool, runID, token string) (err error) {
	request := StopRequest{
		StopForce: StopForce,
	}

	err = newRequestBuilderWithTokenHeader(r.client, token).
		WithURL(runApi+"/"+runID).
		WithMethod(http.PUT).
		WithQueryParam("action", "stop").
		WithBody(request).
		Do()

	if err != nil {
		return err
	}

	return
}

func (r *run) Retry(ctx context.Context, runID string, token string) (result *RetryRunResponse, err error) {
	result = &RetryRunResponse{}
	err = newRequestBuilderWithTokenHeader(r.client, token).
		WithURL(runApi+"/"+runID).
		WithMethod(http.PUT).
		WithQueryParam("action", "retry").
		WithResult(result).
		Do()

	if err != nil {
		return nil, err
	}
	return
}

func (r *run) Delete(ctx context.Context, runID string, token string) (err error) {
	err = newRequestBuilderWithTokenHeader(r.client, token).
		WithURL(runApi + "/" + runID).
		WithMethod(http.DELETE).
		Do()

	if err != nil {
		return err
	}

	return
}

type RunInterface interface {
	Create(ctx context.Context, request *CreateRunRequest, token string) (result *CreateRunResponse, err error)
	Get(ctx context.Context, runID string, token string) (result *GetRunResponse, err error)
	List(ctx context.Context, request *ListRunRequest, token string) (result *ListRunResponse, err error)
	Stop(ctx context.Context, StopForce bool, runID, token string) (err error)
	Retry(ctx context.Context, runID string, token string) (result *RetryRunResponse, err error)
	Delete(ctx context.Context, runID string, token string) (err error)
}

type RunGetter interface {
	Run() RunInterface
}
