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
	"strconv"
	"strings"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
)

const (
	pipelineApi = Prefix + "/pipeline"
)

type CreatePipelineRequest struct {
	FsName   string `json:"fsname"`
	YamlPath string `json:"yamlPath,omitempty"` // optional, use "./run.yaml" if not specified
	Name     string `json:"name,omitempty"`     // optional
	UserName string `json:"username,omitempty"` // optional, only for root user
}

type CreatePipelineResponse struct {
	ID   string `json:"pipelineID"`
	Name string `json:"name"`
}

type GetPipelineResponse struct {
	ID           string `json:"pipelineID"`
	Name         string `json:"name"`
	FsName       string `json:"fsname"`
	UserName     string `json:"username"`
	PipelineYaml string `json:"pipelineYaml"`
	PipelineMd5  string `json:"pipelineMd5"`
	CreateTime   string `json:"createTime,omitempty"`
	UpdateTime   string `json:"updateTime,omitempty"`
}

type ListPipelineRequest struct {
	Marker     string
	MaxKeys    int
	UserFilter []string
	FsFilter   []string
	NameFilter []string
}

type ListPipelineResponse struct {
	common.MarkerInfo
	PipelineList []GetPipelineResponse `json:"pipelineList"`
}

type pipeline struct {
	client *core.PaddleFlowClient
}

// new run returns a pipeline.
func newPipeline(c *APIV1Client) *pipeline {
	return &pipeline{
		client: c.RESTClient(),
	}
}

func (p *pipeline) Create(ctx context.Context, request *CreatePipelineRequest, token string) (result *CreatePipelineResponse, err error) {
	result = &CreatePipelineResponse{}
	err = newRequestBuilderWithTokenHeader(p.client, token).
		WithURL(pipelineApi).
		WithMethod(http.POST).
		WithBody(request).
		WithResult(result).
		Do()

	if err != nil {
		return nil, err
	}

	return
}

func (p *pipeline) Get(ctx context.Context, pipelineID, token string) (result *GetPipelineResponse, err error) {
	result = &GetPipelineResponse{}
	err = newRequestBuilderWithTokenHeader(p.client, token).
		WithURL(pipelineApi + "/" + pipelineID).
		WithMethod(http.GET).
		WithResult(result).
		Do()

	if err != nil {
		return nil, err
	}

	return
}

func (p *pipeline) List(ctx context.Context, request *ListPipelineRequest,
	token string) (result *ListPipelineResponse, err error) {
	result = &ListPipelineResponse{}
	err = newRequestBuilderWithTokenHeader(p.client, token).
		WithMethod(http.GET).
		WithResult(result).
		WithURL(pipelineApi).
		WithQueryParam("fsFilter", strings.Join(request.FsFilter, ",")).
		WithQueryParam("userFilter", strings.Join(request.UserFilter, ",")).
		WithQueryParam("nameFilter", strings.Join(request.NameFilter, ",")).
		WithQueryParam("marker", request.Marker).
		WithQueryParam("maxKeys", strconv.Itoa(request.MaxKeys)).
		Do()

	if err != nil {
		return nil, err
	}

	return
}

func (p *pipeline) Delete(ctx context.Context, pipelineID, token string) (err error) {
	err = newRequestBuilderWithTokenHeader(p.client, token).
		WithURL(pipelineApi + "/" + pipelineID).
		WithMethod(http.DELETE).
		Do()

	if err != nil {
		return err
	}

	return
}

type PipelineInterface interface {
	Create(ctx context.Context, request *CreatePipelineRequest, token string) (result *CreatePipelineResponse, err error)
	Get(ctx context.Context, pipelineID, token string) (result *GetPipelineResponse, err error)
	List(ctx context.Context, request *ListPipelineRequest, token string) (result *ListPipelineResponse, err error)
	Delete(ctx context.Context, pipelineID, token string) (err error)
}

type PipelineGetter interface {
	Pipeline() PipelineInterface
}
