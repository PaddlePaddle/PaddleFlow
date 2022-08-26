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

// Pilepline
type CreatePipelineRequest struct {
	FsName   string `json:"fsName"`
	YamlPath string `json:"yamlPath"` // optional, use "./run.yaml" if not specified
	UserName string `json:"username"` // optional, only for root user
	Desc     string `json:"desc"`     // optional
}

type CreatePipelineResponse struct {
	PipelineID        string `json:"pipelineID"`
	PipelineVersionID string `json:"pipelineVersionID"`
	Name              string `json:"name"`
}

type GetPipelineRequest struct {
	PipelineID string
	FsFilter   []string
	Marker     string
	MaxKeys    int
}

type GetPipelineResponse struct {
	Pipeline         PipelineBrief    `json:"pipeline"`
	PipelineVersions PipelineVersions `json:"pplVersions"`
}

type PipelineBrief struct {
	ID         string `json:"pipelineID"`
	Name       string `json:"name"`
	Desc       string `json:"desc"`
	UserName   string `json:"username"`
	CreateTime string `json:"createTime"`
	UpdateTime string `json:"updateTime"`
}

type PipelineVersions struct {
	common.MarkerInfo
	PipelineVersionList []PipelineVersionBrief `json:"pplVersionList"`
}

type PipelineVersionBrief struct {
	ID           string `json:"pipelineVersionID"`
	PipelineID   string `json:"pipelineID"`
	FsName       string `json:"fsName"`
	YamlPath     string `json:"yamlPath"`
	PipelineYaml string `json:"pipelineYaml"`
	UserName     string `json:"username"`
	CreateTime   string `json:"createTime"`
	UpdateTime   string `json:"updateTime"`
}

type ListPipelineRequest struct {
	Marker     string
	MaxKeys    int
	UserFilter []string
	NameFilter []string
}

type ListPipelineResponse struct {
	common.MarkerInfo
	PipelineList []PipelineBrief `json:"pipelineList"`
}

type UpdatePipelineRequest struct {
	FsName   string `json:"fsName"`
	YamlPath string `json:"yamlPath"` // optional, use "./run.yaml" if not specified
	UserName string `json:"username"` // optional, only for root user
	Desc     string `json:"desc"`     // optional
}

type UpdatePipelineResponse struct {
	PipelineID        string `json:"pipelineID"`
	PipelineVersionID string `json:"pipelineVersionID"`
}

type GetPipelineVersionResponse struct {
	Pipeline        PipelineBrief        `json:"pipeline"`
	PipelineVersion PipelineVersionBrief `json:"pipelineVersion"`
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

func (p *pipeline) Get(ctx context.Context, request *GetPipelineRequest, token string) (result *GetPipelineResponse, err error) {
	result = &GetPipelineResponse{}
	err = newRequestBuilderWithTokenHeader(p.client, token).
		WithURL(pipelineApi + "/" + request.PipelineID).
		WithMethod(http.GET).
		WithResult(result).
		WithQueryParam("fsFilter", strings.Join(request.FsFilter, ",")).
		WithQueryParam("marker", request.Marker).
		WithQueryParam("maxKeys", strconv.Itoa(request.MaxKeys)).
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

func (p *pipeline) Update(ctx context.Context, pipelineID string, request *UpdatePipelineRequest,
	token string) (result *UpdatePipelineResponse, err error) {
	result = &UpdatePipelineResponse{}
	err = newRequestBuilderWithTokenHeader(p.client, token).
		WithURL(pipelineApi + "/" + pipelineID).
		WithMethod(http.POST).
		WithBody(request).
		WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}
	return
}

func (p *pipeline) GetVersion(ctx context.Context, pipelineID, pipelineVersionID, token string) (result *GetPipelineVersionResponse, err error) {
	result = &GetPipelineVersionResponse{}
	err = newRequestBuilderWithTokenHeader(p.client, token).
		WithURL(pipelineApi + "/" + pipelineID + "/" + pipelineVersionID).
		WithMethod(http.GET).
		WithResult(result).
		Do()

	if err != nil {
		return nil, err
	}

	return
}

func (p *pipeline) DeleteVersion(ctx context.Context, pipelineID, pipelineVersionID, token string) (err error) {
	err = newRequestBuilderWithTokenHeader(p.client, token).
		WithURL(pipelineApi + "/" + pipelineID + "/" + pipelineVersionID).
		WithMethod(http.DELETE).
		Do()

	if err != nil {
		return err
	}

	return
}

type PipelineInterface interface {
	Create(ctx context.Context, request *CreatePipelineRequest, token string) (result *CreatePipelineResponse, err error)
	Get(ctx context.Context, request *GetPipelineRequest, token string) (result *GetPipelineResponse, err error)
	List(ctx context.Context, request *ListPipelineRequest, token string) (result *ListPipelineResponse, err error)
	Delete(ctx context.Context, pipelineID, token string) (err error)
	Update(ctx context.Context, pipelineID string, request *UpdatePipelineRequest, token string) (result *UpdatePipelineResponse, err error)
	GetVersion(ctx context.Context, pipelineID, pipelineVersionID, token string) (result *GetPipelineVersionResponse, err error)
	DeleteVersion(ctx context.Context, pipelineID, pipelineVersionID, token string) (err error)
}

type PipelineGetter interface {
	Pipeline() PipelineInterface
}
