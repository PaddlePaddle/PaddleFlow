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
	scheduleAPI = Prefix + "/schedule"
)

type CreateScheduleRequest struct {
	Name              string `json:"name"`
	Desc              string `json:"desc"` // optional
	PipelineID        string `json:"pipelineID"`
	PipelineVersionID string `json:"pipelineVersionID"`
	Crontab           string `json:"crontab"`
	StartTime         string `json:"startTime"`         // optional
	EndTime           string `json:"endTime"`           // optional
	Concurrency       int    `json:"concurrency"`       // optional, 默认 0, 表示不限制
	ConcurrencyPolicy string `json:"concurrencyPolicy"` // optional, 默认 suspend
	ExpireInterval    int    `json:"expireInterval"`    // optional, 默认 0, 表示不限制
	Catchup           bool   `json:"catchup"`           // optional, 默认 false
	UserName          string `json:"username"`          // optional, 只有root用户使用其他用户fsname时，需要指定对应username
}

type CreateScheduleResponse struct {
	ScheduleID string `json:"scheduleID"`
}

type ScheduleBrief struct {
	ID                string          `json:"scheduleID"`
	Name              string          `json:"name"`
	Desc              string          `json:"desc"`
	PipelineID        string          `json:"pipelineID"`
	PipelineVersionID string          `json:"pipelineVersionID"`
	UserName          string          `json:"username"`
	FsConfig          FsConfig        `json:"fsConfig"`
	Crontab           string          `json:"crontab"`
	Options           ScheduleOptions `json:"options"`
	StartTime         string          `json:"startTime"`
	EndTime           string          `json:"endTime"`
	CreateTime        string          `json:"createTime"`
	UpdateTime        string          `json:"updateTime"`
	NextRunTime       string          `json:"nextRunTime"`
	Message           string          `json:"scheduleMsg"`
	Status            string          `json:"status"`
}

type ScheduleOptions struct {
	Catchup           bool   `json:"catchup"`
	ExpireInterval    int    `json:"expireInterval"`
	Concurrency       int    `json:"concurrency"`
	ConcurrencyPolicy string `json:"concurrencyPolicy"`
}

type FsConfig struct {
	Username string `json:"username"`
}

type GetScheduleRequest struct {
	ScheduleID   string
	RunFilter    []string
	StatusFilter []string
	Marker       string
	MaxKeys      int
}

type GetScheduleResponse struct {
	ScheduleBrief
	ListRunResponse ListRunResponse `json:"runs"`
}

type ListScheduleRequest struct {
	UserFilter       []string
	PplFilter        []string
	PplVersionFilter []string
	ScheduleFilter   []string
	NameFilter       []string
	StatusFilter     []string
	Marker           string
	MaxKeys          int
}

type ListScheduleResponse struct {
	common.MarkerInfo
	ScheduleList []ScheduleBrief `json:"scheduleList"`
}

type schedule struct {
	client *core.PaddleFlowClient
}

// newSchedule returns a Schedule.
func newSchedule(c *APIV1Client) *schedule {
	return &schedule{
		client: c.RESTClient(),
	}
}

func (s *schedule) Create(ctx context.Context, request *CreateScheduleRequest, token string) (result *CreateScheduleResponse, err error) {
	result = &CreateScheduleResponse{}
	err = newRequestBuilderWithTokenHeader(s.client, token).
		WithURL(scheduleAPI).
		WithMethod(http.POST).
		WithBody(request).
		WithResult(result).
		Do()

	if err != nil {
		return nil, err
	}

	return
}

func (s *schedule) Get(ctx context.Context, request *GetScheduleRequest, token string) (result *GetScheduleResponse, err error) {
	result = &GetScheduleResponse{}
	err = newRequestBuilderWithTokenHeader(s.client, token).
		WithMethod(http.GET).
		WithURL(scheduleAPI+"/"+request.ScheduleID).
		WithQueryParam("runFilter", strings.Join(request.RunFilter, ",")).
		WithQueryParam("statusFilter", strings.Join(request.StatusFilter, ",")).
		WithQueryParam("marker", request.Marker).
		WithQueryParam("maxKeys", strconv.Itoa(request.MaxKeys)).
		WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}

	return
}

func (s *schedule) List(ctx context.Context, request *ListScheduleRequest, token string) (result *ListScheduleResponse, err error) {
	result = &ListScheduleResponse{}

	err = newRequestBuilderWithTokenHeader(s.client, token).
		WithMethod(http.GET).
		WithResult(result).
		WithURL(scheduleAPI).
		WithQueryParam("userFilter", strings.Join(request.UserFilter, ",")).
		WithQueryParam("pplFilter", strings.Join(request.PplFilter, ",")).
		WithQueryParam("pplVersionFilter", strings.Join(request.PplVersionFilter, ",")).
		WithQueryParam("scheduleFilter", strings.Join(request.ScheduleFilter, ",")).
		WithQueryParam("nameFilter", strings.Join(request.NameFilter, ",")).
		WithQueryParam("statusFilter", strings.Join(request.StatusFilter, ",")).
		WithQueryParam("marker", request.Marker).
		WithQueryParam("maxKeys", strconv.Itoa(request.MaxKeys)).
		Do()

	if err != nil {
		return nil, err
	}

	return
}

func (s *schedule) Stop(ctx context.Context, scheduleID string, token string) (err error) {
	err = newRequestBuilderWithTokenHeader(s.client, token).
		WithURL(scheduleAPI + "/" + scheduleID).
		WithMethod(http.PUT).
		Do()

	if err != nil {
		return err
	}

	return
}

func (s *schedule) Delete(ctx context.Context, scheduleID string, token string) (err error) {
	err = newRequestBuilderWithTokenHeader(s.client, token).
		WithURL(scheduleAPI + "/" + scheduleID).
		WithMethod(http.DELETE).
		Do()

	if err != nil {
		return err
	}

	return
}

type ScheduleInterface interface {
	Create(ctx context.Context, request *CreateScheduleRequest, token string) (result *CreateScheduleResponse, err error)
	Get(ctx context.Context, request *GetScheduleRequest, token string) (result *GetScheduleResponse, err error)
	List(ctx context.Context, request *ListScheduleRequest, token string) (result *ListScheduleResponse, err error)
	Stop(ctx context.Context, scheduleID string, token string) (err error)
	Delete(ctx context.Context, scheduleID string, token string) (err error)
}

type ScheduleGetter interface {
	Schedule() ScheduleInterface
}
