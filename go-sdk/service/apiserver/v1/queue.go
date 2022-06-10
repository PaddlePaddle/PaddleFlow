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
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const (
	QueueApi = Prefix + "/queue"
	KeyName  = "name"
)

type queue struct {
	client *core.PaddleFlowClient
}

type Queue struct {
	ID              string              `json:"id"`
	CreatedAt       time.Time           `json:"-"`
	UpdatedAt       time.Time           `json:"-"`
	Pk              int64               `json:"-" gorm:"primaryKey;autoIncrement"`
	Name            string              `json:"name" gorm:"uniqueIndex"`
	Namespace       string              `json:"namespace" gorm:"column:"`
	ClusterId       string              `json:"-" gorm:"column:cluster_id"`
	ClusterName     string              `json:"clusterName" gorm:"column:cluster_name;->"`
	QuotaType       string              `json:"quotaType"`
	RawMinResources string              `json:"-" gorm:"column:min_resources;default:'{}'"`
	MinResources    schema.ResourceInfo `json:"minResources" gorm:"-"`
	RawMaxResources string              `json:"-" gorm:"column:max_resources;default:'{}'"`
	MaxResources    schema.ResourceInfo `json:"maxResources" gorm:"-"`
	RawLocation     string              `json:"-" gorm:"column:location;type:text;default:'{}'"`
	Location        map[string]string   `json:"location" gorm:"-"`
	// 任务调度策略
	RawSchedulingPolicy string   `json:"-" gorm:"column:scheduling_policy"`
	SchedulingPolicy    []string `json:"schedulingPolicy,omitempty" gorm:"-"`
	Status              string   `json:"status"`
}

type CreateQueueRequest struct {
	Name         string              `json:"name"`
	Namespace    string              `json:"namespace"`
	ClusterName  string              `json:"clusterName"`
	QuotaType    string              `json:"quotaType"`
	MaxResources schema.ResourceInfo `json:"maxResources"`
	MinResources schema.ResourceInfo `json:"minResources"`
	Location     map[string]string   `json:"location"`
	// 任务调度策略
	SchedulingPolicy []string `json:"schedulingPolicy,omitempty"`
	Status           string   `json:"-"`
}

type UpdateQueueRequest struct {
	Name         string              `json:"-"`
	Namespace    string              `json:"-"`
	ClusterName  string              `json:"-"`
	QuotaType    string              `json:"-"`
	MaxResources schema.ResourceInfo `json:"maxResources,omitempty"`
	MinResources schema.ResourceInfo `json:"minResources,omitempty"`
	Location     map[string]string   `json:"location,omitempty"`
	// 任务调度策略
	SchedulingPolicy []string `json:"schedulingPolicy,omitempty"`
	Status           string   `json:"-"`
}

type CreateQueueResponse struct {
	QueueName string `json:"name"`
}

type UpdateQueueResponse struct {
	Queue
}

type GetQueueResponse struct {
	Queue
}

type ListQueueRequest struct {
	Marker    string
	MaxKeys   int
	QueueName string
}

type ListQueueResponse struct {
	common.MarkerInfo
	QueueList []Queue `json:"queueList"`
}

func (q *queue) Create(ctx context.Context, request *CreateQueueRequest,
	token string) (result *CreateQueueResponse, err error) {
	result = &CreateQueueResponse{}
	err = core.NewRequestBuilder(q.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(QueueApi).
		WithMethod(http.POST).
		WithBody(request).
		WithResult(result).
		Do()
	return
}

func (q *queue) Get(ctx context.Context, queueName,
	token string) (result *GetQueueResponse, err error) {
	result = &GetQueueResponse{}
	err = core.NewRequestBuilder(q.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(QueueApi + "/" + queueName).
		WithMethod(http.GET).
		WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}
	return
}

func (q *queue) List(ctx context.Context, request *ListQueueRequest,
	token string) (result *ListQueueResponse, err error) {
	result = &ListQueueResponse{}
	err = core.NewRequestBuilder(q.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(QueueApi).
		WithMethod(http.GET).
		WithQueryParamFilter(KeyMarker, request.Marker).
		WithQueryParamFilter(KeyMaxKeys, strconv.Itoa(request.MaxKeys)).
		WithQueryParamFilter(KeyName, request.QueueName).
		WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}
	return
}

func (q *queue) Update(ctx context.Context, queueName string, request *UpdateQueueRequest,
	token string) (result *UpdateQueueResponse, err error) {
	result = &UpdateQueueResponse{}
	err = core.NewRequestBuilder(q.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(QueueApi + "/" + queueName).
		WithMethod(http.PUT).
		WithBody(request).
		WithResult(result).
		Do()
	return
}

func (q *queue) Delete(ctx context.Context, queueName, token string) (err error) {
	err = core.NewRequestBuilder(q.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(QueueApi + "/" + queueName).
		WithMethod(http.DELETE).
		Do()
	return
}

type QueueGetter interface {
	Queue() QueueInterface
}

type QueueInterface interface {
	Create(ctx context.Context, request *CreateQueueRequest, token string) (*CreateQueueResponse, error)
	Get(ctx context.Context, queueName string, token string) (*GetQueueResponse, error)
	List(ctx context.Context, request *ListQueueRequest, token string) (*ListQueueResponse, error)
	Update(ctx context.Context, queueName string, request *UpdateQueueRequest, token string) (*UpdateQueueResponse, error)
	Delete(ctx context.Context, queueName string, token string) error
}

// newQueue returns a queue.
func newQueue(c *APIV1Client) *queue {
	return &queue{
		client: c.RESTClient(),
	}
}
