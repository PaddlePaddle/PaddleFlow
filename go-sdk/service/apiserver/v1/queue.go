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

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	queue_ "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/queue"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
)

const (
	QueueApi = Prefix + "/queue"
	KeyName  = "name"
)

type queue struct {
	client *core.PaddleFlowClient
}

func (q *queue) Create(ctx context.Context, request *queue_.CreateQueueRequest,
	token string) (result *queue_.CreateQueueResponse, err error) {
	result = &queue_.CreateQueueResponse{}
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
	token string) (result *queue_.GetQueueResponse, err error) {
	result = &queue_.GetQueueResponse{}
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

func (q *queue) List(ctx context.Context, request *queue_.ListQueueRequest,
	token string) (result *queue_.ListQueueResponse, err error) {
	result = &queue_.ListQueueResponse{}
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

func (q *queue) Update(ctx context.Context, queueName string, request *queue_.UpdateQueueRequest,
	token string) (result *queue_.UpdateQueueResponse, err error) {
	result = &queue_.UpdateQueueResponse{}
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
	Create(ctx context.Context, request *queue_.CreateQueueRequest, token string) (*queue_.CreateQueueResponse, error)
	Get(ctx context.Context, queueName string, token string) (*queue_.GetQueueResponse, error)
	List(ctx context.Context, request *queue_.ListQueueRequest, token string) (*queue_.ListQueueResponse, error)
	Update(ctx context.Context, queueName string, request *queue_.UpdateQueueRequest, token string) (*queue_.UpdateQueueResponse, error)
	Delete(ctx context.Context, queueName string, token string) error
}

// newQueue returns a queue.
func newQueue(c *APIV1Client) *queue {
	return &queue{
		client: c.RESTClient(),
	}
}
