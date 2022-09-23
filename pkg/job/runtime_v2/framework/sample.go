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

package framework

import (
	"context"
	"errors"

	"k8s.io/client-go/util/workqueue"

	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
)

type JobSample struct {
}

func (j *JobSample) Submit(ctx context.Context, job *api.PFJob) error {
	return errors.New("submit method for job is not supported")
}

func (j *JobSample) Update(ctx context.Context, job *api.PFJob) error {
	return errors.New("update method for job is not supported")
}

func (j *JobSample) Stop(ctx context.Context, job *api.PFJob) error {
	return errors.New("stop method for job is not supported")
}

func (j *JobSample) Delete(ctx context.Context, job *api.PFJob) error {
	return errors.New("delete method for job is not supported")
}

func (j *JobSample) GetLog(ctx context.Context, jobLogRequest pfschema.JobLogRequest) (pfschema.JobLogInfo, error) {
	return pfschema.JobLogInfo{}, errors.New("GetLog method for job is not supported")
}

func (j *JobSample) AddEventListener(ctx context.Context, listenerType string,
	eventQueue workqueue.RateLimitingInterface, informer interface{}) error {
	return errors.New("AddEventListener method for job is not supported")
}

type QueueSample struct {
}

func (qs *QueueSample) Create(ctx context.Context, q *api.QueueInfo) error {
	return errors.New("create method for queue is not supported")
}

func (qs *QueueSample) Delete(ctx context.Context, q *api.QueueInfo) error {
	return errors.New("delete method for queue is not supported")
}

func (qs *QueueSample) Update(ctx context.Context, q *api.QueueInfo) error {
	return errors.New("update method for queue is not supported")
}

func (qs *QueueSample) AddEventListener(ctx context.Context, listenerType string,
	eventQueue workqueue.RateLimitingInterface, informer interface{}) error {
	return errors.New("AddEventListener method for queue is not supported")
}
