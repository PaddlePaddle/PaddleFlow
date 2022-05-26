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
	"strconv"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	job_ "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/job"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
)

const (
	JobApi          = Prefix + "/job"
	TypeSingle      = "single"
	TypeDistributed = "distributed"
	TypeWorkflow    = "workflow"
	KeyAction       = "action"
	KeyStatus       = "status"
	KeyTimestamp    = "timestamp"
	KeyStartTime    = "startTime"
	KeyQueue        = "queue"
	KeyLabels       = "labels"
)

type job struct {
	client *core.PaddleFlowClient
}

func (j *job) Create(ctx context.Context, single *job_.CreateSingleJobRequest, distributed *job_.CreateDisJobRequest,
	wf *job_.CreateWfJobRequest, token string) (result *job_.CreateJobResponse, err error) {
	result = &job_.CreateJobResponse{}
	requestClient := core.NewRequestBuilder(j.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithMethod(http.POST)
	if single != nil {
		requestClient.WithURL(JobApi + "/" + TypeSingle).
			WithBody(single)
	} else if distributed != nil {
		requestClient.WithURL(JobApi + "/" + TypeDistributed).
			WithBody(distributed)
	} else if wf != nil {
		requestClient.WithURL(JobApi + "/" + TypeWorkflow).
			WithBody(wf)
	}
	err = requestClient.WithResult(result).
		Do()
	return
}

func (j *job) Get(ctx context.Context, jobID,
	token string) (result *job_.GetJobResponse, err error) {
	result = &job_.GetJobResponse{}
	err = core.NewRequestBuilder(j.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(JobApi + "/" + jobID).
		WithMethod(http.GET).
		WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}
	return
}

func (j *job) List(ctx context.Context, request *job_.ListJobRequest,
	token string) (result *job_.ListJobResponse, err error) {
	result = &job_.ListJobResponse{}
	requestClient := core.NewRequestBuilder(j.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(JobApi).
		WithMethod(http.GET).
		WithQueryParamFilter(KeyMarker, request.Marker).
		WithQueryParamFilter(KeyMaxKeys, strconv.Itoa(request.MaxKeys)).
		WithQueryParamFilter(KeyStatus, request.Status).
		WithQueryParamFilter(KeyTimestamp, strconv.FormatInt(request.Timestamp, 10)).
		WithQueryParamFilter(KeyStartTime, request.StartTime).
		WithQueryParamFilter(KeyQueue, request.Queue)
	if request.Labels != nil && len(request.Labels) != 0 {
		labels, err := json.Marshal(request.Labels)
		if err != nil {
			return nil, err
		}
		requestClient.WithQueryParamFilter(KeyLabels, string(labels))
	}
	err = requestClient.WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}
	return
}

func (j *job) Update(ctx context.Context, jobID string, request *job_.UpdateJobRequest,
	token string) (err error) {
	err = core.NewRequestBuilder(j.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(JobApi+"/"+jobID).
		WithQueryParam(KeyAction, "modify").
		WithMethod(http.PUT).
		WithBody(request).
		Do()
	return
}

func (j *job) Stop(ctx context.Context, jobID, token string) (err error) {
	err = core.NewRequestBuilder(j.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(JobApi+"/"+jobID).
		WithQueryParam(KeyAction, "stop").
		WithMethod(http.PUT).
		Do()
	return
}

func (j *job) Delete(ctx context.Context, jobID, token string) (err error) {
	err = core.NewRequestBuilder(j.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(JobApi + "/" + jobID).
		WithMethod(http.DELETE).
		Do()
	return
}

type JobGetter interface {
	Job() JobInterface
}

type JobInterface interface {
	Create(ctx context.Context, single *job_.CreateSingleJobRequest, distributed *job_.CreateDisJobRequest,
		wf *job_.CreateWfJobRequest, token string) (*job_.CreateJobResponse, error)
	Get(ctx context.Context, jobID string, token string) (*job_.GetJobResponse, error)
	List(ctx context.Context, request *job_.ListJobRequest, token string) (*job_.ListJobResponse, error)
	Update(ctx context.Context, jobID string, request *job_.UpdateJobRequest, token string) error
	Stop(ctx context.Context, jobID string, token string) error
	Delete(ctx context.Context, jobID string, token string) error
}

// newJob returns a job.
func newJob(c *APIV1Client) *job {
	return &job{
		client: c.RESTClient(),
	}
}
