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
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
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

// CreateSingleJobRequest convey request for create job
type CreateSingleJobRequest struct {
	CommonJobInfo `json:",inline"`
	JobSpec       `json:",inline"`
}

// CreateDisJobRequest convey request for create distributed job
type CreateDisJobRequest struct {
	CommonJobInfo     `json:",inline"`
	Framework         schema.Framework       `json:"framework"`
	Members           []MemberSpec           `json:"members"`
	ExtensionTemplate map[string]interface{} `json:"extensionTemplate"`
}

// CreateWfJobRequest convey request for create workflow job
type CreateWfJobRequest struct {
	CommonJobInfo     `json:",inline"`
	Framework         schema.Framework       `json:"framework"`
	Members           []MemberSpec           `json:"members"`
	ExtensionTemplate map[string]interface{} `json:"extensionTemplate"`
}

// CommonJobInfo the common fields for jobs
type CommonJobInfo struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Labels           map[string]string `json:"labels"`
	Annotations      map[string]string `json:"annotations"`
	SchedulingPolicy SchedulingPolicy  `json:"schedulingPolicy"`
	UserName         string            `json:",omitempty"`
}

// SchedulingPolicy indicate queueID/priority
type SchedulingPolicy struct {
	Queue    string `json:"queue"`
	QueueID  string `json:"-"`
	Priority string `json:"priority,omitempty"`
}

// JobSpec the spec fields for jobs
type JobSpec struct {
	Flavour           schema.Flavour         `json:"flavour"`
	FileSystem        schema.FileSystem      `json:"fileSystem"`
	ExtraFileSystems  []schema.FileSystem    `json:"extraFileSystems"`
	Image             string                 `json:"image"`
	Env               map[string]string      `json:"env"`
	Command           string                 `json:"command"`
	Args              []string               `json:"args"`
	Port              int                    `json:"port"`
	ExtensionTemplate map[string]interface{} `json:"extensionTemplate"`
}

type MemberSpec struct {
	CommonJobInfo `json:",inline"`
	JobSpec       `json:",inline"`
	Role          string `json:"role"`
	Replicas      int    `json:"replicas"`
}

type UpdateJobRequest struct {
	JobID       string            `json:"-"`
	Priority    string            `json:"priority"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

// CreateJobResponse convey response for create job
type CreateJobResponse struct {
	ID string `json:"id"`
}

type Member struct {
	ID          string            `json:"id"`
	Replicas    int               `json:"replicas"`
	Role        schema.MemberRole `json:"role"`
	schema.Conf `json:",inline"`
}

type DistributedJobSpec struct {
	Framework schema.Framework `json:"framework,omitempty"`
	Members   []Member         `json:"members,omitempty"`
}

type ListJobRequest struct {
	Queue     string            `json:"queue,omitempty"`
	Status    string            `json:"status,omitempty"`
	Timestamp int64             `json:"timestamp,omitempty"`
	StartTime string            `json:"startTime,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Marker    string            `json:"marker"`
	MaxKeys   int               `json:"maxKeys"`
}

type ListJobResponse struct {
	common.MarkerInfo
	JobList []*GetJobResponse `json:"jobList"`
}

type GetJobResponse struct {
	CreateSingleJobRequest `json:",inline"`
	DistributedJobSpec     `json:",inline"`
	Status                 string                  `json:"status"`
	Message                string                  `json:"message"`
	AcceptTime             string                  `json:"acceptTime"`
	StartTime              string                  `json:"startTime"`
	FinishTime             string                  `json:"finishTime"`
	Runtime                *RuntimeInfo            `json:"runtime,omitempty"`
	DistributedRuntime     *DistributedRuntimeInfo `json:"distributedRuntime,omitempty"`
	WorkflowRuntime        *WorkflowRuntimeInfo    `json:"workflowRuntime,omitempty"`
	UpdateTime             time.Time               `json:"-"`
}

type RuntimeInfo struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	ID        string `json:"id,omitempty"`
	Status    string `json:"status,omitempty"`
}

type DistributedRuntimeInfo struct {
	Name      string        `json:"name,omitempty"`
	Namespace string        `json:"namespace,omitempty"`
	ID        string        `json:"id,omitempty"`
	Status    string        `json:"status,omitempty"`
	Runtimes  []RuntimeInfo `json:"runtimes,omitempty"`
}

type WorkflowRuntimeInfo struct {
	Name      string                   `json:"name,omitempty"`
	Namespace string                   `json:"namespace,omitempty"`
	ID        string                   `json:"id,omitempty"`
	Status    string                   `json:"status,omitempty"`
	Nodes     []DistributedRuntimeInfo `json:"nodes,omitempty"`
}

func (j *job) Create(ctx context.Context, single *CreateSingleJobRequest, distributed *CreateDisJobRequest,
	wf *CreateWfJobRequest, token string) (result *CreateJobResponse, err error) {
	result = &CreateJobResponse{}
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
	token string) (result *GetJobResponse, err error) {
	result = &GetJobResponse{}
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

func (j *job) List(ctx context.Context, request *ListJobRequest,
	token string) (result *ListJobResponse, err error) {
	result = &ListJobResponse{}
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

func (j *job) Update(ctx context.Context, jobID string, request *UpdateJobRequest,
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
	Create(ctx context.Context, single *CreateSingleJobRequest, distributed *CreateDisJobRequest,
		wf *CreateWfJobRequest, token string) (*CreateJobResponse, error)
	Get(ctx context.Context, jobID string, token string) (*GetJobResponse, error)
	List(ctx context.Context, request *ListJobRequest, token string) (*ListJobResponse, error)
	Update(ctx context.Context, jobID string, request *UpdateJobRequest, token string) error
	Stop(ctx context.Context, jobID string, token string) error
	Delete(ctx context.Context, jobID string, token string) error
}

// newJob returns a job.
func newJob(c *APIV1Client) *job {
	return &job{
		client: c.RESTClient(),
	}
}
