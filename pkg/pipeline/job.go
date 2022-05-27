/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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

package pipeline

import (
	"errors"
	"fmt"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job"
)

type Job interface {
	Job() BaseJob
	Update(cmd string, params map[string]string, envs map[string]string, artifacts *schema.Artifacts) error
	Validate() error
	Start() (string, error)
	Stop() error
	Check() (schema.JobStatus, error)
	Watch(chan WorkflowEvent) error
	Started() bool
	Succeeded() bool
	Failed() bool
	Terminated() bool
	Skipped() bool
	NotEnded() bool
}

func NewBaseJob(name, deps string) *BaseJob {
	return &BaseJob{
		Name: name,
		Deps: deps,
	}
}

type BaseJob struct {
	Id         string            `json:"jobID"`
	Name       string            `json:"name"`       // step名字，不同run的不同step，必须拥有不同名字
	Command    string            `json:"command"`    // 区别于step，是替换后的，可以直接运行
	Parameters map[string]string `json:"parameters"` // 区别于step，是替换后的，可以直接运行
	Artifacts  schema.Artifacts  `json:"artifacts"`  // 区别于step，是替换后的，可以直接运行
	Env        map[string]string `json:"env"`
	StartTime  string            `json:"startTime"`
	EndTime    string            `json:"endTime"`
	Status     schema.JobStatus  `json:"status"`
	Deps       string            `json:"deps"`
	Message    string            `json:"message"`
}

// ----------------------------------------------------------------------------
//  K8S Job
// ----------------------------------------------------------------------------
type PaddleFlowJob struct {
	BaseJob
	Image string
}

func NewPaddleFlowJob(name, image, deps string) *PaddleFlowJob {
	return &PaddleFlowJob{
		BaseJob: *NewBaseJob(name, deps),
		Image:   image,
	}
}

// 发起作业接口
func (pfj *PaddleFlowJob) Update(cmd string, params map[string]string, envs map[string]string, artifacts *schema.Artifacts) error {
	if cmd != "" {
		pfj.Command = cmd
	}

	if params != nil {
		pfj.Parameters = params
	}

	if envs != nil {
		pfj.Env = envs
	}

	if artifacts != nil {
		pfj.Artifacts = *artifacts
	}

	return nil
}

// 校验job参数
func (pfj *PaddleFlowJob) Validate() error {
	var err error

	// 调用job子系统接口进行校验
	conf := schema.Conf{
		Name:    pfj.Name,
		Env:     pfj.Env,
		Command: pfj.Command,
		Image:   pfj.Image,
	}

	err = job.ValidateJob(&conf)
	if err != nil {
		return err
	}

	return nil
}

// 发起作业接口
func (pfj *PaddleFlowJob) Start() (string, error) {
	// 此函数不更新job.Status，job.startTime，统一通过watch更新
	var err error

	// 调用job子系统接口发起运行
	conf := schema.Conf{
		Name:    pfj.Name,
		Env:     pfj.Env,
		Command: pfj.Command,
		Image:   pfj.Image,
	}

	pfj.Id, err = job.CreateJob(&conf)
	if err != nil {
		return "", err
	}

	return pfj.Id, nil
}

// 停止作业接口
func (pfj *PaddleFlowJob) Stop() error {
	// 此函数不更新job.Status，job.endTime，统一通过watch更新
	err := job.StopJobByID(pfj.Id)
	if err != nil {
		return err
	}

	return nil
}

// 查作业状态接口
func (pfj *PaddleFlowJob) Check() (schema.JobStatus, error) {
	if pfj.Id == "" {
		errMsg := fmt.Sprintf("job not started, id is empty!")
		err := errors.New(errMsg)
		return "", err
	}
	status, err := models.GetJobStatusByID(pfj.Id)
	if err != nil {
		return "", err
	}
	return status, nil
}

// 同步watch作业接口
func (pfj *PaddleFlowJob) Watch(ch chan WorkflowEvent) error {
	defer close(ch)

	const TryMax = 5
	tryCount := 0
	for {
		if pfj.Id == "" {
			errMsg := fmt.Sprintf("watch paddleflow job failed, job not started, id is empty!")
			wfe := NewWorkflowEvent(WfEventJobWatchErr, errMsg, nil)
			ch <- *wfe
			return nil
		}

		// 在连续查询job子系统出错的情况下，把错误信息返回给run，但不会停止轮询
		jobInstance, err := models.GetJobByID(pfj.Id)
		if err != nil {
			if tryCount < TryMax {
				tryCount += 1
			} else {
				tryCount = 0
				errMsg := fmt.Sprintf("get job by jobid[%s] failed: %s", pfj.Id, err.Error())
				wfe := NewWorkflowEvent(WfEventJobWatchErr, errMsg, nil)
				ch <- *wfe
			}

			continue
		}

		tryCount = 0
		startTime := jobInstance.CreatedAt.Format("2006-01-02 15:04:05")
		if startTime != pfj.StartTime {
			pfj.StartTime = startTime
		}

		if jobInstance.Status != pfj.Status || jobInstance.Message != pfj.Message {
			extra := map[string]interface{}{
				"status":    jobInstance.Status,
				"preStatus": pfj.Status,
				"jobid":     pfj.Id,
				"message":   jobInstance.Message,
			}
			wfe := NewWorkflowEvent(WfEventJobUpdate, "", extra)
			ch <- *wfe
			pfj.Status = jobInstance.Status
			pfj.Message = jobInstance.Message
		}

		if pfj.Succeeded() || pfj.Terminated() || pfj.Failed() {
			pfj.EndTime = jobInstance.UpdatedAt.Format("2006-01-02 15:04:05")
			break
		}
		time.Sleep(time.Second * 3)
	}
	return nil
}

func (pfj *PaddleFlowJob) Succeeded() bool {
	return pfj.Status == schema.StatusJobSucceeded
}

func (pfj *PaddleFlowJob) Failed() bool {
	return pfj.Status == schema.StatusJobFailed
}

func (pfj *PaddleFlowJob) Terminated() bool {
	return pfj.Status == schema.StatusJobTerminated
}

func (pfj *PaddleFlowJob) Skipped() bool {
	return pfj.Status == schema.StatusJobSkipped
}

func (pfj *PaddleFlowJob) Cancelled() bool {
	return pfj.Status == schema.StatusJobCancelled
}

func (pfj *PaddleFlowJob) NotEnded() bool {
	return pfj.Status == "" || pfj.Status == schema.StatusJobTerminating || pfj.Status == schema.StatusJobRunning || pfj.Status == schema.StatusJobPending
}

func (pfj *PaddleFlowJob) Started() bool {
	return pfj.Status != ""
}

func (pfj *PaddleFlowJob) Job() BaseJob {
	return pfj.BaseJob
}

// ----------------------------------------------------------------------------
// Local Process Job
// ----------------------------------------------------------------------------
type LocalJob struct {
	BaseJob
	Pid string
}
