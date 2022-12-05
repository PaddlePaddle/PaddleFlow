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

package job

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

// CreateSingleJobRequest convey request for create job
type CreateSingleJobRequest struct {
	CommonJobInfo `json:",inline"`
	JobSpec       `json:",inline"`
}

func (sj CreateSingleJobRequest) ToJobInfo() *CreateJobInfo {
	return &CreateJobInfo{
		CommonJobInfo: sj.CommonJobInfo,
		Framework:     schema.FrameworkStandalone,
		Type:          schema.TypeSingle,
		Members: []MemberSpec{
			{
				CommonJobInfo: sj.CommonJobInfo,
				JobSpec:       sj.JobSpec,
				Role:          string(schema.RoleWorker),
				Replicas:      1,
			},
		},
		ExtensionTemplate: sj.JobSpec.ExtensionTemplate,
	}
}

// CreateDisJobRequest convey request for create distributed job
type CreateDisJobRequest struct {
	CommonJobInfo     `json:",inline"`
	Framework         schema.Framework       `json:"framework"`
	Members           []MemberSpec           `json:"members"`
	ExtensionTemplate map[string]interface{} `json:"extensionTemplate"`
}

func (ds CreateDisJobRequest) ToJobInfo() *CreateJobInfo {
	return &CreateJobInfo{
		CommonJobInfo:     ds.CommonJobInfo,
		Framework:         ds.Framework,
		Type:              schema.TypeDistributed,
		Members:           ds.Members,
		ExtensionTemplate: ds.ExtensionTemplate,
	}
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
	Queue        string              `json:"queue"`
	QueueID      string              `json:"-"`
	MaxResources *resources.Resource `json:"-"`
	QueueType    string              `json:"-"`
	ClusterId    string              `json:"-"`
	Namespace    string              `json:"-"`
	Priority     string              `json:"priority,omitempty"`
}

// JobSpec the spec fields for jobs
type JobSpec struct {
	Flavour           schema.Flavour         `json:"flavour"`
	FileSystem        schema.FileSystem      `json:"fs"`
	ExtraFileSystems  []schema.FileSystem    `json:"extraFS"`
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

func DeleteJob(ctx *logger.RequestContext, jobID string) error {
	job, err := storage.Job.GetJobByID(jobID)
	if err != nil {
		ctx.ErrorCode = common.JobNotFound
		msg := fmt.Sprintf("get job %s failed, err: %v", jobID, err)
		log.Errorf(msg)
		return fmt.Errorf(msg)
	}
	if err = common.CheckPermission(ctx.UserName, job.UserName, common.ResourceTypeJob, jobID); err != nil {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return err
	}

	// check job status before delete
	if !schema.IsImmutableJobStatus(job.Status) {
		ctx.ErrorCode = common.ActionNotAllowed
		msg := fmt.Sprintf("job %s status is %s, please stop it first.", jobID, job.Status)
		log.Errorf(msg)
		return fmt.Errorf(msg)
	}
	err = storage.Job.DeleteJob(jobID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		log.Errorf("delete job %s from cluster failed, err: %v", jobID, err)
		return err
	}
	return nil
}

func StopJob(ctx *logger.RequestContext, jobID string) error {
	job, err := storage.Job.GetJobByID(jobID)
	if err != nil {
		ctx.ErrorCode = common.JobNotFound
		log.Errorf("get job %s from database failed, err: %v", jobID, err)
		return err
	}
	if err = common.CheckPermission(ctx.UserName, job.UserName, common.ResourceTypeJob, jobID); err != nil {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return err
	}
	// check job status
	if schema.IsImmutableJobStatus(job.Status) {
		msg := fmt.Sprintf("job %s status is already %s, and job cannot be stopped", jobID, job.Status)
		log.Errorf(msg)
		return fmt.Errorf(msg)
	}

	if job.Status == schema.StatusJobInit {
		err = storage.Job.UpdateJobStatus(jobID, "job is terminated.", schema.StatusJobTerminated)
	} else {
		var runtimeSvc runtime.RuntimeService
		runtimeSvc, err = getRuntimeByQueue(ctx, job.QueueID)
		if err != nil {
			log.Errorf("get runtime by queue failed, err: %v", err)
			return err
		}
		// stop job on cluster
		go func(job *model.Job, runtimeSvc runtime.RuntimeService) {
			pfjob, err := api.NewJobInfo(job)
			if err != nil {
				return
			}
			err = runtimeSvc.StopJob(pfjob)
			if err != nil {
				log.Errorf("delete job %s from cluster failed, err: %v", job.ID, err)
				return
			}
		}(&job, runtimeSvc)
		// update job status
		err = storage.Job.UpdateJobStatus(jobID, "job is terminating.", schema.StatusJobTerminating)
	}
	if err != nil {
		log.Errorf("update job[%s] status to [%s] failed, err: %v", jobID, schema.StatusJobTerminating, err)
		return err
	}
	return nil
}

func UpdateJob(ctx *logger.RequestContext, request *UpdateJobRequest) error {
	job, err := storage.Job.GetJobByID(request.JobID)
	if err != nil {
		ctx.ErrorCode = common.JobNotFound
		log.Errorf("get job %s from database failed, err: %v", job.ID, err)
		return err
	}
	if err = common.CheckPermission(ctx.UserName, job.UserName, common.ResourceTypeJob, request.JobID); err != nil {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return err
	}

	// check job status when update job on cluster
	needUpdateCluster := false
	if request.Priority != "" {
		// need to update job priority
		if job.Status != schema.StatusJobPending && job.Status != schema.StatusJobInit {
			ctx.ErrorCode = common.ActionNotAllowed
			err = fmt.Errorf("the status of job %s is %s, job priority cannot be updated", job.ID, job.Status)
			log.Errorln(err)
			return err
		}
		needUpdateCluster = job.Status == schema.StatusJobPending
	} else {
		// need to update job labels or annotations
		if job.Status == schema.StatusJobPending || job.Status == schema.StatusJobRunning {
			needUpdateCluster = true
		}
	}

	if needUpdateCluster {
		// update job on cluster
		err = updateRuntimeJob(ctx, &job, request)
		if err != nil {
			ctx.ErrorCode = common.InternalError
			log.Errorf("update job %s on cluster failed, err: %v", job.ID, err)
			return err
		}
	}

	// update job on database
	if request.Priority != "" {
		job.Config.Priority = request.Priority
	}
	for label, value := range request.Labels {
		job.Config.SetLabels(label, value)
	}
	for key, value := range request.Annotations {
		job.Config.SetAnnotations(key, value)
	}

	err = storage.Job.UpdateJobConfig(job.ID, job.Config)
	if err != nil {
		log.Errorf("update job %s on database failed, err: %v", job.ID, err)
		ctx.ErrorCode = common.DBUpdateFailed
	}
	return err
}

func updateRuntimeJob(ctx *logger.RequestContext, job *model.Job, request *UpdateJobRequest) error {
	// update labels and annotations
	runtimeSvc, err := getRuntimeByQueue(ctx, job.QueueID)
	if err != nil {
		log.Errorf("get cluster runtime failed, err: %v", err)
		return err
	}
	pfjob, err := api.NewJobInfo(job)
	if err != nil {
		log.Errorf("new paddleflow job failed, err: %v", err)
		return err
	}
	if request.Labels != nil {
		pfjob.UpdateLabels(request.Labels)
	}
	if request.Annotations != nil {
		pfjob.UpdateAnnotations(request.Annotations)
	}
	if request.Priority != "" {
		pfjob.UpdateJobPriority(request.Priority)
	}
	return runtimeSvc.UpdateJob(pfjob)
}

func getRuntimeByQueue(ctx *logger.RequestContext, queueID string) (runtime.RuntimeService, error) {
	queue, err := storage.Queue.GetQueueByID(queueID)
	if err != nil {
		log.Errorf("get queue for job failed, err: %v", err)
		return nil, err
	}
	// TODO: GetOrCreateRuntime by cluster id
	clusterInfo, err := storage.Cluster.GetClusterById(queue.ClusterId)
	if err != nil {
		ctx.Logging().Errorf("get clusterInfo by id %s failed. error: %s",
			queue.ClusterId, err.Error())
		return nil, err
	}
	runtimeSvc, err := runtime.GetOrCreateRuntime(clusterInfo)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("get or create runtime failed, err: %v", err)
		return nil, fmt.Errorf("delete queue failed")
	}
	return runtimeSvc, nil
}
