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

	"github.com/ghodss/yaml"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/errors"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/job"
	"paddleflow/pkg/job/api"
	"paddleflow/pkg/job/runtime"
)

// CreateSingleJobRequest convey request for create job
type CreateSingleJobRequest struct {
	CommonJobInfo `json:",inline"`
	JobSpec       `json:",inline"`
}

// CreateDisJobRequest convey request for create distributed job
type CreateDisJobRequest struct {
	CommonJobInfo     `json:",inline"`
	Framework         schema.Framework `json:"framework"`
	Members           []MemberSpec     `json:"members"`
	ExtensionTemplate string           `json:"extensionTemplate"`
}

// CreateWfJobRequest convey request for create workflow job
type CreateWfJobRequest struct {
	CommonJobInfo     `json:",inline"`
	Framework         schema.Framework `json:"framework"`
	Members           []MemberSpec     `json:"members"`
	ExtensionTemplate string           `json:"extensionTemplate"`
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
	Priority string `json:"priority,omitempty"`
}

// JobSpec the spec fields for jobs
type JobSpec struct {
	Flavour           schema.Flavour      `json:"flavour"`
	FileSystem        schema.FileSystem   `json:"fileSystem"`
	ExtraFileSystems  []schema.FileSystem `json:"extraFileSystems"`
	Image             string              `json:"image"`
	Env               map[string]string   `json:"env"`
	Command           string              `json:"command"`
	Args              []string            `json:"args"`
	Port              int                 `json:"port"`
	ExtensionTemplate string              `json:"extensionTemplate"`
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

// CreateSingleJob handler for creating job
func CreateSingleJob(ctx *logger.RequestContext, request *CreateSingleJobRequest) (*CreateJobResponse, error) {
	extensionTemplate := request.ExtensionTemplate
	if extensionTemplate != "" {
		bytes, err := yaml.JSONToYAML([]byte(request.ExtensionTemplate))
		if err != nil {
			log.Errorf("Failed to parse extension template to yaml: %v", err)
			return nil, err
		}
		extensionTemplate = string(bytes)
	} else {
		extensionTemplate = ""
	}
	conf := schema.Conf{
		Name:            request.CommonJobInfo.Name,
		Labels:          request.CommonJobInfo.Labels,
		Annotations:     request.CommonJobInfo.Annotations,
		Env:             request.Env,
		Port:            request.Port,
		Image:           request.Image,
		FileSystem:      request.FileSystem,
		ExtraFileSystem: request.ExtraFileSystems,
		Priority:        request.CommonJobInfo.SchedulingPolicy.Priority,
		Flavour:         request.Flavour,
	}

	if err := patchSingleEnvs(&conf, request); err != nil {
		log.Errorf("patch envs when creating job %s failed, err=%v", request.CommonJobInfo.Name, err)
		return nil, err
	}

	// execute in runtime
	id, err := createJob(&conf, request.CommonJobInfo.ID, extensionTemplate)
	if err != nil {
		log.Errorf("failed to create job %s, err=%v", request.CommonJobInfo.Name, err)
		return nil, err
	}

	response := &CreateJobResponse{
		ID: id,
	}
	return response, nil
}

func patchEnvs(conf *schema.Conf, commonJobInfo *CommonJobInfo) error {
	log.Debugf("patch envs for job %s", commonJobInfo.Name)
	// basic fields required
	conf.Labels = commonJobInfo.Labels
	conf.Annotations = commonJobInfo.Annotations
	conf.SetEnv(schema.EnvJobType, string(schema.TypeSingle))
	conf.SetUserName(commonJobInfo.UserName)
	// info in SchedulingPolicy: queue,Priority,ClusterId,Namespace
	queueName := commonJobInfo.SchedulingPolicy.Queue
	queue, err := models.GetQueueByName(queueName)
	if err != nil {
		log.Errorf("Get queue by id failed when creating job %s failed, err=%v", commonJobInfo.Name, err)
		if err == gorm.ErrRecordNotFound {
			return fmt.Errorf("queue not found by id %s", queueName)
		}
		return err
	}
	conf.SetQueueID(queue.ID)
	conf.SetEnv(schema.EnvJobQueueName, queue.Name)
	if commonJobInfo.SchedulingPolicy.Priority != "" {
		conf.Priority = commonJobInfo.SchedulingPolicy.Priority
	}
	conf.SetClusterID(queue.ClusterId)
	conf.SetNamespace(queue.Namespace)

	return nil
}

func patchSingleEnvs(conf *schema.Conf, request *CreateSingleJobRequest) error {
	log.Debugf("patchSingleEnvs conf=%#v, request=%#v", conf, request)
	// fields in request.CommonJobInfo
	if err := patchEnvs(conf, &request.CommonJobInfo); err != nil {
		log.Errorf("patch commonInfo of single job failed, err=%v", err)
		return err
	}

	// fields in request
	conf.ExtraFileSystem = request.ExtraFileSystems
	conf.Command = request.Command
	conf.Image = request.Image
	conf.Port = request.Port
	conf.Args = request.Args
	// flavour
	flavour, err := models.GetFlavour(request.Flavour.Name)
	if err != nil {
		log.Errorf("Get flavour by name %s failed when creating job %s failed, err=%v",
			request.Flavour.Name, request.CommonJobInfo.Name, err)
		return err
	}
	conf.SetFlavour(flavour.Name)
	// todo others in FileSystem
	fsID := common.ID(request.CommonJobInfo.UserName, request.FileSystem.Name)
	conf.SetFS(fsID)

	return nil
}

// CreateDistributedJob handler for creating job
func CreateDistributedJob(ctx *logger.RequestContext, request *CreateDisJobRequest) (*CreateJobResponse, error) {
	// todo(zhongzichao)
	return &CreateJobResponse{}, nil
}

// CreateWorkflowJob handler for creating job
func CreateWorkflowJob(ctx *logger.RequestContext, request *CreateWfJobRequest) (*CreateJobResponse, error) {
	var extensionTemplate string
	if request.ExtensionTemplate != "" {
		bytes, err := yaml.JSONToYAML([]byte(request.ExtensionTemplate))
		if err != nil {
			log.Errorf("Failed to parse extension template to yaml: %v", err)
			return nil, err
		}
		extensionTemplate = string(bytes)
	} else {
		return nil, fmt.Errorf("ExtensionTemplate for workflow job is needed")
	}

	// TODO: get workflow job conf
	conf := schema.Conf{
		Name:        request.Name,
		Labels:      request.Labels,
		Annotations: request.Annotations,
		Priority:    request.SchedulingPolicy.Priority,
	}
	// validate queue
	if err := job.ValidateQueue(&conf, ctx.UserName, request.SchedulingPolicy.Queue); err != nil {
		msg := fmt.Sprintf("valiate queue for workflow job failed, err: %v", err)
		log.Errorf(msg)
		return nil, fmt.Errorf(msg)
	}
	conf.SetEnv(schema.EnvJobQueueName, request.SchedulingPolicy.Queue)

	// TODO: add more fields
	// create workflow job
	jobInfo := &models.Job{
		ID:                request.ID,
		Type:              string(schema.TypeWorkflow),
		UserName:          conf.GetUserName(),
		QueueID:           conf.GetQueueID(),
		Status:            schema.StatusJobInit,
		Config:            &conf,
		ExtensionTemplate: extensionTemplate,
	}

	if err := models.CreateJob(jobInfo); err != nil {
		log.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
		return nil, fmt.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
	}
	log.Infof("create job[%s] successful.", jobInfo.ID)
	return &CreateJobResponse{ID: jobInfo.ID}, nil
}

// createJob handler for creating job, and the job_service.CreateJob will be deprecated
func createJob(conf schema.PFJobConf, jobID, jobTemplate string) (string, error) {
	if err := job.ValidateJob(conf); err != nil {
		return "", err
	}
	if err := checkPriority(conf); err != nil {
		log.Errorf("check priority failed, err=%v", err)
		return "", err
	}

	jobConf := conf.(*schema.Conf)
	jobInfo := &models.Job{
		ID:                jobID,
		Type:              string(conf.Type()),
		UserName:          conf.GetUserName(),
		QueueID:           conf.GetQueueID(),
		Status:            schema.StatusJobInit,
		Config:            jobConf,
		ExtensionTemplate: jobTemplate,
	}

	if err := models.CreateJob(jobInfo); err != nil {
		log.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
		return "", fmt.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
	}
	log.Infof("create job[%s] successful.", jobInfo.ID)
	return jobInfo.ID, nil
}

func checkPriority(conf schema.PFJobConf) error {
	// check job priority
	priority := conf.GetPriority()
	if len(priority) == 0 {
		conf.SetPriority(schema.EnvJobNormalPriority)
	} else {
		if priority != schema.EnvJobLowPriority &&
			priority != schema.EnvJobNormalPriority && priority != schema.EnvJobHighPriority {
			return errors.InvalidJobPriorityError(priority)
		}
	}
	return nil
}

func CheckPermission(ctx *logger.RequestContext) error {
	// TODO: check permission
	return nil
}

func DeleteJob(ctx *logger.RequestContext, jobID string) error {
	if err := CheckPermission(ctx); err != nil {
		return err
	}
	job, err := models.GetJobByID(jobID)
	if err != nil {
		ctx.ErrorCode = common.JobNotFound
		log.Errorf("get job from database failed, err: %v", err)
		return err
	}
	runtimeSvc, err := getRuntimeByQueue(ctx, job.QueueID)
	if err != nil {
		log.Errorf("get runtime by queue failed, err: %v", err)
		return err
	}
	pfjob, err := api.NewJobInfo(&job)
	if err != nil {
		return err
	}
	err = runtimeSvc.DeleteJob(pfjob)
	if err != nil {
		log.Errorf("delete job %s from cluster failed, err: %v", jobID, err)
		return err
	}
	err = models.DeleteJob(jobID)
	if err != nil {
		log.Errorf("delete job %s from cluster failed, err: %v", jobID, err)
		return err
	}
	return nil
}

func StopJob(ctx *logger.RequestContext, jobID string) error {
	if err := CheckPermission(ctx); err != nil {
		return err
	}
	job, err := models.GetJobByID(jobID)
	if err != nil {
		ctx.ErrorCode = common.JobNotFound
		log.Errorf("get job %s from database failed, err: %v", jobID, err)
		return err
	}
	// check job status
	if schema.IsImmutableJobStatus(job.Status) {
		msg := fmt.Sprintf("job %s status is already %s, and job cannot be stopped", jobID, job.Status)
		log.Errorf(msg)
		return fmt.Errorf(msg)
	}
	runtimeSvc, err := getRuntimeByQueue(ctx, job.QueueID)
	if err != nil {
		log.Errorf("get runtime by queue failed, err: %v", err)
		return err
	}
	pfjob, err := api.NewJobInfo(&job)
	if err != nil {
		return err
	}
	err = runtimeSvc.StopJob(pfjob)
	if err != nil {
		log.Errorf("delete job %s from cluster failed, err: %v", job.ID, err)
		return err
	}
	return nil
}

func UpdateJob(ctx *logger.RequestContext, request *UpdateJobRequest) error {
	if err := CheckPermission(ctx); err != nil {
		return err
	}
	job, err := models.GetJobByID(request.JobID)
	if err != nil {
		ctx.ErrorCode = common.JobNotFound
		log.Errorf("get job %s from database failed, err: %v", job.ID, err)
		return err
	}
	// check job status
	if !schema.IsImmutableJobStatus(job.Status) && job.Status != schema.StatusJobInit {
		// update job on cluster
		err = updateRuntimeJob(ctx, &job, request)
		if err != nil {
			ctx.ErrorCode = common.InternalError
			log.Errorf("update job %s on cluster failed, err: %v", job.ID, err)
			return err
		}
	}

	if request.Priority != "" {
		job.Config.Priority = request.Priority
	}
	for label, value := range request.Labels {
		job.Config.SetLabels(label, value)
	}
	for key, value := range request.Annotations {
		job.Config.SetAnnotations(key, value)
	}

	err = models.UpdateJobConfig(job.ID, job.Config)
	if err != nil {
		log.Errorf("update job %s on database failed, err: %v", job.ID, err)
		ctx.ErrorCode = common.DBUpdateFailed
	}
	return err
}

func updateRuntimeJob(ctx *logger.RequestContext, job *models.Job, request *UpdateJobRequest) error {
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
	pfjob.UpdateLabels(request.Labels)
	pfjob.UpdateAnnotations(request.Annotations)
	// TODO: update job priority
	return runtimeSvc.UpdateJob(pfjob)
}

func getRuntimeByQueue(ctx *logger.RequestContext, queueID string) (runtime.RuntimeService, error) {
	queue, err := models.GetQueueByID(queueID)
	if err != nil {
		log.Errorf("get queue for job failed, err: %v", err)
		return nil, err
	}
	// TODO: GetOrCreateRuntime by cluster id
	clusterInfo, err := models.GetClusterById(queue.ClusterId)
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
