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
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/ghodss/yaml"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/flavour"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
)

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
	Queue        string              `json:"queue"`
	QueueID      string              `json:"-"`
	MaxResources *resources.Resource `json:"-"`
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

// CreateSingleJob handler for creating job
func CreateSingleJob(ctx *logger.RequestContext, request *CreateSingleJobRequest) (*CreateJobResponse, error) {
	if err := CheckPermission(ctx); err != nil {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return nil, err
	}

	// validate single Job
	if err := validateSingleJob(ctx, request); err != nil {
		ctx.ErrorCode = common.JobInvalidField
		ctx.Logging().Errorf("validate job request failed. request:%v error:%s", request, err.Error())
		return nil, err
	}
	templateJson, err := newExtensionTemplateJson(request.ExtensionTemplate)
	if err != nil {
		ctx.Logging().Errorf("parse extension template failed, job[%v], err=%v", request, err)
		return nil, err
	}

	// new job conf and set values
	conf := schema.Conf{
		Name: request.Name,
		// 存储资源
		FileSystem:      request.FileSystem,
		ExtraFileSystem: request.ExtraFileSystems,
		// 计算资源
		Flavour:  request.Flavour,
		Priority: request.SchedulingPolicy.Priority,
		// 运行时需要的参数
		Labels:      request.Labels,
		Annotations: request.Annotations,
		Env:         request.Env,
		Port:        request.Port,
		Image:       request.Image,
		Args:        request.Args,
		Command:     request.Command,
	}

	if err := patchSingleConf(&conf, request); err != nil {
		log.Errorf("patch envs when creating job %s failed, err=%v", request.CommonJobInfo.Name, err)
		return nil, err
	}

	jobInfo := &models.Job{
		ID:                request.ID,
		Name:              request.Name,
		Type:              string(schema.TypeSingle),
		UserName:          request.UserName,
		QueueID:           request.SchedulingPolicy.QueueID,
		Status:            schema.StatusJobInit,
		Framework:         schema.FrameworkStandalone,
		Config:            &conf,
		ExtensionTemplate: templateJson,
	}
	log.Debugf("create single job %#v", jobInfo)
	if err := models.CreateJob(jobInfo); err != nil {
		log.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
		return nil, fmt.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
	}

	log.Infof("create single job[%s] successful.", jobInfo.ID)
	response := &CreateJobResponse{
		ID: jobInfo.ID,
	}
	return response, nil
}

// newExtensionTemplateJson parse extensionTemplate
func newExtensionTemplateJson(extensionTemplate map[string]interface{}) (string, error) {
	yamlExtensionTemplate := ""
	if extensionTemplate != nil && len(extensionTemplate) > 0 {
		extensionTemplateJSON, err := json.Marshal(&extensionTemplate)
		bytes, err := yaml.JSONToYAML(extensionTemplateJSON)
		if err != nil {
			log.Errorf("Failed to parse extension template to yaml: %v", err)
			return "", err
		}
		yamlExtensionTemplate = string(bytes)
	}
	return yamlExtensionTemplate, nil
}

func patchSingleConf(conf *schema.Conf, request *CreateSingleJobRequest) error {
	log.Debugf("patchSingleConf conf=%#v, request=%#v", conf, request)
	if err := patchFromJobSpec(conf, &request.JobSpec, request.UserName); err != nil {
		log.Errorf("patch from JobSpec failed, err=%v", err)
		return err
	}
	if err := patchFromCommonInfo(conf, &request.CommonJobInfo); err != nil {
		log.Errorf("patch from CommonJobInfo failed, err=%v", err)
		return err
	}
	return nil
}

func patchFromJobSpec(conf *schema.Conf, jobSpec *JobSpec, userName string) error {
	var err error
	conf.Flavour, err = flavour.GetFlavourWithCheck(jobSpec.Flavour)
	if err != nil {
		log.Errorf("get flavour failed when create job, err:%v", err)
		return err
	}
	return nil
}

func patchFromCommonInfo(conf *schema.Conf, commonJobInfo *CommonJobInfo) error {
	log.Debugf("patch envs for job %s", commonJobInfo.Name)
	// basic fields required
	conf.Labels = commonJobInfo.Labels
	conf.Annotations = commonJobInfo.Annotations
	// info in SchedulingPolicy: queue,Priority,ClusterId,Namespace
	schedulingPolicy := commonJobInfo.SchedulingPolicy
	conf.SetQueueID(schedulingPolicy.QueueID)
	conf.SetQueueName(schedulingPolicy.Queue)
	conf.SetPriority(schedulingPolicy.Priority)
	conf.SetClusterID(schedulingPolicy.ClusterId)
	conf.SetNamespace(schedulingPolicy.Namespace)

	return nil
}

// CreateDistributedJob handler for creating job
func CreateDistributedJob(ctx *logger.RequestContext, request *CreateDisJobRequest) (*CreateJobResponse, error) {
	log.Debugf("CreateDistributedJob request=%#v", request)
	if err := CheckPermission(ctx); err != nil {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return nil, err
	}
	// validate Job
	if err := validateDistributedJob(ctx, request); err != nil {
		ctx.Logging().Errorf("validate job request failed. request:%v error:%s", request, err.Error())
		return nil, err
	}
	request.UserName = ctx.UserName
	var err error
	templateJson, err := newExtensionTemplateJson(request.ExtensionTemplate)
	if err != nil {
		log.Errorf("parse extension template failed, err=%v", err)
		return nil, err
	}

	jobInfo := &models.Job{
		ID:                request.ID,
		Name:              request.Name,
		UserName:          request.UserName,
		QueueID:           request.SchedulingPolicy.QueueID,
		Type:              string(schema.TypeDistributed),
		Status:            schema.StatusJobInit,
		Framework:         request.Framework,
		ExtensionTemplate: templateJson,
	}

	conf := schema.Conf{
		Name:        request.Name,
		Labels:      request.Labels,
		Annotations: request.Annotations,
		Priority:    request.SchedulingPolicy.Priority,
	}

	if err := patchDistributedConf(&conf, request); err != nil {
		log.Errorf("patch envs when creating job %s failed, err=%v", request.CommonJobInfo.Name, err)
		return nil, err
	}
	// set roles for members
	if len(request.ExtensionTemplate) == 0 {
		jobMode, err := validateJobMode(ctx, request)
		if err != nil || jobMode == "" {
			log.Errorf("create members failed, err=%v", err)
			return nil, err
		}
		switch jobMode {
		case schema.EnvJobModeCollective:
			// validate replicas
			if request.Members[0].Replicas < 2 {
				ctx.ErrorCode = common.JobInvalidField
				ctx.Logging().Errorln("replicas must be greater than 1")
				return nil, fmt.Errorf("replicas must be greater than 1")
			}
			conf.SetEnv(schema.EnvJobMode, schema.EnvJobModeCollective)
			if jobInfo.Members, err = newCollectiveMembers(request); err != nil {
				log.Errorf("create job with collective members failed, err=%v", err)
				return nil, err
			}
		case schema.EnvJobModePS:
			conf.SetEnv(schema.EnvJobMode, schema.EnvJobModePS)
			if jobInfo.Members, err = newPSMembers(request); err != nil {
				ctx.ErrorCode = common.JobInvalidField
				log.Errorf("create job with ps members failed, err=%v", err)
				return nil, err
			}
		default:
			log.Errorf("invalid members number, cannot recognize job mode %s", jobMode)
			return nil, fmt.Errorf("invalid job mode %s", jobMode)
		}
	}

	jobInfo.Config = &conf

	log.Debugf("create distributed job %#v", jobInfo)
	if err := models.CreateJob(jobInfo); err != nil {
		log.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
		return nil, fmt.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
	}

	log.Infof("create job[%s] successful.", jobInfo.ID)
	response := &CreateJobResponse{
		ID: jobInfo.ID,
	}
	return response, nil
}

func validateJobMode(ctx *logger.RequestContext, request *CreateDisJobRequest) (string, error) {
	if len(request.Members) == 1 && (request.Members[0].Role == string(schema.RolePWorker) ||
		request.Members[0].Role == string(schema.RoleWorker)) {
		log.Debugf("create distributed job %s with collective mode", request.CommonJobInfo.ID)
		return schema.EnvJobModeCollective, nil
	} else if isPSMode(request.Members) {
		log.Debugf("create distributed job %s with ps mode", request.CommonJobInfo.ID)
		return schema.EnvJobModePS, nil
	}
	ctx.ErrorCode = common.JobInvalidField
	ctx.Logging().Errorf("create distributed job %s failed, invalid members number %d", request.CommonJobInfo.ID,
		len(request.Members))
	return "", fmt.Errorf("create distributed job %s failed, invalid members number %d", request.CommonJobInfo.ID,
		len(request.Members))
}

func isPSMode(members []MemberSpec) bool {
	if len(members) != 2 {
		return false
	}
	var pserver, pworker, driver, executor bool
	for _, member := range members {
		switch member.Role {
		case string(schema.RolePWorker):
			pserver = true
		case string(schema.RolePServer):
			pworker = true
		case string(schema.RoleDriver):
			driver = true
		case string(schema.RoleExecutor):
			executor = true
		}
	}
	if (pserver && pworker) || (driver && executor) {
		return true
	}
	return false
}

func patchDistributedConf(conf *schema.Conf, request *CreateDisJobRequest) error {
	log.Debugf("patchSingleConf conf=%#v, request=%#v", conf, request)
	// fields in request.CommonJobInfo
	patchFromCommonInfo(conf, &request.CommonJobInfo)
	// info in SchedulingPolicy: queue,Priority,ClusterId,Namespace
	if request.SchedulingPolicy.Priority != "" {
		conf.Priority = request.SchedulingPolicy.Priority
	}

	return nil
}

func newCollectiveMembers(request *CreateDisJobRequest) ([]models.Member, error) {
	members := make([]models.Member, 0)
	for _, reqMem := range request.Members {
		reqMem.UserName = request.UserName
		// validate FileSystem
		if err := validateFileSystem(&reqMem.JobSpec, request.UserName); err != nil {
			return nil, err
		}
		if reqMem.Role == string(schema.RoleWorker) {
			member, err := newMember(reqMem, schema.RoleWorker)
			if err != nil {
				log.Errorf("create collective members failed, err: %v", err)
				return nil, err
			}
			patchFromCommonInfo(&member.Conf, &request.CommonJobInfo)
			members = append(members, member)
		}
	}
	return members, nil
}

func newPSMembers(request *CreateDisJobRequest) ([]models.Member, error) {
	members := make([]models.Member, 0)
	for _, reqMember := range request.Members {
		reqMember.UserName = request.UserName
		// validate FileSystem
		if err := validateFileSystem(&reqMember.JobSpec, request.UserName); err != nil {
			return nil, err
		}

		var member models.Member
		var err error
		member, err = newMember(reqMember, schema.MemberRole(reqMember.Role))
		if err != nil {
			log.Errorf("create ps members failed, err=%v", err)
			return nil, err
		}
		patchFromCommonInfo(&member.Conf, &request.CommonJobInfo)
		members = append(members, member)
	}
	return members, nil
}

// newMember create models.member from request.Member
func newMember(member MemberSpec, role schema.MemberRole) (models.Member, error) {
	f, err := flavour.GetFlavourWithCheck(member.Flavour)
	if err != nil {
		log.Errorf("get flavour failed, err:%v", err)
		return models.Member{}, err
	}
	if member.Replicas < 1 {
		log.Errorf("member with invalid replicas %d", member.Replicas)
		return models.Member{}, fmt.Errorf("invalid replicas %d", member.Replicas)
	}

	conf := schema.Conf{
		Name: member.Name,
		// 存储资源
		FileSystem:      member.FileSystem,
		ExtraFileSystem: member.ExtraFileSystems,
		// 计算资源
		Flavour:  f,
		Priority: member.SchedulingPolicy.Priority,
		QueueID:  member.SchedulingPolicy.QueueID,
		// 运行时需要的参数
		Labels:      member.Labels,
		Annotations: member.Annotations,
		Env:         member.Env,
		Command:     member.Command,
		Image:       member.Image,
		Port:        member.Port,
		Args:        member.Args,
	}

	return models.Member{
		ID:       member.ID,
		Role:     role,
		Replicas: member.Replicas,
		Conf:     conf,
	}, nil
}

// CreateWorkflowJob handler for creating job
func CreateWorkflowJob(ctx *logger.RequestContext, request *CreateWfJobRequest) (*CreateJobResponse, error) {
	if err := CheckPermission(ctx); err != nil {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return nil, err
	}
	if err := validateWorkflowJob(ctx, request); err != nil {
		ctx.Logging().Errorf("validate job request failed. request:%v error:%s", request, err.Error())
		return nil, err
	}

	var templateJson string
	if request.ExtensionTemplate == nil {
		return nil, fmt.Errorf("ExtensionTemplate for workflow job is needed")
	}
	var err error
	templateJson, err = newExtensionTemplateJson(request.ExtensionTemplate)
	if err != nil {
		log.Errorf("parse extension template failed, err=%v", err)
		return nil, err
	}

	// TODO: get workflow job conf
	conf := schema.Conf{
		Name:        request.Name,
		Labels:      request.Labels,
		Annotations: request.Annotations,
		Priority:    request.SchedulingPolicy.Priority,
	}
	// validate queue
	if err := ValidateQueue(&conf, ctx.UserName, request.SchedulingPolicy.Queue); err != nil {
		msg := fmt.Sprintf("valiate queue for workflow job failed, err: %v", err)
		log.Errorf(msg)
		return nil, fmt.Errorf(msg)
	}
	conf.SetEnv(schema.EnvJobQueueName, request.SchedulingPolicy.Queue)

	// create workflow job
	jobInfo := &models.Job{
		ID:                request.ID,
		Name:              request.Name,
		Type:              string(schema.TypeWorkflow),
		UserName:          conf.GetUserName(),
		QueueID:           conf.GetQueueID(),
		Status:            schema.StatusJobInit,
		Config:            &conf,
		ExtensionTemplate: templateJson,
	}

	if err := models.CreateJob(jobInfo); err != nil {
		log.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
		return nil, fmt.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
	}
	log.Infof("create job[%s] successful.", jobInfo.ID)
	return &CreateJobResponse{ID: jobInfo.ID}, nil
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
		msg := fmt.Sprintf("get job %s failed, err: %v", jobID, err)
		log.Errorf(msg)
		return fmt.Errorf(msg)
	}
	// check job status before delete
	if !schema.IsImmutableJobStatus(job.Status) {
		ctx.ErrorCode = common.ActionNotAllowed
		msg := fmt.Sprintf("job %s status is %s, please stop it first.", jobID, job.Status)
		log.Errorf(msg)
		return fmt.Errorf(msg)
	}
	err = models.DeleteJob(jobID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
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

	// stop job on cluster
	go func(job *models.Job, runtimeSvc runtime.RuntimeService) {
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

	if err = models.UpdateJobStatus(jobID, "job is terminating.", schema.StatusJobTerminating); err != nil {
		log.Errorf("update job[%s] status to [%s] failed, err: %v", jobID, schema.StatusJobTerminating, err)
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

func validateDistributedJob(ctx *logger.RequestContext, request *CreateDisJobRequest) error {
	if err := validateCommonJobInfo(ctx, &request.CommonJobInfo); err != nil {
		log.Errorf("validateCommonJobInfo failed, err: %v", err)
		return err
	}

	switch request.Framework {
	case schema.FrameworkSpark, schema.FrameworkPaddle:
		break
	case schema.FrameworkTF, schema.FrameworkMPI:
		ctx.Logging().Errorf("framework: %s will be supported in the future", request.Framework)
		ctx.ErrorCode = common.JobInvalidField
		return fmt.Errorf("framework: %s will be supported in the future", request.Framework)
	default:
		ctx.Logging().Errorf("invalid framework: %s", request.Framework)
		ctx.ErrorCode = common.JobInvalidField
		return fmt.Errorf("invalid framework: %s", request.Framework)
	}

	if len(request.ExtensionTemplate) != 0 {
		log.Infof("RequestID[%s]: request.ExtensionTemplate is not empty, pass validate members", ctx.RequestID)
		return nil
	}

	if err := validateMembers(ctx, request.Members, request.SchedulingPolicy); err != nil {
		ctx.Logging().Errorf("RequestID[%s]: validate members failed, err: %v", ctx.RequestID, err)
		return err
	}

	return nil
}

func validateWorkflowJob(ctx *logger.RequestContext, request *CreateWfJobRequest) error {
	if err := validateCommonJobInfo(ctx, &request.CommonJobInfo); err != nil {
		log.Errorf("WorkflowJob validateCommonJobInfo failed, err: %v", err)
		return err
	}

	if request.ExtensionTemplate == nil {
		ctx.ErrorCode = common.RequiredFieldEmpty
		err := fmt.Errorf("ExtensionTemplate for workflow job is needed, and now is empty")
		ctx.Logging().Errorf("create workflow job failed. error: %s", err.Error())
		return err
	}
	return nil
}

func validateSingleJob(ctx *logger.RequestContext, request *CreateSingleJobRequest) error {
	// ensure required fields
	emptyFields := validateEmptyFieldInSingle(request)
	if len(emptyFields) != 0 {
		emptyFieldStr := strings.Join(emptyFields, ",")
		err := fmt.Errorf("required fields in {%s} are empty, please fill it", emptyFieldStr)
		ctx.Logging().Errorf("create single job failed. error: %s", err.Error())
		ctx.ErrorCode = common.RequiredFieldEmpty
		return err
	}
	if err := validateCommonJobInfo(ctx, &request.CommonJobInfo); err != nil {
		log.Errorf("validateCommonJobInfo failed, err: %v", err)
		return err
	}

	if err := validateJobSpec(ctx, &request.JobSpec); err != nil {
		log.Errorf("validateCommonJobInfo failed, err: %v", err)
		return err
	}

	if err := validateSingleJobResource(request.Flavour, request.SchedulingPolicy); err != nil {
		log.Errorf("validateSingleJobResource failed, err: %v", err)
		return err
	}

	return nil
}

func validateSingleJobResource(flavour schema.Flavour, schedulingPolicy SchedulingPolicy) error {
	return IsEnoughQueueCapacity(flavour, schedulingPolicy.MaxResources)
}

func validateJobSpec(ctx *logger.RequestContext, requestJobSpec *JobSpec) error {
	port := requestJobSpec.Port
	if port != 0 && !(port > 0 && port < common.JobPortMaximums) {
		err := fmt.Errorf("port must be in range [0, %d], but got %d", common.JobPortMaximums, port)
		ctx.Logging().Errorf("validate job failed, err: %v", err)
		return err
	}
	// validate FileSystem
	if err := validateFileSystem(requestJobSpec, ctx.UserName); err != nil {
		ctx.Logging().Errorf("validateFileSystem failed, requestJobSpec[%v], err: %v", requestJobSpec, err)
		return err
	}
	var err error
	requestJobSpec.Flavour, err = flavour.GetFlavourWithCheck(requestJobSpec.Flavour)
	if err != nil {
		log.Errorf("get flavour failed, err:%v", err)
		return err
	}

	return nil
}

func validateCommonJobInfo(ctx *logger.RequestContext, requestCommonJobInfo *CommonJobInfo) error {
	// validate job id
	if requestCommonJobInfo.ID != "" {
		// check namespace format
		if errStr := common.IsDNS1123Label(requestCommonJobInfo.ID); len(errStr) != 0 {
			err := fmt.Errorf("ID[%s] of Job is invalid, err: %s", requestCommonJobInfo.ID, strings.Join(errStr, ","))
			ctx.Logging().Errorf("validate Job id failed, err: %v", err)
			return err
		}
	}
	if requestCommonJobInfo.Name != "" && len(requestCommonJobInfo.Name) > common.JobNameMaxLength {
		err := fmt.Errorf("length of jobName[%s] must be no more than %d characters",
			requestCommonJobInfo.Name, common.JobNameMaxLength)
		ctx.Logging().Errorf("validate Job name failed, err: %v", err)
		return err
	}
	if err := validateQueue(ctx, &requestCommonJobInfo.SchedulingPolicy); err != nil {
		ctx.Logging().Errorf("validate queue failed. error: %s", err.Error())
		return err
	}
	// SchedulingPolicy
	if err := checkPriority(&requestCommonJobInfo.SchedulingPolicy, nil); err != nil {
		ctx.Logging().Errorf("Failed to check priority: %v", err)
		ctx.ErrorCode = common.JobInvalidField
		return err
	}

	return nil
}

// validateQueue validate queue and set queueID in request.SchedulingPolicy
func validateQueue(ctx *logger.RequestContext, schedulingPolicy *SchedulingPolicy) error {
	if schedulingPolicy.Queue == "" {
		schedulingPolicy.Queue = config.DefaultQueueName
	}
	queueName := schedulingPolicy.Queue
	queue, err := models.GetQueueByName(queueName)
	if err != nil {
		if errors.GetErrorCode(err) == errors.ErrorKeyIsDuplicated {
			ctx.ErrorCode = common.QueueNameDuplicated
		} else {
			ctx.ErrorCode = common.InternalError
		}
		ctx.ErrorCode = common.InternalError
		err = fmt.Errorf("get queue failed when creating job, err=%v", err)
		log.Error(err)
		return err
	}
	schedulingPolicy.QueueID = queue.ID
	schedulingPolicy.MaxResources = queue.MaxResources
	schedulingPolicy.ClusterId = queue.ClusterId
	schedulingPolicy.Namespace = queue.Namespace
	return nil
}

// checkPriority check priority and fill parent's priority if schedulingPolicy.Priority is empty
func checkPriority(schedulingPolicy, parentSP *SchedulingPolicy) error {
	priority := strings.ToUpper(schedulingPolicy.Priority)
	// check job priority
	if priority == "" {
		if parentSP != nil {
			priority = strings.ToUpper(parentSP.Priority)
		} else {
			priority = schema.EnvJobNormalPriority
		}
	}
	if priority != schema.EnvJobLowPriority &&
		priority != schema.EnvJobNormalPriority && priority != schema.EnvJobHighPriority {
		return errors.InvalidJobPriorityError(priority)
	}
	schedulingPolicy.Priority = priority
	return nil
}

func validateEmptyFieldInSingle(request *CreateSingleJobRequest) []string {
	var emptyFields []string
	if request.Image == "" {
		emptyFields = append(emptyFields, "image")
	}

	return emptyFields
}

func validateMembers(ctx *logger.RequestContext, members []MemberSpec, schePolicy SchedulingPolicy) error {
	if len(members) == 0 {
		err := fmt.Errorf("request.Members is empty")
		ctx.Logging().Errorf("create distributed job failed. error: %s", err.Error())
		ctx.ErrorCode = common.RequiredFieldEmpty
		return err
	}
	// calculate total member resource, and compare with queue.MaxResource
	sumResource := resources.EmptyResource()
	for index, member := range members {
		// validate queue
		var err error
		if members[index], err = validateMembersQueue(ctx, member, schePolicy); err != nil {
			ctx.Logging().Errorf("Failed to check Members' Queue: %v", err)
			ctx.ErrorCode = common.JobInvalidField
			return err
		}
		// check members priority
		if err = checkPriority(&members[index].SchedulingPolicy, &schePolicy); err != nil {
			ctx.Logging().Errorf("Failed to check priority: %v", err)
			ctx.ErrorCode = common.JobInvalidField
			return err
		}
		// sum = sum + member.Replicas * member.Flavour.ResourceInfo
		memberRes, err := resources.NewResourceFromMap(member.Flavour.ResourceInfo.ToMap())
		if err != nil {
			ctx.Logging().Errorf("Failed to multiply replicas=%d and resourceInfo=%v, err: %v", member.Replicas, member.Flavour.ResourceInfo, err)
			ctx.ErrorCode = common.JobInvalidField
			return err
		}
		memberRes.Multi(member.Replicas)
		sumResource.Add(memberRes)
	}
	// validate queue and total-member-resource
	if !sumResource.LessEqual(schePolicy.MaxResources) {
		errMsg := fmt.Sprintf("the flavour[%+v] is larger than queue's [%+v]", sumResource, schePolicy.MaxResources)
		log.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	return nil
}

func validateMembersQueue(ctx *logger.RequestContext, member MemberSpec, schePolicy SchedulingPolicy) (MemberSpec, error) {
	queueName := schePolicy.Queue

	mQueueName := member.SchedulingPolicy.Queue
	if mQueueName != "" && mQueueName != queueName {
		err := fmt.Errorf("schedulingPolicy.Queue should be the same, there are %s and %s", queueName, mQueueName)
		ctx.Logging().Errorf("create distributed job failed. error: %s", err.Error())
		ctx.ErrorCode = common.JobInvalidField
		return member, err
	}
	member.SchedulingPolicy.QueueID = schePolicy.QueueID
	member.SchedulingPolicy.Namespace = schePolicy.Namespace
	member.SchedulingPolicy.ClusterId = schePolicy.ClusterId
	member.SchedulingPolicy.MaxResources = schePolicy.MaxResources
	return member, nil
}

func validateFileSystem(jobSpec *JobSpec, userName string) error {
	fsService := fs.GetFileSystemService()
	fsName := jobSpec.FileSystem.Name
	if fsName != "" {
		jobSpec.FileSystem.MountPath = filepath.Clean(jobSpec.FileSystem.MountPath)
		if jobSpec.FileSystem.MountPath == "/" {
			err := fmt.Errorf("mountPath cannot be `/` in fileSystem[%s]", fsName)
			log.Errorf("validateFileSystem failed, err: %v", err)
			return err
		}
		fileSystem, err := fsService.GetFileSystem(userName, fsName)
		if err != nil {
			log.Errorf("get filesystem by userName[%s] fsName[%s] failed, err: %v", userName, fsName, err)
			return fmt.Errorf("find file system %s failed, err: %v", fsName, err)
		}
		jobSpec.FileSystem.ID = fileSystem.ID
	}
	// todo(zhongzichao) check all fs whether has 'contains' relationship
	for index, fs := range jobSpec.ExtraFileSystems {
		if fs.Name == "" {
			log.Errorf("name of fileSystem %v is null", fs)
			return fmt.Errorf("name of fileSystem %v is null", fs)
		}
		jobSpec.ExtraFileSystems[index].MountPath = filepath.Clean(fs.MountPath)
		if jobSpec.ExtraFileSystems[index].MountPath == "/" {
			err := fmt.Errorf("mountPath cannot be `/` in fileSystem[%s]", fs.Name)
			log.Errorf("validateFileSystem failed, err: %v", err)
			return err
		}

		fileSystem, err := fsService.GetFileSystem(userName, fs.Name)
		if err != nil {
			log.Errorf("get filesystem by userName[%s] fsName[%s] failed, err: %v", userName, fs.Name, err)
			return fmt.Errorf("find file system %s failed, err: %v", fs.Name, err)
		}
		jobSpec.ExtraFileSystems[index].ID = fileSystem.ID
	}

	return nil
}
