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

	"github.com/ghodss/yaml"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/controller/flavour"
	"paddleflow/pkg/apiserver/models"
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

// CreateSingleJob handler for creating job
func CreateSingleJob(ctx *logger.RequestContext, request *CreateSingleJobRequest) (*CreateJobResponse, error) {
	if err := CheckPermission(ctx); err != nil {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return nil, err
	}

	f, err := flavour.GetFlavourWithCheck(request.Flavour)
	if err != nil {
		log.Errorf("get flavour failed, err:%v", err)
		return nil, err
	}
	extensionTemplate, err := newExtensionTemplate(request.ExtensionTemplate)
	if err != nil {
		log.Errorf("parse extension template failed, err=%v", err)
		return nil, err
	}
	// validate FileSystem
	if err := validateFileSystem(request.FileSystem, request.UserName); err != nil {
		return nil, err
	}
	// parse job template

	// new job conf and set values
	conf := schema.Conf{
		Name: request.Name,
		// 存储资源
		FileSystem:      request.FileSystem,
		ExtraFileSystem: request.ExtraFileSystems,
		// 计算资源
		Flavour:  f,
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
		Config:            &conf,
		ExtensionTemplate: extensionTemplate,
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

// newExtensionTemplate parse extensionTemplate
func newExtensionTemplate(extensionTemplate map[string]interface{}) (string, error) {
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
	// FileSystem
	if jobSpec.FileSystem.Name != "" {
		fsID := common.ID(userName, jobSpec.FileSystem.Name)
		// todo to be removed if not used
		conf.SetFS(fsID)
	}
	// todo ExtraFileSystem
	return nil
}

func patchFromCommonInfo(conf *schema.Conf, commonJobInfo *CommonJobInfo) error {
	log.Debugf("patch envs for job %s", commonJobInfo.Name)
	// basic fields required
	conf.Labels = commonJobInfo.Labels
	conf.Annotations = commonJobInfo.Annotations
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
	queueID := commonJobInfo.SchedulingPolicy.QueueID
	conf.SetQueueID(queueID)
	conf.SetQueueName(queueName)
	conf.SetPriority(commonJobInfo.SchedulingPolicy.Priority)

	conf.SetClusterID(queue.ClusterId)
	conf.SetNamespace(queue.Namespace)

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
	var err error
	extensionTemplate, err := newExtensionTemplate(request.ExtensionTemplate)
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
		ExtensionTemplate: extensionTemplate,
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
		// validate FileSystem
		if err := validateFileSystem(reqMem.FileSystem, request.UserName); err != nil {
			return nil, err
		}
		if reqMem.Role == string(schema.RoleWorker) {
			member, err := newMember(reqMem, schema.RoleWorker)
			if err != nil {
				log.Errorf("create collective members failed, err=%v", err)
				return nil, err
			}
			members = append(members, member)
		}
	}
	return members, nil
}

func newPSMembers(request *CreateDisJobRequest) ([]models.Member, error) {
	members := make([]models.Member, 0)
	for _, reqMember := range request.Members {
		// validate FileSystem
		if err := validateFileSystem(reqMember.FileSystem, request.UserName); err != nil {
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
	return models.Member{
		ID:       member.ID,
		Role:     role,
		Replicas: member.Replicas,
		Conf: schema.Conf{
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
		},
	}, nil
}

// CreateWorkflowJob handler for creating job
func CreateWorkflowJob(ctx *logger.RequestContext, request *CreateWfJobRequest) (*CreateJobResponse, error) {
	if err := CheckPermission(ctx); err != nil {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return nil, err
	}

	var extensionTemplate string
	if request.ExtensionTemplate == nil {
		return nil, fmt.Errorf("ExtensionTemplate for workflow job is needed")
	}
	var err error
	extensionTemplate, err = newExtensionTemplate(request.ExtensionTemplate)
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
	if err := job.ValidateQueue(&conf, ctx.UserName, request.SchedulingPolicy.Queue); err != nil {
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
		ExtensionTemplate: extensionTemplate,
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
		log.Errorf("get job from database failed, err: %v", err)
		return err
	}
	// check job status
	if !schema.IsImmutableJobStatus(job.Status) {
		ctx.ErrorCode = common.ActionNotAllowed
		msg := fmt.Sprintf("job %s status is %s, please stop it first.", jobID, job.Status)
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
	if err = models.UpdateJobStatus(jobID, "job is terminated.", schema.StatusJobTerminated); err != nil {
		log.Errorf("update job[%s] status to [%s] failed, err: %v", jobID, schema.StatusJobTerminated, err)
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

func validateFileSystem(fs schema.FileSystem, userName string) error {
	if fs.Name == "" {
		return nil
	}
	fsID := common.ID(userName, fs.Name)
	if _, err := models.GetFileSystemWithFsID(fsID); err != nil {
		log.Errorf("get filesystem %s failed, err: %v", fsID, err)
		return fmt.Errorf("find file system %s failed, err: %v", fs.Name, err)
	}
	return nil
}
