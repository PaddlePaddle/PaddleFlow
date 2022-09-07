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
	"time"

	"github.com/ghodss/yaml"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/flavour"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/metrics"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

// CreateJobInfo defines
type CreateJobInfo struct {
	CommonJobInfo     `json:",inline"`
	Framework         schema.Framework       `json:"framework"`
	Type              schema.JobType         `json:"type"`
	Mode              string                 `json:"mode,omitempty"`
	Members           []MemberSpec           `json:"members"`
	ExtensionTemplate map[string]interface{} `json:"extensionTemplate,omitempty"`
}

// CreatePFJob handler for creating job
func CreatePFJob(ctx *logger.RequestContext, request *CreateJobInfo) (*CreateJobResponse, error) {

	log.Debugf("Create PF job with request: %#v", request)
	if err := CheckPermission(ctx); err != nil {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return nil, err
	}
	request.UserName = ctx.UserName
	// validate Job
	// gen jobID if not presented in request
	if request.ID == "" {
		request.ID = uuid.GenerateIDWithLength(schema.JobPrefix, uuid.JobIDLength)
	}
	// add time point for job create request
	metrics.Job.AddTimestamp(request.ID, metrics.T1, time.Now())
	if err := validateJob(ctx, request); err != nil {
		ctx.Logging().Errorf("validate job request failed. request:%v error:%s", request, err.Error())
		return nil, err
	}

	// build job from request
	jobInfo, err := buildJob(request)
	if err != nil {
		ctx.Logging().Errorf("patch envs when creating job %s failed, err=%v", request.CommonJobInfo.Name, err)
		return nil, err
	}

	ctx.Logging().Debugf("create distributed job %#v", jobInfo)
	if err = storage.Job.CreateJob(jobInfo); err != nil {
		ctx.Logging().Errorf("create job[%s] in database faield, err: %v", jobInfo.Config.GetName(), err)
		return nil, fmt.Errorf("create job[%s] in database faield, err: %v", jobInfo.Config.GetName(), err)
	}

	ctx.Logging().Infof("create job[%s] successful.", jobInfo.ID)
	return &CreateJobResponse{
		ID: jobInfo.ID,
	}, nil
}

func validateJob(ctx *logger.RequestContext, request *CreateJobInfo) error {
	if err := validateCommonJobInfo(ctx, &request.CommonJobInfo); err != nil {
		ctx.Logging().Errorf("validateCommonJobInfo failed, err: %v", err)
		return err
	}

	// check job type and framework
	if err := validateJobFramework(ctx, request.Type, request.Framework); err != nil {
		ctx.Logging().Errorf("validate job framework failed, err: %v", err)
		return err
	}

	if len(request.ExtensionTemplate) != 0 {
		// extension template from user
		ctx.Logging().Infof("request ExtensionTemplate is not empty, pass validate members")
	} else {
		// validate members
		if err := validateJobMembers(ctx, request); err != nil {
			ctx.Logging().Errorf("validate members failed, err: %v", err)
			return err
		}
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

func validateJobMembers(ctx *logger.RequestContext, request *CreateJobInfo) error {
	if len(request.Members) == 0 {
		err := fmt.Errorf("request.Members is empty")
		ctx.Logging().Errorf("create distributed job failed. error: %s", err.Error())
		ctx.ErrorCode = common.RequiredFieldEmpty
		return err
	}

	frameworkRoles := getFrameworkRoles(request.Framework)
	// calculate total member resource, and compare with queue.MaxResource
	sumResource := resources.EmptyResource()
	for index, member := range request.Members {
		// validate member
		err := validateMember(ctx, &request.Members[index], request.Framework, frameworkRoles, request.SchedulingPolicy)
		if err != nil {
			ctx.Logging().Errorf("Failed to check member: %v", err)
			ctx.ErrorCode = common.JobInvalidField
			return err
		}
		// TODO: use flavour point
		member.Flavour, err = flavour.GetFlavourWithCheck(member.Flavour)
		if err != nil {
			log.Errorf("get flavour failed, err:%v", err)
			return err
		}
		request.Members[index].Flavour.ResourceInfo = member.Flavour.ResourceInfo
		// sum = sum + member.Replicas * member.Flavour.ResourceInfo
		memberRes, err := resources.NewResourceFromMap(member.Flavour.ResourceInfo.ToMap())
		if err != nil {
			ctx.Logging().Errorf("Failed to multiply replicas=%d and resourceInfo=%v, err: %v", member.Replicas, member.Flavour.ResourceInfo, err)
			ctx.ErrorCode = common.JobInvalidField
			return err
		}
		ctx.Logging().Debugf("member resource info %v", member.Flavour.ResourceInfo)
		if memberRes.CPU() == 0 || memberRes.Memory() == 0 {
			err = fmt.Errorf("flavour[%v] cpu or memory is empty", memberRes)
			ctx.Logging().Errorf("Failed to check flavour: %v", err)
			return err
		}
		memberRes.Multi(member.Replicas)
		sumResource.Add(memberRes)
	}
	// validate queue and total-member-resource
	if !sumResource.LessEqual(request.SchedulingPolicy.MaxResources) {
		errMsg := fmt.Sprintf("the flavour[%+v] is larger than queue's [%+v]", sumResource, request.SchedulingPolicy.MaxResources)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	var err error
	request.Mode, err = checkMemberRole(request.Framework, frameworkRoles)
	if err != nil {
		ctx.Logging().Errorf("check member role for framework %s failed, err: %v", request.Framework, err)
		return err
	}
	return nil
}

// validateMember validate member's fields
func validateMember(ctx *logger.RequestContext, member *MemberSpec, framework schema.Framework,
	frameworkRoles map[schema.MemberRole]int, schedulingPolicy SchedulingPolicy) error {
	// validate member role and replicas
	memberRole := schema.MemberRole(member.Role)
	_, find := frameworkRoles[memberRole]
	if !find {
		err := fmt.Errorf("the role[%s] for framework %s is not supported", member.Role, framework)
		ctx.Logging().Errorf("Failed to check Members' role, err: %v", err)
		return err
	}
	if member.Replicas < 1 {
		err := fmt.Errorf("the repilcas of member is less than 1")
		ctx.Logging().Errorf("Failed to check Members' replicas, err: %v", err)
		return err
	}
	frameworkRoles[memberRole] = frameworkRoles[memberRole] + member.Replicas
	// TODO: move more check to checkJobSpec
	err := checkJobSpec(ctx, &member.JobSpec)
	if err != nil {
		ctx.Logging().Errorf("Failed to check Members: %v", err)
		return err
	}
	// validate queue
	if err = validateMembersQueue(ctx, member, schedulingPolicy); err != nil {
		ctx.Logging().Errorf("Failed to check Members' Queue: %v", err)
		return err
	}
	// check members priority
	if err = checkPriority(&member.SchedulingPolicy, &schedulingPolicy); err != nil {
		ctx.Logging().Errorf("Failed to check priority: %v", err)
		return err
	}
	return nil
}

func checkJobSpec(ctx *logger.RequestContext, jobSpec *JobSpec) error {
	port := jobSpec.Port
	if port != 0 && !(port > 0 && port < common.JobPortMaximums) {
		err := fmt.Errorf("port must be in range [0, %d], but got %d", common.JobPortMaximums, port)
		ctx.Logging().Errorf("validate job failed, err: %v", err)
		return err
	}
	// ensure required fields
	emptyFields := checkEmptyField(jobSpec)
	if len(emptyFields) != 0 {
		emptyFieldStr := strings.Join(emptyFields, ",")
		err := fmt.Errorf("required fields in {%s} are empty, please fill it", emptyFieldStr)
		ctx.Logging().Errorf("create single job failed. error: %s", err.Error())
		ctx.ErrorCode = common.RequiredFieldEmpty
		return err
	}
	// validate FileSystem
	if err := validateFileSystems(jobSpec, ctx.UserName); err != nil {
		ctx.Logging().Errorf("validateFileSystem failed, requestJobSpec[%v], err: %v", jobSpec, err)
		return err
	}
	return nil
}

// validateQueue validate queue and set queueID in request.SchedulingPolicy
func validateQueue(ctx *logger.RequestContext, schedulingPolicy *SchedulingPolicy) error {
	if schedulingPolicy.Queue == "" {
		if config.GlobalServerConfig.Job.IsSingleCluster {
			schedulingPolicy.Queue = config.DefaultQueueName
		} else {
			err := fmt.Errorf("queue is empty")
			ctx.Logging().Errorf("Failed to check Queue: %v", err)
			return err
		}
	}
	queueName := schedulingPolicy.Queue
	queue, err := storage.Queue.GetQueueByName(queueName)
	if err != nil {
		if errors.GetErrorCode(err) == errors.ErrorKeyIsDuplicated {
			ctx.ErrorCode = common.QueueNameDuplicated
		} else {
			ctx.ErrorCode = common.InternalError
		}
		ctx.ErrorCode = common.InternalError
		err = fmt.Errorf("get queue failed when creating job, err=%v", err)
		ctx.Logging().Error(err)
		return err
	}
	if queue.Status != schema.StatusQueueOpen {
		errMsg := fmt.Sprintf("queue[%s] status is %s, and only queue with open status can submit jobs", queueName, queue.Status)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
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

func validateMembersQueue(ctx *logger.RequestContext, member *MemberSpec, schePolicy SchedulingPolicy) error {
	queueName := schePolicy.Queue

	mQueueName := member.SchedulingPolicy.Queue
	if mQueueName != "" && mQueueName != queueName {
		err := fmt.Errorf("schedulingPolicy.Queue should be the same, there are %s and %s", queueName, mQueueName)
		ctx.Logging().Errorf("create distributed job failed. error: %s", err.Error())
		ctx.ErrorCode = common.JobInvalidField
		return err
	}
	member.SchedulingPolicy.QueueID = schePolicy.QueueID
	member.SchedulingPolicy.Namespace = schePolicy.Namespace
	member.SchedulingPolicy.ClusterId = schePolicy.ClusterId
	member.SchedulingPolicy.MaxResources = schePolicy.MaxResources
	return nil
}

func validateFileSystems(jobSpec *JobSpec, userName string) error {
	if jobSpec.FileSystem.Name != "" {
		if err := validateFileSystem(userName, &jobSpec.FileSystem); err != nil {
			err = fmt.Errorf("validateFileSystem failed, err: %v", err)
			log.Error(err)
			return err
		}
	}

	for index, _ := range jobSpec.ExtraFileSystems {
		if err := validateFileSystem(userName, &jobSpec.ExtraFileSystems[index]); err != nil {
			err = fmt.Errorf("validate extraFileSystems failed, err: %v", err)
			log.Error(err)
			return err
		}
	}
	return nil
}

func validateFileSystem(userName string, fs *schema.FileSystem) error {
	fsName := fs.Name
	fsID := fs.ID
	if fsID == "" {
		// generate fsID by fsName if fsID is nil
		fsID = common.ID(userName, fsName)
	}
	if fs.MountPath == "" {
		log.Debugf("mountPath is %s, changes to .", fs.MountPath)
		fs.MountPath = filepath.Join(schema.DefaultFSMountPath, fs.ID)
	}
	mountPath := utils.MountPathClean(fs.MountPath)
	if mountPath == "/" || mountPath == "." || mountPath == ".." {
		err := fmt.Errorf("mountPath cannot be '/' in fsName: %s fsID: %s, got %s", fsName, fsID, fs.MountPath)
		log.Errorf("validateFileSystem failed, err: %v", err)
		return err
	}

	fileSystem, err := storage.Filesystem.GetFileSystemWithFsID(fsID)
	if err != nil {
		log.Errorf("get filesystem by userName[%s] fsName[%s] fsID[%s] failed, err: %v", userName, fsName, fsID, err)
		return fmt.Errorf("find file system %s failed, err: %v", fsName, err)
	}
	// fill back
	fs.ID = fileSystem.ID
	fs.Name = fileSystem.Name

	return nil
}

func checkEmptyField(request *JobSpec) []string {
	var emptyFields []string
	if request.Image == "" {
		emptyFields = append(emptyFields, "image")
	}
	return emptyFields
}

// validateJobFramework validate job type and framework
func validateJobFramework(ctx *logger.RequestContext, jobType schema.JobType, framework schema.Framework) error {
	var err error
	switch jobType {
	case schema.TypeSingle:
		if framework != schema.FrameworkStandalone {
			err = fmt.Errorf("framework for single job must be standalone")
		}
	case schema.TypeDistributed:
		switch framework {
		case schema.FrameworkSpark, schema.FrameworkPaddle, schema.FrameworkTF,
			schema.FrameworkPytorch, schema.FrameworkMXNet, schema.FrameworkRay:
			err = nil
		case schema.FrameworkMPI:
			err = fmt.Errorf("framework: %s for distributed job will be supported in the future", framework)
		default:
			err = fmt.Errorf("invalid framework %s for distributed job", framework)
		}
	case schema.TypeWorkflow:
		// TODO: add check for workflow
	default:
		err = fmt.Errorf("job type %s does not supported", jobType)
	}
	if err != nil {
		ctx.Logging().Error(err)
		ctx.ErrorCode = common.JobInvalidField
	}
	return err
}

func checkMemberRole(framework schema.Framework, roles map[schema.MemberRole]int) (string, error) {
	var err error
	var jobMode string
	switch framework {
	case schema.FrameworkPaddle, schema.FrameworkTF, schema.FrameworkPytorch, schema.FrameworkMXNet:
		if roles[schema.RolePServer] > 0 {
			// parameter server mode
			jobMode = schema.EnvJobModePS
			if roles[schema.RolePWorker] < 1 || roles[schema.RoleWorker] > 0 {
				err = fmt.Errorf("framework %s in parameter server mode, role pwork must be set", framework)
			}
		} else {
			// collective mode
			jobMode = schema.EnvJobModeCollective
			if roles[schema.RoleWorker] < 2 || roles[schema.RolePWorker] > 0 {
				err = fmt.Errorf("framework %s in collective mode, only setting role work", framework)
			}
		}
	case schema.FrameworkSpark:
		jobMode = schema.EnvJobModePS
		if roles[schema.RoleDriver] < 1 {
			err = fmt.Errorf("spark application must be set role driver")
		}
	case schema.FrameworkMPI:
		if roles[schema.RoleMaster] < 1 {
			err = fmt.Errorf("mpi job must be set role master")
		}
	case schema.FrameworkRay:
		if roles[schema.RoleMaster] < 1 || roles[schema.RoleWorker] < 1 {
			err = fmt.Errorf("ray job must be set a master role and a worker role")
		}
	case schema.FrameworkStandalone:
		if roles[schema.RoleWorker] != 1 {
			err = fmt.Errorf("replicas for single job must be 1")
		}
	}
	return jobMode, err
}

func getFrameworkRoles(framework schema.Framework) map[schema.MemberRole]int {
	var roles = make(map[schema.MemberRole]int)
	switch framework {
	case schema.FrameworkPaddle, schema.FrameworkTF, schema.FrameworkPytorch, schema.FrameworkMXNet:
		roles[schema.RolePServer] = 0
		roles[schema.RolePWorker] = 0
		roles[schema.RoleWorker] = 0
	case schema.FrameworkSpark:
		roles[schema.RoleDriver] = 0
		roles[schema.RoleExecutor] = 0
	case schema.FrameworkMPI, schema.FrameworkRay:
		roles[schema.RoleMaster] = 0
		roles[schema.RoleWorker] = 0
	case schema.FrameworkStandalone:
		roles[schema.RoleWorker] = 0
	}
	return roles
}

// buildJob build a models job
func buildJob(request *CreateJobInfo) (*model.Job, error) {
	log.Debugf("begin build job with info: %#v", request)
	// build main job config
	conf := buildMainConf(request)
	// convert job members if necessary
	var members []schema.Member
	var templateJson string
	var err error
	if len(request.ExtensionTemplate) == 0 {
		members = buildMembers(request)
	} else {
		templateJson, err = newExtensionTemplateJson(request.ExtensionTemplate)
		if err != nil {
			log.Errorf("parse extension template failed, err: %v", err)
			return nil, err
		}
	}

	jobInfo := &model.Job{
		ID:                request.ID,
		Name:              request.Name,
		UserName:          request.UserName,
		QueueID:           request.SchedulingPolicy.QueueID,
		Type:              string(request.Type),
		Status:            schema.StatusJobInit,
		Config:            conf,
		Members:           members,
		Framework:         request.Framework,
		ExtensionTemplate: templateJson,
	}
	return jobInfo, nil
}

func buildMainConf(request *CreateJobInfo) *schema.Conf {
	var conf = &schema.Conf{
		Name: request.Name,
	}
	if request.Type == schema.TypeSingle && len(request.Members) == 1 {
		// build conf for single job
		conf = &schema.Conf{
			Name:            request.Name,
			FileSystem:      request.Members[0].FileSystem,
			ExtraFileSystem: request.Members[0].ExtraFileSystems,
			Flavour:         request.Members[0].Flavour,
			Env:             request.Members[0].Env,
			Image:           request.Members[0].Image,
			Command:         request.Members[0].Command,
			Port:            request.Members[0].Port,
			Args:            request.Members[0].Args,
		}
	}
	// fields in request.CommonJobInfo
	buildCommonInfo(conf, &request.CommonJobInfo)
	// set scheduling priority
	if request.SchedulingPolicy.Priority != "" {
		conf.Priority = request.SchedulingPolicy.Priority
	}
	// TODO: remove job mode
	conf.SetEnv(schema.EnvJobMode, request.Mode)
	return conf
}

func buildMembers(request *CreateJobInfo) []schema.Member {
	members := make([]schema.Member, 0)
	log.Infof("build merbers for framework %s with mode %s", request.Framework, request.Mode)
	for _, reqMember := range request.Members {
		member := newMember(reqMember, schema.MemberRole(reqMember.Role))
		buildCommonInfo(&member.Conf, &request.CommonJobInfo)
		members = append(members, member)
	}
	return members
}

func buildCommonInfo(conf *schema.Conf, commonJobInfo *CommonJobInfo) {
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
}

// newMember convert request.Member to models.member
func newMember(member MemberSpec, role schema.MemberRole) schema.Member {
	conf := schema.Conf{
		Name: member.Name,
		// 存储资源
		FileSystem:      member.FileSystem,
		ExtraFileSystem: member.ExtraFileSystems,
		// 计算资源
		Flavour:  member.Flavour,
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

	return schema.Member{
		ID:       member.ID,
		Role:     role,
		Replicas: member.Replicas,
		Conf:     conf,
	}
}

// newExtensionTemplateJson parse extensionTemplate
func newExtensionTemplateJson(extensionTemplate map[string]interface{}) (string, error) {
	yamlExtensionTemplate := ""
	if len(extensionTemplate) > 0 {
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
	if err := validateQueue(ctx, &request.SchedulingPolicy); err != nil {
		msg := fmt.Sprintf("valiate queue for workflow job failed, err: %v", err)
		log.Errorf(msg)
		return nil, fmt.Errorf(msg)
	}
	conf.SetQueueID(request.SchedulingPolicy.QueueID)
	conf.SetNamespace(request.SchedulingPolicy.Namespace)
	conf.SetClusterID(request.SchedulingPolicy.ClusterId)
	conf.SetQueueName(request.SchedulingPolicy.Queue)

	// create workflow job
	jobInfo := &model.Job{
		ID:                request.ID,
		Name:              request.Name,
		Type:              string(schema.TypeWorkflow),
		UserName:          conf.GetUserName(),
		QueueID:           conf.GetQueueID(),
		Status:            schema.StatusJobInit,
		Config:            &conf,
		ExtensionTemplate: templateJson,
	}

	if err := storage.Job.CreateJob(jobInfo); err != nil {
		log.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
		return nil, fmt.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
	}
	log.Infof("create job[%s] successful.", jobInfo.ID)
	return &CreateJobResponse{ID: jobInfo.ID}, nil
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

// CreatePPLJob create a run job, used by pipeline
func CreatePPLJob(conf schema.PFJobConf) (string, error) {
	createJobInfo, err := jobConfToCreateJobInfo(conf)
	if err != nil {
		log.Errorf("convert job config to CreateJobInfo failed. err: %s", err)
		return "", err
	}
	ctx := &logger.RequestContext{
		UserName: createJobInfo.UserName,
	}
	jobResponse, err := CreatePFJob(ctx, createJobInfo)
	if err != nil {
		log.Errorf("create pipeline job failed. err: %s", err)
		return "", err
	}
	return jobResponse.ID, nil
}

func ValidatePPLJob(conf schema.PFJobConf) error {
	createJobInfo, err := jobConfToCreateJobInfo(conf)
	if err != nil {
		log.Errorf("convert job config to CreateJobInfo failed. err: %s", err)
		return err
	}
	// pipeline job check
	if len(createJobInfo.Name) == 0 {
		return errors.EmptyJobNameError()
	}
	if len(createJobInfo.UserName) == 0 {
		return errors.EmptyUserNameError()
	}
	ctx := &logger.RequestContext{
		UserName: createJobInfo.UserName,
	}
	return validateJob(ctx, createJobInfo)
}

func jobConfToCreateJobInfo(conf schema.PFJobConf) (*CreateJobInfo, error) {
	commonJobInfo := CommonJobInfo{
		ID:   generateJobID(conf.GetName()),
		Name: conf.GetName(),
		SchedulingPolicy: SchedulingPolicy{
			Queue:    conf.GetQueueName(),
			Priority: conf.GetPriority(),
		},
		UserName: conf.GetUserName(),
	}
	jobSpec := JobSpec{
		Flavour: schema.Flavour{
			Name: conf.GetFlavour(),
		},
		FileSystem:       conf.GetFileSystem(),
		ExtraFileSystems: conf.GetExtraFS(),
		Image:            conf.GetImage(),
		Env:              conf.GetEnv(),
		Command:          conf.GetCommand(),
		Args:             conf.GetArgs(),
	}

	jobType := conf.Type()
	var err error
	var framework schema.Framework
	switch jobType {
	case "", schema.TypeSingle, schema.TypeVcJob:
		jobType = schema.TypeSingle
		framework = schema.FrameworkStandalone
	case schema.TypeDistributed:
		err = fmt.Errorf("distributed job is not implemented")
	default:
		err = fmt.Errorf("job type %s is not support", jobType)
	}
	if err != nil {
		log.Errorf("check pipeline job type failed, err: %v", err)
		return nil, err
	}

	return &CreateJobInfo{
		CommonJobInfo: commonJobInfo,
		Type:          jobType,
		Framework:     framework,
		Members: []MemberSpec{
			{
				CommonJobInfo: commonJobInfo,
				JobSpec:       jobSpec,
				Role:          string(schema.RoleWorker),
				Replicas:      1,
			},
		},
	}, nil
}

func generateJobID(param string) string {
	return uuid.GenerateID(fmt.Sprintf("%s-%s", schema.JobPrefix, param))
}
