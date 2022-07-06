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
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/flavour"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
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
	if err := validateJob(ctx, request); err != nil {
		ctx.Logging().Errorf("validate job request failed. request:%v error:%s", request, err.Error())
		return nil, err
	}

	// build job from request
	jobInfo, err := buildJob(request)
	if err != nil {
		log.Errorf("patch envs when creating job %s failed, err=%v", request.CommonJobInfo.Name, err)
		return nil, err
	}

	log.Debugf("create distributed job %#v", jobInfo)
	if err = models.CreateJob(jobInfo); err != nil {
		log.Errorf("create job[%s] in database faield, err: %v", jobInfo.Config.GetName(), err)
		return nil, fmt.Errorf("create job[%s] in database faield, err: %v", jobInfo.Config.GetName(), err)
	}

	log.Infof("create job[%s] successful.", jobInfo.ID)
	return &CreateJobResponse{
		ID: jobInfo.ID,
	}, nil
}

func validateJob(ctx *logger.RequestContext, request *CreateJobInfo) error {
	if err := validateCommonJobInfo(ctx, &request.CommonJobInfo); err != nil {
		ctx.Logging().Errorf("validateCommonJobInfo failed, err: %v", err)
		return err
	}

	// check job framework
	var jobType schema.JobType
	switch request.Framework {
	case schema.FrameworkSpark, schema.FrameworkPaddle:
		jobType = schema.TypeDistributed
	case "", schema.FrameworkStandalone:
		jobType = schema.TypeSingle
	case schema.FrameworkTF, schema.FrameworkMPI:
		ctx.Logging().Errorf("framework: %s will be supported in the future", request.Framework)
		ctx.ErrorCode = common.JobInvalidField
		return fmt.Errorf("framework: %s will be supported in the future", request.Framework)
	default:
		ctx.Logging().Errorf("invalid framework: %s", request.Framework)
		ctx.ErrorCode = common.JobInvalidField
		return fmt.Errorf("invalid framework: %s", request.Framework)
	}
	if request.Type == "" {
		// set job type
		request.Type = jobType
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
		// validate member role and replicas
		memberRole := schema.MemberRole(member.Role)
		_, find := frameworkRoles[memberRole]
		if !find {
			err := fmt.Errorf("the role[%s] for framework %s is not supported", member.Role, request.Framework)
			ctx.ErrorCode = common.JobInvalidField
			ctx.Logging().Errorf("Failed to check Members' role, err: %v", err)
			return err
		}
		if member.Replicas < 1 {
			err := fmt.Errorf("the repilcas of member is less than 1")
			ctx.ErrorCode = common.JobInvalidField
			ctx.Logging().Errorf("Failed to check Members' replicas, err: %v", err)
			return err
		}
		frameworkRoles[memberRole] = frameworkRoles[memberRole] + member.Replicas
		// TODO: move more check to checkJobSpec
		err := checkJobSpec(ctx, &request.Members[index].JobSpec)
		if err != nil {
			ctx.Logging().Errorf("Failed to check Members: %v", err)
			ctx.ErrorCode = common.JobInvalidField
			return err
		}
		// validate queue
		if request.Members[index], err = validateMembersQueue(ctx, member, request.SchedulingPolicy); err != nil {
			ctx.Logging().Errorf("Failed to check Members' Queue: %v", err)
			ctx.ErrorCode = common.JobInvalidField
			return err
		}
		// check members priority
		if err = checkPriority(&request.Members[index].SchedulingPolicy, &request.SchedulingPolicy); err != nil {
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

func checkJobSpec(ctx *logger.RequestContext, jobSpec *JobSpec) error {
	port := jobSpec.Port
	if port != 0 && !(port > 0 && port < common.JobPortMaximums) {
		err := fmt.Errorf("port must be in range [0, %d], but got %d", common.JobPortMaximums, port)
		ctx.Logging().Errorf("validate job failed, err: %v", err)
		return err
	}
	// validate FileSystem
	if err := validateFileSystem(jobSpec, ctx.UserName); err != nil {
		ctx.Logging().Errorf("validateFileSystem failed, requestJobSpec[%v], err: %v", jobSpec, err)
		return err
	}
	var err error
	jobSpec.Flavour, err = flavour.GetFlavourWithCheck(jobSpec.Flavour)
	if err != nil {
		log.Errorf("get flavour failed, err:%v", err)
		return err
	}
	return nil
}

func checkMemberRole(framework schema.Framework, roles map[schema.MemberRole]int) (string, error) {
	var err error
	var jobMode string
	switch framework {
	case schema.FrameworkPaddle, schema.FrameworkTF:
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
	case schema.FrameworkPaddle, schema.FrameworkTF:
		roles[schema.RolePServer] = 0
		roles[schema.RolePWorker] = 0
		roles[schema.RoleWorker] = 0
	case schema.FrameworkSpark:
		roles[schema.RoleDriver] = 0
		roles[schema.RoleExecutor] = 0
	case schema.FrameworkMPI:
		roles[schema.RoleMaster] = 0
		roles[schema.RoleWorker] = 0
	case schema.FrameworkStandalone:
		roles[schema.RoleWorker] = 0
	}
	return roles
}

func buildJob(request *CreateJobInfo) (*models.Job, error) {
	log.Debugf("begin build job with info: %#v", request)
	conf := &schema.Conf{
		Name:        request.Name,
		Labels:      request.Labels,
		Annotations: request.Annotations,
		Priority:    request.SchedulingPolicy.Priority,
	}
	// fields in request.CommonJobInfo
	patchFromCommonInfo(conf, &request.CommonJobInfo)
	// set scheduling priority
	if request.SchedulingPolicy.Priority != "" {
		conf.Priority = request.SchedulingPolicy.Priority
	}
	// TODO: remove job mode
	conf.SetEnv(schema.EnvJobMode, request.Mode)
	// convert job members if necessary
	var members []models.Member
	var templateJson string
	var err error
	if len(request.ExtensionTemplate) == 0 {
		members, err = buildMembers(request)
		if err != nil {
			log.Errorf("create job with ps members failed, err: %v", err)
			return nil, err
		}
	} else {
		templateJson, err = newExtensionTemplateJson(request.ExtensionTemplate)
		if err != nil {
			log.Errorf("parse extension template failed, err: %v", err)
			return nil, err
		}
	}

	jobInfo := &models.Job{
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

func buildMembers(request *CreateJobInfo) ([]models.Member, error) {
	members := make([]models.Member, 0)
	log.Infof("build merbers for framework %s with mode %s", request.Framework, request.Mode)
	for _, reqMember := range request.Members {
		reqMember.UserName = request.UserName
		var member models.Member
		var err error
		member, err = newMember(reqMember, schema.MemberRole(reqMember.Role))
		if err != nil {
			log.Errorf("create ps members failed, err: %v", err)
			return nil, err
		}
		patchFromCommonInfo(&member.Conf, &request.CommonJobInfo)
		members = append(members, member)
	}
	return members, nil
}

// newMember convert request.Member to models.member
func newMember(member MemberSpec, role schema.MemberRole) (models.Member, error) {
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

	return models.Member{
		ID:       member.ID,
		Role:     role,
		Replicas: member.Replicas,
		Conf:     conf,
	}, nil
}
