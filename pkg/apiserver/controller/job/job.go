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

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/errors"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/common/uuid"
	"paddleflow/pkg/job"
)

// CreateSingleJobRequest convey request for create job
type CreateSingleJobRequest struct {
	CommonJobInfo
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

// CreateDisJobRequest convey request for create distributed job
type CreateDisJobRequest struct {
	// todo(zhongzichao)
}

// CreateWfJobRequest convey request for create workflow job
type CreateWfJobRequest struct {
	CommonSpec CommonJobInfo `json:",inline"`
}

// CommonJobInfo the common fields for jobs
type CommonJobInfo struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Labels           map[string]string `json:"labels"`
	Annotations      map[string]string `json:"annotations"`
	SchedulingPolicy SchedulingPolicy  `json:"SchedulingPolicy"`
	UserName         string            `json:",omitempty"`
}

// SchedulingPolicy indicate queueID/priority
type SchedulingPolicy struct {
	QueueID  string `json:"queue"`
	Priority string `json:"priority,omitempty"`
}

// CreateJobResponse convey response for create job
type CreateJobResponse struct {
	ID string `json:"id"`
}

// CreateSingleJob handler for creating job
func CreateSingleJob(request *CreateSingleJobRequest) (*CreateJobResponse, error) {
	extensionTemplate := request.ExtensionTemplate
	if extensionTemplate != "" {
		bytes, err := yaml.JSONToYAML([]byte(request.ExtensionTemplate))
		if err != nil {
			log.Errorf("Failed to parse extension template to yaml: %v", err)
			return nil, err
		}
		extensionTemplate = string(bytes)
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
	id, err := CreateJob(&conf, request.CommonJobInfo.ID, extensionTemplate)
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
	// basic fields required
	conf.Labels = commonJobInfo.Labels
	conf.Annotations = commonJobInfo.Annotations
	conf.SetEnv(schema.EnvJobType, string(schema.TypePodJob))
	conf.SetUserName(commonJobInfo.UserName)
	// info in SchedulingPolicy: queueID,Priority,ClusterId,Namespace
	queueID := commonJobInfo.SchedulingPolicy.QueueID
	queue, err := models.GetQueueByID(&logger.RequestContext{}, queueID)
	if err != nil {
		log.Errorf("Get queue by id failed when creating job %s failed, err=%v", commonJobInfo.Name, err)
		return err
	}
	conf.SetQueueID(queueID)
	conf.SetEnv(schema.EnvJobQueueName, queue.Name)
	if commonJobInfo.SchedulingPolicy.Priority != "" {
		conf.Priority = commonJobInfo.SchedulingPolicy.Priority
	}
	conf.SetClusterID(queue.ClusterId)
	conf.SetNamespace(queue.Namespace)

	return nil
}

func patchSingleEnvs(conf *schema.Conf, request *CreateSingleJobRequest) error {
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
func CreateDistributedJob(request *CreateDisJobRequest) (*CreateJobResponse, error) {
	// todo(zhongzichao)
	return &CreateJobResponse{}, nil
}

// CreateWorkflowJob handler for creating job
func CreateWorkflowJob(request *CreateWfJobRequest) (*CreateJobResponse, error) {
	// todo(zhongzichao)
	return &CreateJobResponse{}, nil
}

// CreateJob handler for creating job, and the job_service.CreateJob will be deprecated
func CreateJob(conf schema.PFJobConf, jobID, jobTemplate string) (string, error) {
	if err := job.ValidateJob(conf); err != nil {
		return "", err
	}
	if err := checkPriority(conf); err != nil {
		return "", err
	}

	jobConf := conf.(*schema.Conf)
	if jobID == "" {
		jobID = uuid.GenerateID(schema.JobPrefix)
	}

	jobInfo := &models.Job{
		ID:                jobID,
		Type:              string(conf.Type()),
		UserName:          conf.GetUserName(),
		QueueID:           conf.GetQueueID(),
		Status:            schema.StatusJobInit,
		Config:            *jobConf,
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
