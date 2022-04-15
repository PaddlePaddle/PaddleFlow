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
	"time"

	"github.com/ghodss/yaml"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/errors"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/common/uuid"
	"paddleflow/pkg/job"
)

var (
	WSManager = WebsocketManager{
		Connections:   make(map[string]*Connection),
		BroadcastChan: make(chan GetJobResponse, 1000),
	}
	UpdateTime = time.Now()
)

type JobSpec struct {
	ExtraFileSystem   []schema.FileSystem `json:"extraFileSystem,omitempty"`
	Image             string              `json:"image"`
	Env               map[string]string   `json:"env,omitempty"`
	Command           string              `json:"command,omitempty"`
	Args              []string            `json:"args,omitempty"`
	Port              int                 `json:"port,omitempty"`
	schema.Flavour    `json:"flavour,omitempty"`
	schema.FileSystem `json:"fileSystem,omitempty"`
}

type DistributedJobSpec struct {
	Framework schema.Framework `json:"framework,omitempty"`
	Members   []models.Member  `json:"members,omitempty"`
}

type CommonSpec struct {
	ID                string            `json:"id,omitempty"`
	Name              string            `json:"name,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	ExtensionTemplate string            `json:"extensionTemplate,omitempty"`
	SchedulingPolicy  `json:"schedulingPolicy"`
}

type ListJobRequest struct {
	Status  string `json:"status"`
	Marker  string `json:"marker"`
	MaxKeys int    `json:"maxKeys"`
}

type ListJobResponse struct {
	common.MarkerInfo
	JobList []*GetJobResponse `json:"jobList"`
}

type GetJobResponse struct {
	CommonSpec         `json:",inline"`
	JobSpec            `json:",inline"`
	DistributedJobSpec `json:",inline"`
	Status             string                 `json:"status"`
	AcceptTime         string                 `json:"acceptTime"`
	StartTime          string                 `json:"startTime"`
	FinishTime         string                 `json:"finishTime"`
	User               string                 `json:"userName"`
	Runtime            RuntimeInfo            `json:"runtime,omitempty"`
	DistributedRuntime DistributedRuntimeInfo `json:"distributedRuntime,omitempty"`
	WorkflowRuntime    WorkflowRuntimeInfo    `json:"workflowRuntime,omitempty"`
	UpdateTime         time.Time              `json:"-"`
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
	SchedulingPolicy SchedulingPolicy  `json:"schedulingPolicy"`
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
		if err == gorm.ErrRecordNotFound {
			return fmt.Errorf("queue not found by id %s", queueID)
		}
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




func ListJob(ctx *logger.RequestContext, request ListJobRequest) (*ListJobResponse, error) {
	// TODO
	return nil, nil
}

func GetJob(ctx *logger.RequestContext, jobID string) (*GetJobResponse, error) {
	job, err := models.GetJobByID(jobID)
	if err != nil {
		ctx.ErrorCode = common.JobNotFound
		ctx.Logging().Errorln(err.Error())
		return nil, common.NotFoundError(common.ResourceTypeJob, jobID)
	}
	if !common.IsRootUser(ctx.UserName) && ctx.UserName != job.UserName {
		err := common.NoAccessError(ctx.UserName, common.ResourceTypeJob, jobID)
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorln(err.Error())
		return nil, err
	}
	response, err := convertJobToResponse(job)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func DeleteJob(ctx *logger.RequestContext, jobID string) error {
	// TODO
	return nil
}

func convertJobToResponse(job models.Job) (GetJobResponse, error) {
	response := GetJobResponse{}
	b, err := json.Marshal(job)
	if err != nil {
		return response, err
	}
	err = json.Unmarshal(b, &response)
	if err != nil {
		return response, err
	}

	response.AcceptTime = job.CreatedAt.Format(timeLayoutStr)
	if job.ActivatedAt.Valid {
		response.StartTime = job.ActivatedAt.Time.Format(timeLayoutStr)
	}
	if schema.IsImmutableJobStatus(job.Status) {
		response.FinishTime = job.UpdatedAt.Format(timeLayoutStr)
	}
	response.ID = job.ID
	response.Name = job.Name
	response.SchedulingPolicy = SchedulingPolicy{
		QueueID:    job.QueueID,
		Priority: job.Config.Priority,
	}
	// process runtime info && member
	switch job.Type {
	case string(schema.TypeSingle):
		if job.RuntimeInfo != nil {
			statusByte, err := json.Marshal(job.RuntimeInfo.(v1.Pod).Status)
			if err != nil {
				log.Errorf("parse single job[%s] status failed, error:[%s]", job.ID, err.Error())
				return response, err
			}
			response.Runtime = RuntimeInfo{
				ID:        string(job.RuntimeInfo.(v1.Pod).UID),
				Name:      job.RuntimeInfo.(v1.Pod).Name,
				Namespace: job.RuntimeInfo.(v1.Pod).Namespace,
				Status:    string(statusByte),
			}
		}
		var jobSpec JobSpec
		if err := json.Unmarshal([]byte(job.ConfigJson), &jobSpec); err != nil {
			log.Errorf("parse job[%s] config failed, error:[%s]", job.ID, err.Error())
			return response, err
		}
		response.JobSpec = jobSpec
	case string(schema.TypeDistributed):
		if job.RuntimeInfo != nil {
			k8sMeta, err := parseK8sMeta(job.RuntimeInfo)
			if err != nil {
				log.Errorf("parse distributed job[%s] runtimeinfo job meta failed, error:[%s]", job.ID, err.Error())
				return response, err
			}
			statusByte, err := json.Marshal(job.RuntimeInfo.(map[string]interface{})["status"])
			if err != nil {
				log.Errorf("parse distributed job[%s] status failed, error:[%s]", job.ID, err.Error())
				return response, err
			}
			runtimes, err := getTaskRuntime(job.ID)
			if err != nil {
				return response, err
			}
			response.DistributedRuntime = DistributedRuntimeInfo{
				ID:        string(k8sMeta.UID),
				Name:      k8sMeta.Name,
				Namespace: k8sMeta.Namespace,
				Status:    string(statusByte),
				Runtimes:  runtimes,
			}
		}
		members := make([]models.Member, 0)
		if job.Members != nil {
			if err := json.Unmarshal([]byte(job.MembersJson), &members); err != nil {
				log.Errorf("parse job[%s] member failed, error:[%s]", job.ID, err.Error())
				return response, err
			}
		}
		response.DistributedJobSpec = DistributedJobSpec{
			Framework: job.Framework,
			Members:   members,
		}
	case string(schema.TypeWorkflow):
		if job.RuntimeInfo != nil {
			k8sMeta, err := parseK8sMeta(job.RuntimeInfo)
			if err != nil {
				log.Errorf("parse workflow job[%s] runtimeinfo job meta failed, error:[%s]", job.ID, err.Error())
				return response, err
			}
			statusByte, err := json.Marshal(job.RuntimeInfo.(map[string]interface{})["status"])
			if err != nil {
				log.Errorf("parse workflow job[%s] status failed, error:[%s]", job.ID, err.Error())
				return response, err
			}
			nodeRuntimes, err := getNodeRuntime(job.ID)
			if err != nil {
				return response, err
			}
			response.WorkflowRuntime = WorkflowRuntimeInfo{
				Name:      k8sMeta.Name,
				Namespace: k8sMeta.Namespace,
				ID:        k8sMeta.Namespace,
				Status:    string(statusByte),
				Nodes:     nodeRuntimes,
			}
		}
	}
	return response, nil
}

func parseK8sMeta(runtimeInfo interface{}) (metav1.ObjectMeta, error) {
	var k8sMeta metav1.ObjectMeta
	metaData := runtimeInfo.(map[string]interface{})["metadata"]
	metaDataByte, err := json.Marshal(metaData)
	if err != nil {
		log.Errorf(err.Error())
		return k8sMeta, err
	}
	err = json.Unmarshal(metaDataByte, &k8sMeta)
	if err != nil {
		log.Errorf(err.Error())
		return k8sMeta, err
	}
	return k8sMeta, nil
}

func getTaskRuntime(jobID string) ([]RuntimeInfo, error) {
	tasks, err := models.ListByJobID(jobID)
	if err != nil {
		log.Errorf("list job[%s] tasks failed, error:[%s]", jobID, err.Error())
		return nil, err
	}
	runtimes := make([]RuntimeInfo, 0)
	for _, task := range tasks {
		runtime := RuntimeInfo{
			ID:        task.ID,
			Name:      task.Name,
			Namespace: task.Namespace,
			Status:    task.ExtRuntimeStatusJSON,
		}
		runtimes = append(runtimes, runtime)
	}
	return runtimes, nil
}

func getNodeRuntime(jobID string) ([]DistributedRuntimeInfo, error) {
	nodeRuntimes := make([]DistributedRuntimeInfo, 0)
	nodeList, err := models.ListJobByParentID(jobID)
	if err != nil {
		log.Errorf("list job[%s] nodes failed, error:[%s]", jobID, err.Error())
		return nil, err
	}
	for _, node := range nodeList {
		k8sNodeMeta, err := parseK8sMeta(node.RuntimeInfo)
		if err != nil {
			log.Errorf("parse workflow node[%s] runtimeinfo job meta failed, error:[%s]", node.ID, err.Error())
		}
		nodeStatusByte, err := json.Marshal(node.RuntimeInfo.(map[string]interface{})["status"])
		if err != nil {
			log.Errorf("parse workflow node[%s] status failed, error:[%s]", node.ID, err.Error())
			return nil, err
		}
		taskRuntime, err := getTaskRuntime(node.ID)
		if err != nil {
			return nil, err
		}
		nodeRuntime := DistributedRuntimeInfo{
			Name:      k8sNodeMeta.Name,
			Namespace: k8sNodeMeta.Namespace,
			ID:        string(k8sNodeMeta.UID),
			Status:    string(nodeStatusByte),
			Runtimes:  taskRuntime,
		}
		nodeRuntimes = append(nodeRuntimes, nodeRuntime)
	}
	return nodeRuntimes, nil
}
