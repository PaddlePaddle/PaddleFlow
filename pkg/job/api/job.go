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

package api

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type PFJobInterface interface {
	CreateJob() (string, error)
	StopJobByID(string) error
	UpdateJob([]byte) error
	GetID() string
}

// PFJob will have all info of a Job
type PFJob struct {
	ID        string
	Name      string
	Namespace string
	UserName  string
	// JobType of job, such as TypeSingle, TypeDistributed, and TypeWorkflow
	JobType   schema.JobType
	Framework schema.Framework
	// TODO: use Framework and Tasks.Role instead of JobMode
	JobMode string
	Status  string
	// compute resource request resource for job
	// ClusterID and QueueID of job
	ClusterID    ClusterID
	QueueID      QueueID
	QueueName    string
	Resource     *resources.Resource
	Priority     int32
	MinAvailable int32
	// PriorityClassName defines job info on cluster
	PriorityClassName string

	// Tasks for TypeDistributed job
	Tasks []schema.Member
	// ExtRuntimeConf define extra runtime conf
	ExtRuntimeConf []byte
	// ExtensionTemplate records the extension template of job
	ExtensionTemplate []byte
	IsCustomYaml      bool
	// Conf for job
	Conf schema.Conf

	// Labels for job to update
	Labels      map[string]string
	Annotations map[string]string

	// extend field
	Tags   []string
	LogUrl string

	WaitingTime *time.Duration
	CreateTime  time.Time
	StartTime   time.Time
	EndTIme     time.Time
}

func NewJobInfo(job *model.Job) (*PFJob, error) {
	if job == nil {
		return nil, fmt.Errorf("job is nil")
	}
	log.Debugf("starting NewJobInfo: %#v", job)
	pfjob := &PFJob{
		ID:                job.ID,
		Name:              job.Name,
		Namespace:         job.Config.GetNamespace(),
		JobType:           schema.JobType(job.Type),
		JobMode:           job.Config.GetJobMode(),
		Framework:         job.Framework,
		ClusterID:         ClusterID(job.Config.GetClusterID()),
		QueueID:           QueueID(job.QueueID),
		QueueName:         job.Config.GetQueueName(),
		UserName:          job.UserName,
		Conf:              *job.Config,
		Labels:            make(map[string]string),
		Annotations:       make(map[string]string),
		Resource:          job.Resource,
		Tasks:             job.Members,
		ExtensionTemplate: []byte(job.ExtensionTemplate),
	}
	log.Debugf("gererated pfjob is: %#v", pfjob)
	return pfjob, nil
}

func (pfj *PFJob) NamespacedName() string {
	return fmt.Sprintf("%s/%s", pfj.Namespace, pfj.ID)
}

func (pfj *PFJob) UpdateLabels(labels map[string]string) {
	if labels == nil {
		return
	}
	pfj.Labels = labels
}

func (pfj *PFJob) UpdateAnnotations(annotations map[string]string) {
	if annotations == nil {
		return
	}
	pfj.Annotations = annotations
}

func (pfj *PFJob) UpdateJobPriority(priorityClassName string) {
	pfj.PriorityClassName = priorityClassName
}

func (pfj *PFJob) GetID() string {
	return pfj.ID
}

type JobSyncInfo struct {
	ID               string
	Namespace        string
	ParentJobID      string
	FrameworkVersion schema.FrameworkVersion
	Status           schema.JobStatus
	RuntimeInfo      interface{}
	RuntimeStatus    interface{}
	Annotations      map[string]string
	Message          string
	Action           schema.ActionType
	RetryTimes       int
}

func (js *JobSyncInfo) String() string {
	return fmt.Sprintf("job id: %s, parentJobID: %s, framework: %s, status: %s, message: %s",
		js.ID, js.ParentJobID, js.FrameworkVersion, js.Status, js.Message)
}

type TaskSyncInfo struct {
	ID         string
	Name       string
	Namespace  string
	JobID      string
	NodeName   string
	MemberRole schema.MemberRole
	Status     schema.TaskStatus
	Message    string
	PodStatus  interface{}
	Action     schema.ActionType
	RetryTimes int
}

// FinishedJobInfo contains gc job info
type FinishedJobInfo struct {
	Namespace        string
	Name             string
	Duration         time.Duration
	FrameworkVersion schema.FrameworkVersion
}

type StatusInfo struct {
	OriginStatus string
	Status       schema.JobStatus
	Message      string
}

type GetStatusFunc func(interface{}) (StatusInfo, error)
