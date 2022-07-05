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

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
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
	Resource     *resources.Resource
	Priority     int32
	MinAvailable int32
	// PriorityClassName defines job info on cluster
	PriorityClassName string
	// storage resource for job
	FSID string
	// Tasks for TypeDistributed job
	Tasks []models.Member
	// ExtRuntimeConf define extra runtime conf
	ExtRuntimeConf []byte
	// ExtensionTemplate records the extension template of job
	ExtensionTemplate string
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

func NewJobInfo(job *models.Job) (*PFJob, error) {
	if job == nil {
		return nil, fmt.Errorf("job is nil")
	}
	pfjob := &PFJob{
		ID:                job.ID,
		Name:              job.Name,
		Namespace:         job.Config.GetNamespace(),
		JobType:           schema.JobType(job.Type),
		JobMode:           job.Config.GetJobMode(),
		Framework:         job.Framework,
		ClusterID:         ClusterID(job.Config.GetClusterID()),
		QueueID:           QueueID(job.QueueID),
		FSID:              job.Config.GetFS(),
		UserName:          job.UserName,
		Conf:              *job.Config,
		Labels:            make(map[string]string),
		Annotations:       make(map[string]string),
		Resource:          job.Resource,
		Tasks:             job.Members,
		ExtensionTemplate: job.ExtensionTemplate,
	}

	return pfjob, nil
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
