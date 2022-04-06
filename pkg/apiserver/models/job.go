/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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

package models

import (
	"database/sql"
	"encoding/json"
	"paddleflow/pkg/common/database/dbflag"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"paddleflow/pkg/common/errors"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
)

type Job struct {
	Pk                int64            `json:"-" gorm:"primaryKey;autoIncrement"`
	ID                string           `json:"jobID" gorm:"type:varchar(60);uniqueIndex"`
	Name              string           `json:"jobName" gorm:"type:varchar(512);default:''"`
	UserName          string           `json:"userName" gorm:"type:varchar(255);NOT NULL"`
	QueueID           string           `json:"queueID" gorm:"type:varchar(36);NOT NULL"`
	Type              string           `json:"type" gorm:"type:varchar(20);NOT NULL"`
	Config            schema.Conf      `json:"config" gorm:"type:text"`
	RuntimeInfoJson   string           `json:"-" gorm:"column:runtime_info;default:'{}'"`
	RuntimeInfo       interface{}      `json:"runtimeInfo" gorm:"-"`
	Status            schema.JobStatus `json:"status"`
	Message           string           `json:"message"`
	ResourceJson      string           `json:"-" gorm:"column:resource;type:text;default:'{}'"`
	Resource          *schema.Resource `json:"resource" gorm:"-"`
	Framework         schema.Framework `json:"framework" gorm:"type:varchar(30)"`
	Members           []Member         `json:"members" gorm:"type:text"`
	ExtensionTemplate string           `json:"extensionTemplate" gorm:"type:text"`
	ParentJob         string           `json:"-" gorm:"type:varchar(60)"`
	CreatedAt         time.Time        `json:"createTime"`
	ActivatedAt       sql.NullTime     `json:"activateTime"`
	UpdatedAt         time.Time        `json:"updateTime,omitempty"`
	DeletedAt         gorm.DeletedAt   `json:"-" gorm:"index"`
}

type Member struct {
	ID          string            `json:"id"`
	Replicas    int               `json:"replicas"`
	Role        schema.RoleMember `json:"role"`
	schema.Conf `json:",inline"`
}

func (Job) TableName() string {
	return "job"
}

func (job *Job) BeforeSave(tx *gorm.DB) error {
	if job.RuntimeInfo != nil {
		infoJson, err := json.Marshal(job.RuntimeInfo)
		if err != nil {
			return err
		}
		job.RuntimeInfoJson = string(infoJson)
	}
	if job.Resource != nil {
		infoJson, err := json.Marshal(&job.ResourceJson)
		if err != nil {
			return err
		}
		job.ResourceJson = string(infoJson)
	}
	return nil
}

// CreateJob creates a new job
func CreateJob(job *Job) error {
	db := dbflag.DB
	return db.Create(job).Error
}

func GetJobByID(jobID string) (Job, error) {
	var job Job
	tx := dbflag.DB.Table("job").Where("id = ?", jobID).First(&job)
	if tx.Error != nil {
		logger.LoggerForJob(jobID).Errorf("get job failed, err %v", tx.Error.Error())
		return Job{}, tx.Error
	}
	return job, nil
}

func GetJobStatusByID(jobID string) (schema.JobStatus, error) {
	job, err := GetJobByID(jobID)
	if err != nil {
		return "", errors.JobIDNotFoundError(jobID)
	}
	return job.Status, nil
}

func UpdateJobStatus(jobId, errMessage string, jobStatus schema.JobStatus) error {
	job, err := GetJobByID(jobId)
	if err != nil {
		return errors.JobIDNotFoundError(jobId)
	}
	if jobStatus != "" && !schema.IsImmutableJobStatus(job.Status) {
		job.Status = jobStatus
	}
	if errMessage != "" {
		job.Message = errMessage
	}
	log.Infof("update job [%+v]", job)
	tx := dbflag.DB.Model(&Job{}).Where("id = ?", jobId).Updates(job)
	if tx.Error != nil {
		return tx.Error
	}
	return nil
}

func UpdateJob(jobID string, status schema.JobStatus, info interface{}, message string) (schema.JobStatus, error) {
	job, err := GetJobByID(jobID)
	if err != nil {
		return "", errors.JobIDNotFoundError(jobID)
	}
	if status != "" && !schema.IsImmutableJobStatus(job.Status) {
		job.Status = status
	}
	if info != nil {
		job.RuntimeInfo = info
	}
	if message != "" {
		job.Message = message
	}
	if status == schema.StatusJobRunning {
		job.ActivatedAt.Time = time.Now()
		job.ActivatedAt.Valid = true
	}
	tx := dbflag.DB.Table("job").Where("id = ?", jobID).Save(&job)
	if tx.Error != nil {
		logger.LoggerForJob(jobID).Errorf("update job failed, err %v", err)
		return "", err
	}
	return job.Status, nil
}

func ListQueueJob(queueID string, status []schema.JobStatus) []Job {
	db := dbflag.DB.Table("job").Where("status in ?", status).Where("queue_id = ?", queueID)

	var jobs []Job
	err := db.Find(&jobs).Error
	if err != nil {
		return []Job{}
	}
	return jobs
}

func GetJobsByRunID(ctx *logger.RequestContext, runID string, jobID string) ([]Job, error) {
	var jobList []Job
	query := dbflag.DB.Table("job").Where("id like ?", "job-"+runID+"-%")
	if jobID != "" {
		query = query.Where("id = ?", jobID)
	}
	err := query.Find(&jobList).Error
	if err != nil {
		ctx.Logging().Errorf("get jobs by run[%s] failed. error : %s ", runID, err.Error())
		return nil, err
	}
	return jobList, nil
}
