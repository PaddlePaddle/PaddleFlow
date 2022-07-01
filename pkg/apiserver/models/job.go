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
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type Job struct {
	Pk                int64               `json:"-" gorm:"primaryKey;autoIncrement"`
	ID                string              `json:"jobID" gorm:"type:varchar(60);index:idx_id,unique;NOT NULL"`
	Name              string              `json:"jobName" gorm:"type:varchar(512);default:''"`
	UserName          string              `json:"userName" gorm:"NOT NULL"`
	QueueID           string              `json:"queueID" gorm:"NOT NULL"`
	Type              string              `json:"type" gorm:"type:varchar(20);NOT NULL"`
	ConfigJson        string              `json:"-" gorm:"column:config;type:text"`
	Config            *schema.Conf        `json:"config" gorm:"-"`
	RuntimeInfoJson   string              `json:"-" gorm:"column:runtime_info;default:'{}'"`
	RuntimeInfo       interface{}         `json:"runtimeInfo" gorm:"-"`
	Status            schema.JobStatus    `json:"status" gorm:"type:varchar(32);"`
	Message           string              `json:"message"`
	ResourceJson      string              `json:"-" gorm:"column:resource;type:text;default:'{}'"`
	Resource          *resources.Resource `json:"resource" gorm:"-"`
	Framework         schema.Framework    `json:"framework" gorm:"type:varchar(30)"`
	MembersJson       string              `json:"-" gorm:"column:members;type:text"`
	Members           []Member            `json:"members" gorm:"-"`
	ExtensionTemplate string              `json:"-" gorm:"type:text"`
	ParentJob         string              `json:"-" gorm:"type:varchar(60)"`
	CreatedAt         time.Time           `json:"createTime"`
	ActivatedAt       sql.NullTime        `json:"activateTime"`
	UpdatedAt         time.Time           `json:"updateTime,omitempty"`
	DeletedAt         string              `json:"-" gorm:"index:idx_id"`
}

type Member struct {
	ID          string            `json:"id"`
	Replicas    int               `json:"replicas"`
	Role        schema.MemberRole `json:"role"`
	schema.Conf `json:",inline"`
}

func (Job) TableName() string {
	return "job"
}

func (job *Job) BeforeSave(tx *gorm.DB) error {
	if job.ID == "" {
		job.ID = uuid.GenerateIDWithLength(schema.JobPrefix, uuid.JobIDLength)
	}
	if job.RuntimeInfo != nil {
		infoJson, err := json.Marshal(job.RuntimeInfo)
		if err != nil {
			return err
		}
		job.RuntimeInfoJson = string(infoJson)
	}
	if len(job.Members) != 0 {
		infoJson, err := json.Marshal(job.Members)
		if err != nil {
			return err
		}
		job.MembersJson = string(infoJson)
	}
	if job.Resource != nil {
		infoJson, err := json.Marshal(job.Resource)
		if err != nil {
			return err
		}
		job.ResourceJson = string(infoJson)
	}
	if job.Config != nil {
		infoJson, err := json.Marshal(job.Config)
		if err != nil {
			return err
		}
		job.ConfigJson = string(infoJson)
	}
	return nil
}

func (job *Job) AfterFind(tx *gorm.DB) error {
	if len(job.RuntimeInfoJson) > 0 {
		var runtime interface{}
		err := json.Unmarshal([]byte(job.RuntimeInfoJson), &runtime)
		if err != nil {
			log.Errorf("job[%s] json unmarshal runtime failed, error: %s", job.ID, err.Error())
			return err
		}
		job.RuntimeInfo = runtime
	}
	if len(job.MembersJson) > 0 {
		var members []Member
		err := json.Unmarshal([]byte(job.MembersJson), &members)
		if err != nil {
			log.Errorf("job[%s] json unmarshal member failed, error: %s", job.ID, err.Error())
			return err
		}
		job.Members = members
	}
	if len(job.ConfigJson) > 0 {
		conf := schema.Conf{}
		err := json.Unmarshal([]byte(job.ConfigJson), &conf)
		if err != nil {
			log.Errorf("job[%s] json unmarshal config failed, error: %s", job.ID, err.Error())
			return err
		}
		job.Config = &conf
	}
	return nil
}

// CreateJob creates a new job
func CreateJob(job *Job) error {
	db := storage.DB
	return db.Create(job).Error
}

func GetJobByID(jobID string) (Job, error) {
	var job Job
	tx := storage.DB.Table("job").Where("id = ?", jobID).Where("deleted_at = ''").First(&job)
	if tx.Error != nil {
		logger.LoggerForJob(jobID).Errorf("get job failed, err %v", tx.Error.Error())
		return Job{}, tx.Error
	}
	return job, nil
}

func GetUnscopedJobByID(jobID string) (Job, error) {
	var job Job
	tx := storage.DB.Table("job").Where("id = ?", jobID).First(&job)
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

func DeleteJob(jobID string) error {
	t := storage.DB.Table("job").Where("id = ?", jobID).Where("deleted_at = ''").UpdateColumn("deleted_at", time.Now().Format(TimeFormat))
	if t.Error != nil {
		return t.Error
	}
	return nil
}

func UpdateJobStatus(jobId, errMessage string, newStatus schema.JobStatus) error {
	job, err := GetJobByID(jobId)
	if err != nil {
		return errors.JobIDNotFoundError(jobId)
	}
	job.Status, errMessage = jobStatusTransition(job.ID, job.Status, newStatus, errMessage)
	if errMessage != "" {
		job.Message = errMessage
	}
	log.Infof("update job [%+v]", job)
	tx := storage.DB.Model(&Job{}).Where("id = ?", jobId).Where("deleted_at = ''").Updates(job)
	if tx.Error != nil {
		return tx.Error
	}
	return nil
}

func UpdateJobConfig(jobId string, conf *schema.Conf) error {
	if conf == nil {
		return fmt.Errorf("job config is nil")
	}
	confJSON, err := json.Marshal(conf)
	if err != nil {
		return err
	}
	log.Infof("update job config [%v]", conf)
	tx := storage.DB.Model(&Job{}).Where("id = ?", jobId).Where("deleted_at = ''").UpdateColumn("config", confJSON)
	if tx.Error != nil {
		return tx.Error
	}
	return nil
}

func jobStatusTransition(jobID string, preStatus, newStatus schema.JobStatus, msg string) (schema.JobStatus, string) {
	if schema.IsImmutableJobStatus(preStatus) {
		return preStatus, ""
	}
	if preStatus == schema.StatusJobTerminating {
		if newStatus == schema.StatusJobRunning {
			newStatus = schema.StatusJobTerminating
			msg = "job is terminating"
		} else {
			newStatus = schema.StatusJobTerminated
			msg = "job is terminated"
		}
	}
	log.Infof("job %s status update from %s to %s", jobID, preStatus, newStatus)
	return newStatus, msg
}

func UpdateJob(jobID string, status schema.JobStatus, info interface{}, message string) (schema.JobStatus, error) {
	job, err := GetUnscopedJobByID(jobID)
	if err != nil {
		return "", errors.JobIDNotFoundError(jobID)
	}
	job.Status, message = jobStatusTransition(jobID, job.Status, status, message)
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
	log.Debugf("update job [%+v]", job)
	tx := storage.DB.Table("job").Where("id = ?", jobID).Where("deleted_at = ''").Updates(&job)
	if tx.Error != nil {
		logger.LoggerForJob(jobID).Errorf("update job failed, err %v", tx.Error)
		return "", tx.Error
	}
	return job.Status, nil
}

func ListQueueJob(queueID string, status []schema.JobStatus) []Job {
	db := storage.DB.Table("job").Where("status in ?", status).Where("queue_id = ?", queueID).Where("deleted_at = ''")

	var jobs []Job
	err := db.Find(&jobs).Error
	if err != nil {
		return []Job{}
	}
	return jobs
}

func GetJobsByRunID(runID string, jobID string) ([]Job, error) {
	var jobList []Job
	query := storage.DB.Table("job").Where("id like ?", "job-"+runID+"-%").Where("deleted_at = ''")
	if jobID != "" {
		query = query.Where("id = ?", jobID)
	}
	err := query.Find(&jobList).Error
	if err != nil {
		log.Errorf("get jobs by run[%s] failed. error : %s ", runID, err.Error())
		return nil, err
	}
	return jobList, nil
}

func ListJobByUpdateTime(updateTime string) ([]Job, error) {
	var jobList []Job
	err := storage.DB.Table("job").Where("updated_at >= ?", updateTime).Where("deleted_at = ''").Find(&jobList).Error
	if err != nil {
		log.Errorf("list job by updateTime[%s] failed, error:[%s]", updateTime, err.Error())
		return nil, err
	}
	return jobList, nil
}

func ListJobByParentID(parentID string) ([]Job, error) {
	var jobList []Job
	err := storage.DB.Table("job").Where("parent_job = ?", parentID).Where("deleted_at = ''").Find(&jobList).Error
	if err != nil {
		log.Errorf("list job by parentID[%s] failed, error:[%s]", parentID, err.Error())
		return nil, err
	}
	return jobList, nil
}

func GetLastJob() (Job, error) {
	job := Job{}
	tx := storage.DB.Table("job").Where("deleted_at = ''").Last(&job)
	if tx.Error != nil {
		log.Errorf("get last job failed. error:%s", tx.Error.Error())
		return Job{}, tx.Error
	}
	return job, nil
}

func ListJob(pk int64, maxKeys int, queue, status, startTime, timestamp, userFilter string, labels map[string]string) ([]Job, error) {
	tx := storage.DB.Table("job").Where("pk > ?", pk).Where("parent_job = ''").Where("deleted_at = ''")
	if userFilter != "root" {
		tx = tx.Where("user_name = ?", userFilter)
	}
	if queue != "" {
		tx = tx.Where("queue_id = ?", queue)
	}
	if status != "" {
		tx = tx.Where("status = ?", status)
	}
	if startTime != "" {
		tx = tx.Where("activated_at > ?", startTime)
	}
	if len(labels) > 0 {
		jobIDs, err := ListJobIDByLabels(labels)
		if err != nil {
			return []Job{}, err
		}
		tx = tx.Where("id IN (?)", jobIDs)
	}
	if timestamp != "" {
		tx = tx.Where("updated_at > ?", timestamp)
	}
	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var jobList []Job
	tx = tx.Find(&jobList)
	if tx.Error != nil {
		return []Job{}, tx.Error
	}

	return jobList, nil
}
