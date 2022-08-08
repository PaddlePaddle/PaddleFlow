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

package storage

import (
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/metrics"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type JobStore struct {
	db *gorm.DB
}

func newJobStore(db *gorm.DB) *JobStore {
	return &JobStore{db: db}
}

// CreateJob creates a new job
func (js *JobStore) CreateJob(job *model.Job) error {
	if job.ID == "" {
		job.ID = uuid.GenerateIDWithLength(schema.JobPrefix, uuid.JobIDLength)
	}
	err := js.db.Create(job).Error
	if err == nil {
		// in case panic
		var queueName string
		if job.Config != nil {
			queueName = job.Config.GetQueueName()
		}
		metrics.Job.AddTimestamp(job.ID, metrics.T2, time.Now(), metrics.Info{
			metrics.QueueIDLabel:   job.QueueID,
			metrics.QueueNameLabel: queueName,
			metrics.UserNameLabel:  job.UserName,
		})
	}
	return err
}

func (js *JobStore) GetJobByID(jobID string) (model.Job, error) {
	var job model.Job
	tx := js.db.Table("job").Where("id = ?", jobID).Where("deleted_at = ''").First(&job)
	if tx.Error != nil {
		logger.LoggerForJob(jobID).Errorf("get job failed, err %v", tx.Error.Error())
		return model.Job{}, tx.Error
	}
	return job, nil
}

func (js *JobStore) GetUnscopedJobByID(jobID string) (model.Job, error) {
	var job model.Job
	tx := js.db.Table("job").Where("id = ?", jobID).First(&job)
	if tx.Error != nil {
		logger.LoggerForJob(jobID).Errorf("get job failed, err %v", tx.Error.Error())
		return model.Job{}, tx.Error
	}
	return job, nil
}

func (js *JobStore) GetJobStatusByID(jobID string) (schema.JobStatus, error) {
	job, err := js.GetJobByID(jobID)
	if err != nil {
		return "", errors.JobIDNotFoundError(jobID)
	}
	return job.Status, nil
}

func (js *JobStore) DeleteJob(jobID string) error {
	t := js.db.Table("job").Where("id = ?", jobID).Where("deleted_at = ''").UpdateColumn("deleted_at", time.Now().Format(model.TimeFormat))
	if t.Error != nil {
		return t.Error
	}
	return nil
}

func (js *JobStore) UpdateJobStatus(jobId, errMessage string, newStatus schema.JobStatus) error {
	job, err := js.GetJobByID(jobId)
	if err != nil {
		return errors.JobIDNotFoundError(jobId)
	}
	updatedJob := model.Job{}
	updatedJob.Status, errMessage = jobStatusTransition(job.ID, job.Status, newStatus, errMessage)
	if errMessage != "" {
		updatedJob.Message = errMessage
	}
	log.Infof("update for job %s, updated content [%+v]", jobId, updatedJob)
	tx := js.db.Model(&model.Job{}).Where("id = ?", jobId).Where("deleted_at = ''").Updates(updatedJob)
	if tx.Error != nil {
		return tx.Error
	}
	return nil
}

func (js *JobStore) UpdateJobConfig(jobId string, conf *schema.Conf) error {
	if conf == nil {
		return fmt.Errorf("job config is nil")
	}
	confJSON, err := json.Marshal(conf)
	if err != nil {
		return err
	}
	log.Infof("update job config [%v]", conf)
	tx := js.db.Model(&model.Job{}).Where("id = ?", jobId).Where("deleted_at = ''").UpdateColumn("config", confJSON)
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

func (js *JobStore) UpdateJob(jobID string, status schema.JobStatus, runtimeInfo, runtimeStatus interface{}, message string) (schema.JobStatus, error) {
	job, err := js.GetUnscopedJobByID(jobID)
	if err != nil {
		return "", errors.JobIDNotFoundError(jobID)
	}
	updatedJob := model.Job{}
	updatedJob.Status, message = jobStatusTransition(jobID, job.Status, status, message)
	if runtimeInfo != nil {
		updatedJob.RuntimeInfo = runtimeInfo
	}
	if runtimeStatus != nil {
		updatedJob.RuntimeStatus = runtimeStatus
	}
	if message != "" {
		updatedJob.Message = message
	}
	if status == schema.StatusJobRunning && !job.ActivatedAt.Valid {
		// add queue id here
		// in case panic
		var queueName, userName string
		if job.Config != nil {
			queueName = job.Config.GetQueueName()
			userName = job.Config.GetUserName()
		}
		metrics.Job.AddTimestamp(jobID, metrics.T7, time.Now(), metrics.Info{
			metrics.QueueIDLabel:   job.QueueID,
			metrics.QueueNameLabel: queueName,
			metrics.UserNameLabel:  userName,
		})
		updatedJob.ActivatedAt.Time = time.Now()
		updatedJob.ActivatedAt.Valid = true
	}
	log.Debugf("update for job %s, updated content [%+v]", jobID, updatedJob)
	tx := js.db.Table("job").Where("id = ?", jobID).Where("deleted_at = ''").Updates(&updatedJob)
	if tx.Error != nil {
		log.Errorf("update job failed, err %v", tx.Error)
		return "", tx.Error
	}
	return updatedJob.Status, nil
}

func (js *JobStore) ListQueueJob(queueID string, status []schema.JobStatus) []model.Job {
	db := js.db.Table("job").Where("status in ?", status).Where("queue_id = ?", queueID).Where("deleted_at = ''")

	var jobs []model.Job
	err := db.Find(&jobs).Error
	if err != nil {
		return []model.Job{}
	}
	return jobs
}

func (js *JobStore) ListQueueInitJob(queueID string) []model.Job {
	db := js.db.Table("job").Where("queue_id = ?", queueID).Where("status = ?", schema.StatusJobInit).Where("deleted_at = ''")

	var jobs []model.Job
	err := db.Find(&jobs).Error
	if err != nil {
		log.Errorf("list init jobs in queue %s failed, err: %s", queueID, err.Error())
		return []model.Job{}
	}
	return jobs
}

func (js *JobStore) ListJobsByQueueIDsAndStatus(queueIDs []string, status schema.JobStatus) []model.Job {
	var jobs []model.Job
	db := js.db.Table("job").Where("queue_id in ?", queueIDs).Where("status = ?", status).Where("deleted_at = ''")
	err := db.Find(&jobs).Error
	if err != nil {
		log.Errorf("list jobs in queues %v with status %s failed, err: %s", queueIDs, status, err.Error())
		return []model.Job{}
	}
	return jobs
}

func (js *JobStore) ListJobByStatus(status schema.JobStatus) []model.Job {
	db := js.db.Table("job").Where("status = ?", status).Where("deleted_at = ''")

	var jobs []model.Job
	if err := db.Find(&jobs).Error; err != nil {
		log.Errorf("get collect jobs failed, error:%s", err.Error())
		return []model.Job{}
	}
	return jobs
}

func (js *JobStore) GetJobsByRunID(runID string, jobID string) ([]model.Job, error) {
	var jobList []model.Job
	query := js.db.Table("job").Where("id like ?", "job-"+runID+"-%").Where("deleted_at = ''")
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

func (js *JobStore) ListJobByUpdateTime(updateTime string) ([]model.Job, error) {
	var jobList []model.Job
	err := js.db.Table("job").Where("updated_at >= ?", updateTime).Where("deleted_at = ''").Find(&jobList).Error
	if err != nil {
		log.Errorf("list job by updateTime[%s] failed, error:[%s]", updateTime, err.Error())
		return nil, err
	}
	return jobList, nil
}

func (js *JobStore) ListJobByParentID(parentID string) ([]model.Job, error) {
	var jobList []model.Job
	err := js.db.Table("job").Where("parent_job = ?", parentID).Where("deleted_at = ''").Find(&jobList).Error
	if err != nil {
		log.Errorf("list job by parentID[%s] failed, error:[%s]", parentID, err.Error())
		return nil, err
	}
	return jobList, nil
}

func (js *JobStore) GetLastJob() (model.Job, error) {
	job := model.Job{}
	tx := js.db.Table("job").Where("deleted_at = ''").Last(&job)
	if tx.Error != nil {
		log.Errorf("get last job failed. error:%s", tx.Error.Error())
		return model.Job{}, tx.Error
	}
	return job, nil
}

func (js *JobStore) ListJob(pk int64, maxKeys int, queue, status, startTime, timestamp, userFilter string, labels map[string]string) ([]model.Job, error) {
	tx := js.db.Table("job").Where("pk > ?", pk).Where("parent_job = ''").Where("deleted_at = ''")
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
		jobIDs, err := js.ListJobIDByLabels(labels)
		if err != nil {
			return []model.Job{}, err
		}
		tx = tx.Where("id IN (?)", jobIDs)
	}
	if timestamp != "" {
		tx = tx.Where("updated_at > ?", timestamp)
	}
	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var jobList []model.Job
	tx = tx.Find(&jobList)
	if tx.Error != nil {
		return []model.Job{}, tx.Error
	}

	return jobList, nil
}

// list job process multi label get and result
func (js *JobStore) ListJobIDByLabels(labels map[string]string) ([]string, error) {
	jobIDs := make([]string, 0)
	var jobLabels []model.JobLabel
	labelsListStr := make([]string, 0)
	for k, v := range labels {
		item := k + "=" + v
		labelsListStr = append(labelsListStr, item)
	}
	err := js.db.Table("job_label").Where("label IN (?)", labelsListStr).Find(&jobLabels).Error
	if err != nil {
		log.Errorf("list jobID by labels failed, error:[%s]", err.Error())
		return nil, err
	}
	jobLabelsMap := make(map[string]map[string]interface{}, 0)
	for _, v := range jobLabels {
		if _, ok := jobLabelsMap[v.JobID]; !ok {
			jobLabelsMap[v.JobID] = make(map[string]interface{}, 0)
			jobLabelsMap[v.JobID][v.Label] = nil
		} else {
			jobLabelsMap[v.JobID][v.Label] = nil
		}
	}
	for k, v := range jobLabelsMap {
		flag := true
		for _, label := range labelsListStr {
			if _, ok := v[label]; !ok {
				flag = false
				break
			}
		}
		if flag {
			jobIDs = append(jobIDs, k)
		}
	}
	return jobIDs, nil
}

func (js *JobStore) GetJobTaskByID(id string) (model.JobTask, error) {
	var taskStatus model.JobTask
	tx := js.db.Table(model.JobTaskTableName).Where("id = ?", id).First(&taskStatus)
	if tx.Error != nil {
		logger.LoggerForJob(id).Errorf("get job task status failed, err %v", tx.Error.Error())
		return model.JobTask{}, tx.Error
	}
	return taskStatus, nil
}

func (js *JobStore) UpdateTask(task *model.JobTask) error {
	if task == nil {
		return fmt.Errorf("JobTask is nil")
	}
	// TODO: change update task logic
	tx := js.db.Table(model.JobTaskTableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"status", "message", "ext_runtime_status", "node_name", "deleted_at"}),
	}).Create(task)
	return tx.Error
}

func (js *JobStore) ListByJobID(jobID string) ([]model.JobTask, error) {
	var jobList []model.JobTask
	err := js.db.Table(model.JobTaskTableName).Where("job_id = ?", jobID).Find(&jobList).Error
	if err != nil {
		return nil, err
	}
	return jobList, nil
}
