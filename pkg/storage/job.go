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
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

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
	updatedJob.Status, errMessage = JobStatusTransition(job.ID, job.Status, newStatus, errMessage)
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

func JobStatusTransition(jobID string, preStatus, newStatus schema.JobStatus, msg string) (schema.JobStatus, string) {
	if schema.IsImmutableJobStatus(preStatus) {
		return preStatus, ""
	}
	switch preStatus {
	case schema.StatusJobTerminating:
		if newStatus == schema.StatusJobRunning {
			newStatus = schema.StatusJobTerminating
			msg = "job is terminating"
		} else {
			newStatus = schema.StatusJobTerminated
			msg = "job is terminated"
		}
	case schema.StatusJobPreempting:
		if schema.IsImmutableJobStatus(newStatus) {
			newStatus = schema.StatusJobPreempted
		} else {
			newStatus = schema.StatusJobPreempting
		}
	}
	log.Infof("job %s status update from %s to %s", jobID, preStatus, newStatus)
	return newStatus, msg
}

// Update the status of existing job
func (js *JobStore) Update(jobID string, job *model.Job) error {
	tx := js.db.Model(&model.Job{}).Where("id = ?", jobID).Where("deleted_at = ''").Updates(job)
	if tx.Error != nil {
		return tx.Error
	}
	return nil
}

// UpdateJob update job status and runtime info Deprecated
func (js *JobStore) UpdateJob(jobID string, status schema.JobStatus, runtimeInfo, runtimeStatus interface{}, message string) (schema.JobStatus, error) {
	job, err := js.GetUnscopedJobByID(jobID)
	if err != nil {
		return "", errors.JobIDNotFoundError(jobID)
	}
	updatedJob := model.Job{}
	updatedJob.Status, message = JobStatusTransition(jobID, job.Status, status, message)
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

func findMapWithDefault(mp map[string]string, key, defaultValue string) string {
	value, find := mp[key]
	if !find {
		value = defaultValue
	}
	return value
}

func (js *JobStore) ListJob(filter JobFilter) ([]model.Job, error) {
	tx := js.db.Table("job")
	if len(filter.Labels) > 0 {
		jobIDs, err := js.ListJobIDByLabels(filter.Labels)
		if err != nil {
			return []model.Job{}, err
		}
		tx = tx.Where("id IN (?)", jobIDs)
	}
	if len(filter.QueueIDs) > 0 {
		tx = tx.Where("queue_id IN ?", filter.QueueIDs)
	}
	if len(filter.Status) > 0 {
		tx = tx.Where("status IN ?", filter.Status)
	}
	tx = tx.Where("deleted_at = ''")
	// filter by updateTime
	if filter.UpdateTime != "" {
		tx = tx.Where("updated_at >= ?", filter.UpdateTime)
	}
	// filter by startTime
	if filter.StartTime != "" {
		tx = tx.Where("activated_at >= ?", filter.StartTime)
	}
	// filter by parent job
	if filter.ParentID != "" {
		tx = tx.Where("parent_job = ?", filter.ParentID)
	}
	if filter.PK > 0 {
		tx = tx.Where("pk > ? and parent_job = '' ", filter.PK)
	}
	// filter by user
	if filter.User != "" && filter.User != "root" {
		tx = tx.Where("user_name = ?", filter.User)
	}
	// set orderBy and order
	order := findMapWithDefault(model.OrderMap, strings.ToLower(filter.Order), "asc")
	orderBy := findMapWithDefault(model.OrderByMap, filter.OrderBy, "created_at")
	tx = tx.Order(fmt.Sprintf("%s %s", orderBy, order))
	// set limit
	if filter.MaxKeys > 0 {
		tx = tx.Limit(filter.MaxKeys)
	}
	var jobList []model.Job
	tx = tx.Find(&jobList)
	if tx.Error != nil {
		return []model.Job{}, tx.Error
	}
	return jobList, nil
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

func (js *JobStore) GetTaskByID(id string) (model.JobTask, error) {
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
	var taskInfo model.JobTask
	tx := js.db.Table(model.JobTaskTableName).Where("id = ?", task.ID).First(&taskInfo)
	if tx.Error != nil && gorm.ErrRecordNotFound != tx.Error {
		logger.LoggerForJob(task.ID).Errorf("get job task status failed, err %v", tx.Error.Error())
		return tx.Error
	}
	if taskInfo.ID == task.ID {
		// update task
		tx = js.db.Table(model.JobTaskTableName).Where("id = ?", task.ID).Updates(task)
	} else {
		// create task
		tx = js.db.Table(model.JobTaskTableName).Create(task)
	}
	return tx.Error
}

func (js *JobStore) ListTaskByJobID(jobID string) ([]model.JobTask, error) {
	var jobList []model.JobTask
	err := js.db.Table(model.JobTaskTableName).Where("job_id = ?", jobID).Find(&jobList).Error
	if err != nil {
		return nil, err
	}
	return jobList, nil
}

func (js *JobStore) ListJobStat(startDate, endDate time.Time, queueID, caseType string, minDuration time.Duration) ([]*model.Job, error) {
	session := js.db.Table("job").Select("id,queue_id,user_name,activated_at,finished_at").
		Where(" queue_id = ?", queueID)
	jobStatus := []*model.Job{}
	switch caseType {
	case "case1":
		session = session.Where("activated_at <= ? and activated_at != '0000-00-00 00:00:00'", startDate).
			Where("finished_at >= ? or finished_at = '0000-00-00 00:00:00'", endDate)
	case "case2":
		session = session.Where("activated_at <= ? and activated_at != '0000-00-00 00:00:00'", startDate).
			Where("finished_at <= ? and finished_at > ?", endDate, startDate).
			Where(" TIMESTAMPDIFF(Second,?,finished_at) > ?", startDate, int(minDuration.Seconds())).
	case "case3":
		session = session.Where("activated_at >= ?", startDate).
			Where("finished_at <= ? ", endDate).
			Where("TIMESTAMPDIFF(Second,activated_at,finished_at) > ? ", int(minDuration.Seconds())).
	case "case4":
		session = session.Where("activated_at >= ?", startDate).
			Where("finished_at >= ? or finished_at = '0000-00-00 00:00:00'", endDate).
			Where("TIMESTAMPDIFF(Second,activated_at,?) > ?", endDate, int(minDuration.Seconds())).
	}
	result := session.Find(&jobStatus)
	if result.Error != nil {
		logger.Logger().Errorf("get job status for %v failed, err %v", caseType, result.Error.Error())
		return nil, result.Error
	}
	return jobStatus, nil
}
