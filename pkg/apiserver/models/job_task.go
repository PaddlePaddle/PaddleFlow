package models

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	v1 "k8s.io/api/core/v1"

	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
)

const (
	JobTaskTableName = "job_task"
)

type JobTask struct {
	Pk                   int64             `json:"-" gorm:"primaryKey;autoIncrement"`
	ID                   string            `json:"id" gorm:"type:varchar(64);uniqueIndex"` // k8s:podID
	JobID                string            `json:"jobID" gorm:"type:varchar(60)"`
	Namespace            string            `json:"namespace" gorm:"type:varchar(64)"`
	Name                 string            `json:"name" gorm:"type:varchar(512)"`
	MemberRole           schema.MemberRole `json:"memberRole"`
	Status               schema.TaskStatus `json:"status"`
	Message              string            `json:"message"`
	LogURL               string            `json:"logURL"`
	ExtRuntimeStatusJSON string            `json:"extRuntimeStatus" gorm:"column:ext_runtime_status;default:'{}'"`
	ExtRuntimeStatus     interface{}       `json:"-" gorm:"-"` //k8s:v1.PodStatus
	CreatedAt            time.Time         `json:"-"`
	StartedAt            sql.NullTime      `json:"-"`
	UpdatedAt            time.Time         `json:"-"`
	DeletedAt            sql.NullTime      `json:"-"`
}

func (JobTask) TableName() string {
	return JobTaskTableName
}

func (task *JobTask) BeforeSave(*gorm.DB) error {
	if task.ExtRuntimeStatus != nil {
		statusJSON, err := json.Marshal(task.ExtRuntimeStatus)
		if err != nil {
			return err
		}
		task.ExtRuntimeStatusJSON = string(statusJSON)
	}
	return nil
}

func (task *JobTask) AfterFind(*gorm.DB) error {
	if len(task.ExtRuntimeStatusJSON) > 0 {
		var podStatus v1.PodStatus
		if err := json.Unmarshal([]byte(task.ExtRuntimeStatusJSON), &podStatus); err != nil {
			return err
		}
		task.ExtRuntimeStatus = podStatus
	}
	return nil
}

func GetJobTaskByID(id string) (JobTask, error) {
	var taskStatus JobTask
	tx := database.DB.Table(JobTaskTableName).Where("id = ?", id).First(&taskStatus)
	if tx.Error != nil {
		logger.LoggerForJob(id).Errorf("get job task status failed, err %v", tx.Error.Error())
		return JobTask{}, tx.Error
	}
	return taskStatus, nil
}

func UpdateTask(task *JobTask) error {
	if task == nil {
		return fmt.Errorf("JobTask is nil")
	}
	// TODO: change update task logic
	tx := database.DB.Table(JobTaskTableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"status", "message", "ext_runtime_status", "deleted_at"}),
	}).Create(task)
	return tx.Error
}

func ListByJobID(jobID string) ([]JobTask, error) {
	var jobList []JobTask
	err := database.DB.Table(JobTaskTableName).Where("job_id = ?", jobID).Find(&jobList).Error
	if err != nil {
		return nil, err
	}
	return jobList, nil
}
