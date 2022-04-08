package models

import (
	"encoding/json"
	"fmt"
	"paddleflow/pkg/common/schema"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
)

const (
	JobTaskTableName = "job_task"
)

type JobTask struct {
	Pk            int64             `json:"-" gorm:"primaryKey;autoIncrement"`
	ID            string            `json:"id" gorm:"type:varchar(64);uniqueIndex"` // k8s:podID
	JobID         string            `json:"jobID" gorm:"type:varchar(60)"`
	Namespace     string            `json:"namespace" gorm:"type:varchar(64)"`
	Name          string            `json:"name" gorm:"type:varchar(512)"`
	Role          schema.RoleMember `json:"role"`
	State         string            `json:"state"`
	Message       string            `json:"message"`
	LogURL        string            `json:"logURL"`
	PodStatusJSON string            `json:"-" gorm:"column:status;default:'{}'"`
	PodStatus     interface{}       `json:"status" gorm:"-"` //k8s:v1.PodStatus
	CreatedAt     time.Time         `json:"-"`
	StartedAt     time.Time         `json:"-"`
	UpdatedAt     time.Time         `json:"-"`
	DeletedAt     time.Time         `json:"-"`
}

func (JobTask) TableName() string {
	return JobTaskTableName
}

func (task *JobTask) BeforeSave(*gorm.DB) error {
	if task.PodStatus != nil {
		statusJSON, err := json.Marshal(task.PodStatus)
		if err != nil {
			return err
		}
		task.PodStatusJSON = string(statusJSON)
	}
	return nil
}

func (task *JobTask) AfterFind(*gorm.DB) error {
	if len(task.PodStatusJSON) != 0 {
		err := json.Unmarshal([]byte(task.PodStatusJSON), task.PodStatus)
		if err != nil {
			return err
		}
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
	tx := database.DB.Table(JobTaskTableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"status", "deleted_at"}),
	}).Create(task)
	return tx.Error
}
