package models

import (
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
)

const (
	JobTaskStatusTableName = "job_task_status"
)

type JobTaskStatus struct {
	Pk         int64       `json:"-" gorm:"primaryKey;autoIncrement"`
	ID         string      `json:"id" gorm:"type:varchar(64);uniqueIndex"` // k8s:podID
	JobID      string      `json:"-" gorm:"type:varchar(60)"`
	Namespace  string      `json:"namespace" gorm:"type:varchar(64)"`
	Name       string      `json:"name" gorm:"type:varchar(512)"`
	StatusJSON string      `json:"-" gorm:"column:status;default:'{}'"`
	Status     interface{} `json:"status" gorm:"-"` //k8s:v1.PodStatus
	CreatedAt  time.Time   `json:"-"`
	UpdatedAt  time.Time   `json:"-"`
	DeletedAt  time.Time   `json:"-"`
}

func (JobTaskStatus) TableName() string {
	return JobTaskStatusTableName
}

func (taskStatus *JobTaskStatus) BeforeSave(*gorm.DB) error {
	if taskStatus.Status != nil {
		statusJSON, err := json.Marshal(taskStatus.Status)
		if err != nil {
			return err
		}
		taskStatus.StatusJSON = string(statusJSON)
	}
	return nil
}

func (taskStatus *JobTaskStatus) AfterFind(*gorm.DB) error {
	if len(taskStatus.StatusJSON) != 0 {
		err := json.Unmarshal([]byte(taskStatus.StatusJSON), taskStatus.Status)
		if err != nil {
			return err
		}
	}
	return nil
}

func GetJobTaskByID(id string) (JobTaskStatus, error) {
	var taskStatus JobTaskStatus
	tx := database.DB.Table(JobTaskStatusTableName).Where("id = ?", id).First(&taskStatus)
	if tx.Error != nil {
		logger.LoggerForJob(id).Errorf("get job task status failed, err %v", tx.Error.Error())
		return JobTaskStatus{}, tx.Error
	}
	return taskStatus, nil
}

func UpdateTask(taskStatus *JobTaskStatus) error {
	if taskStatus == nil {
		return fmt.Errorf("JobTaskStatus is nil")
	}
	tx := database.DB.Table(JobTaskStatusTableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"status", "deleted_at"}),
	}).Create(taskStatus)
	return tx.Error
}
