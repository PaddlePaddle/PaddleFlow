package models

import (
	"time"
)

type JobTaskStatus struct {
	Pk         int64       `json:"-" gorm:"primaryKey;autoIncrement"`
	ID         string      `json:"id" gorm:"type:varchar(64);uniqueIndex"` // k8s:podID
	JobID      string      `json:"-" gorm:"type:varchar(60)"`
	Namespace  string      `json:"namespace" gorm:"type:varchar(64)"`
	Name       string      `json:"name" gorm:"type:varchar(512)"`
	Status     interface{} `json:"status" gorm:"type:text"` //k8s:v1.PodStatus
	StartAt    time.Time   `json:"-"`
	UpdatedAt  time.Time   `json:"-"`
	DeletedAtd time.Time   `json:"-"`
}

func (JobTaskStatus) TableName() string {
	return "job_task_status"
}
