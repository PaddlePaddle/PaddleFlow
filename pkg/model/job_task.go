package model

import (
	"database/sql"
	"encoding/json"
	"time"

	"gorm.io/gorm"
	v1 "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
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
	ExtRuntimeStatus     interface{}       `json:"-" gorm:"-"` // k8s:v1.PodStatus
	NodeName             string            `json:"nodeName"`
	Annotations          Map               `json:"annotations"` // k8s:metav1.Annotations
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
