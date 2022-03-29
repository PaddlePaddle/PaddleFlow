package models

import (
	"time"

	"gorm.io/gorm"
)

type JobLabel struct {
	Pk        int64          `json:"-" gorm:"primaryKey;autoIncrement"`
	Label     string         `json:"label" gorm:"type:varchar(255);NOT NULL"`
	JobID     string         `json:"jobID" gorm:"type:varchar(60);NOT NULL"`
	CreatedAt time.Time      `json:"createTime"`
	UpdatedAt time.Time      `json:"updateTime,omitempty"`
	DeletedAt gorm.DeletedAt `json:"-" gorm:"index"`
}

func (JobLabel) TableName() string {
	return "job_label"
}
