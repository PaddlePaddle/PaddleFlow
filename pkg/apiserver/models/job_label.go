package models

import (
	"time"

	"gorm.io/gorm"
)

type JobLabel struct {
	Pk        int64          `gorm:"primaryKey;autoIncrement"`
	Label     string         `gorm:"type:varchar(255);NOT NULL"`
	JobID     string         `gorm:"type:varchar(60);NOT NULL"`
	CreatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

func (JobLabel) TableName() string {
	return "job_label"
}
