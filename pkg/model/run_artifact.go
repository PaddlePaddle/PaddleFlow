package model

import (
	"time"

	"gorm.io/gorm"
)

type ArtifactEvent struct {
	Pk           int64          `json:"-"                    gorm:"primaryKey;autoIncrement;not null"`
	Md5          string         `json:"-"                    gorm:"type:varchar(32);not null"`
	RunID        string         `json:"runID"                gorm:"type:varchar(60);not null"`
	FsID         string         `json:"-"                    gorm:"type:varchar(60);not null"`
	FsName       string         `json:"fsname"               gorm:"type:varchar(60);not null"`
	UserName     string         `json:"username"             gorm:"type:varchar(60);not null"`
	ArtifactPath string         `json:"artifactPath"         gorm:"type:varchar(256);not null"`
	Step         string         `json:"step"                 gorm:"type:varchar(256);not null"`
	JobID        string         `json:"jobID"                gorm:"type:varchar(60);not null"`
	Type         string         `json:"type"                 gorm:"type:varchar(16);not null"`
	ArtifactName string         `json:"artifactName"         gorm:"type:varchar(32);not null"`
	Meta         string         `json:"meta"                 gorm:"type:text;size:65535"`
	CreateTime   string         `json:"createTime"           gorm:"-"`
	UpdateTime   string         `json:"updateTime,omitempty" gorm:"-"`
	CreatedAt    time.Time      `json:"-"`
	UpdatedAt    time.Time      `json:"-"`
	DeletedAt    gorm.DeletedAt `json:"-"                    gorm:"index"`
}

func (ArtifactEvent) TableName() string {
	return "artifact_event"
}

func (a *ArtifactEvent) AfterFind(*gorm.DB) error {
	// format time
	a.CreateTime = a.CreatedAt.Format("2006-01-02 15:04:05")
	a.UpdateTime = a.UpdatedAt.Format("2006-01-02 15:04:05")
	return nil
}
