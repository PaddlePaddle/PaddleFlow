/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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

package models

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

func (a *ArtifactEvent) AfterFind() error {
	a.CreateTime = a.CreatedAt.Format("2006-01-02 15:04:05")
	a.UpdateTime = a.UpdatedAt.Format("2006-01-02 15:04:05")
	return nil
}
