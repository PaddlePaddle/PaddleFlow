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

package model

import (
	"time"

	"gorm.io/gorm"
)

type PipelineVersion struct {
	Pk           int64          `json:"-"                    gorm:"primaryKey;autoIncrement;not null"`
	ID           string         `json:"pipelineVersionID"    gorm:"type:varchar(60);not null"`
	PipelineID   string         `json:"pipelineID"           gorm:"type:varchar(60);not null"`
	FsID         string         `json:"-"                    gorm:"type:varchar(60);not null"`
	FsName       string         `json:"fsName"               gorm:"type:varchar(60);not null"`
	YamlPath     string         `json:"yamlPath"             gorm:"type:text;size:65535;not null"`
	PipelineYaml string         `json:"pipelineYaml"         gorm:"type:text;size:65535;not null"`
	PipelineMd5  string         `json:"pipelineMd5"          gorm:"type:varchar(32);not null"`
	UserName     string         `json:"username"             gorm:"type:varchar(60);not null"`
	CreatedAt    time.Time      `json:"-"`
	UpdatedAt    time.Time      `json:"-"`
	DeletedAt    gorm.DeletedAt `json:"-"`
}

func (PipelineVersion) TableName() string {
	return "pipeline_version"
}
