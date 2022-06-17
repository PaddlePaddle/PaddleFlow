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

type Pipeline struct {
	Pk           int64          `json:"-"                    gorm:"primaryKey;autoIncrement;not null"`
	ID           string         `json:"pipelineID"           gorm:"type:varchar(60);not null;uniqueIndex"`
	Name         string         `json:"name"                 gorm:"type:varchar(60);not null;uniqueIndex:idx_fs_name"`
	FsID         string         `json:"-"                    gorm:"type:varchar(60);not null;uniqueIndex:idx_fs_name;uniqueIndex:idx_fs_md5"`
	FsName       string         `json:"fsname"               gorm:"type:varchar(60);not null"`
	UserName     string         `json:"username"             gorm:"type:varchar(60);not null"`
	PipelineYaml string         `json:"pipelineYaml"         gorm:"type:text;size:65535"`
	PipelineMd5  string         `json:"pipelineMd5"          gorm:"type:varchar(32);not null;uniqueIndex:idx_fs_md5"`
	CreateTime   string         `json:"createTime,omitempty" gorm:"-"`
	UpdateTime   string         `json:"updateTime,omitempty" gorm:"-"`
	CreatedAt    time.Time      `json:"-"`
	UpdatedAt    time.Time      `json:"-"`
	DeletedAt    gorm.DeletedAt `json:"-"                    gorm:"index"`
}

func (Pipeline) TableName() string {
	return "pipeline"
}

func (p *Pipeline) AfterFind(*gorm.DB) error {
	p.CreateTime = p.CreatedAt.Format("2006-01-02 15:04:05")
	p.UpdateTime = p.UpdatedAt.Format("2006-01-02 15:04:05")
	return nil
}
