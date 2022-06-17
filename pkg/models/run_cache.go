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

type RunCache struct {
	Pk          int64          `json:"-"                    gorm:"primaryKey;autoIncrement;not null"`
	ID          string         `json:"cacheID"              gorm:"type:varchar(60);not null;uniqueIndex"`
	FirstFp     string         `json:"firstFp"              gorm:"type:varchar(256)"`
	SecondFp    string         `json:"secondFp"             gorm:"type:varchar(256)"`
	RunID       string         `json:"runID"                gorm:"type:varchar(60);not null"`
	Source      string         `json:"source"               gorm:"type:varchar(256);not null"`
	Step        string         `json:"step"                 gorm:"type:varchar(256);not null"`
	FsID        string         `json:"-"                    gorm:"type:varchar(60);not null"`
	FsName      string         `json:"fsname"               gorm:"type:varchar(60);not null"`
	UserName    string         `json:"username"             gorm:"type:varchar(60);not null"`
	ExpiredTime string         `json:"expiredTime"          gorm:"type:varchar(64);default:'-1'"`
	Strategy    string         `json:"strategy"             gorm:"type:varchar(16);default:'conservative'"`
	Custom      string         `json:"custom"               gorm:"type:text;size:65535"`
	CreateTime  string         `json:"createTime"           gorm:"-"`
	UpdateTime  string         `json:"updateTime,omitempty" gorm:"-"`
	CreatedAt   time.Time      `json:"-"`
	UpdatedAt   time.Time      `json:"-"`
	DeletedAt   gorm.DeletedAt `json:"-"                    gorm:"index"`
}

func (RunCache) TableName() string {
	return "run_cache"
}

func (c *RunCache) AfterFind(*gorm.DB) error {
	c.CreateTime = c.CreatedAt.Format("2006-01-02 15:04:05")
	c.UpdateTime = c.UpdatedAt.Format("2006-01-02 15:04:05")
	return nil
}
