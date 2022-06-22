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

const FsMountTableName = "fs_mount"

// FsMount defined file system mount info
type FsMount struct {
	PK         int64          `json:"-" gorm:"primaryKey;autoIncrement"`
	MountID    string         `json:"mountID" gorm:"type:varchar(36);column:mount_id"`
	FsID       string         `json:"fsID" gorm:"type:varchar(36);column:fs_id"`
	MountPoint string         `json:"mountpoint" gorm:"type:varchar(4096);column:mountpoint"`
	NodeName   string         `json:"nodename" gorm:"type:varchar(256);column:nodename"`
	ClusterID  string         `json:"-"   gorm:"column:cluster_id;default:''"`
	CreatedAt  time.Time      `json:"-"`
	UpdatedAt  time.Time      `json:"-"`
	DeletedAt  gorm.DeletedAt `json:"-"  gorm:"index"`
}

func (f *FsMount) TableName() string {
	return FsMountTableName
}
