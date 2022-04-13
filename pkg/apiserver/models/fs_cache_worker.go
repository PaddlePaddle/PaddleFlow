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

package models

import (
	"time"

	"gorm.io/gorm"
)

type FSCacheWorker struct {
	PK         int64          `json:"-" gorm:"primaryKey;autoIncrement"`
	FSID       string         `json:"fsID" gorm:"type:varchar(36);column:fs_id"`
	CacheDir   string         `json:"cacheDir" gorm:"type:varchar(4096);column:cache_dir"`
	NodeName   string         `json:"nodename" gorm:"type:varchar(256);column:nodename"`
	MountPoint string         `json:"mountpoint" gorm:"type:varchar(1024);column:mountpoint"`
	UsedSize   int            `json:"usedSize" gorm:"type:bigint(20);column:usedsize"`
	CreatedAt  time.Time      `json:"-"`
	UpdatedAt  time.Time      `json:"-"`
	DeletedAt  gorm.DeletedAt `json:"-"`
}

func (s *FSCacheWorker) TableName() string {
	return "fs_cache_worker"
}
