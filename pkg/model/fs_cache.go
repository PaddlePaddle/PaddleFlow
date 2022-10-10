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
	"crypto/md5"
	"encoding/hex"
	"time"

	"gorm.io/gorm"
)

const FsCacheTableName = "fs_cache"

type FSCache struct {
	PK          int64          `json:"-" gorm:"primaryKey;autoIncrement"`
	CacheID     string         `json:"cacheID" gorm:"type:varchar(36);column:cache_id"`
	CacheHashID string         `json:"cacheHashID" gorm:"type:varchar(36);column:cache_hash_id"`
	FsID        string         `json:"fsID" gorm:"type:varchar(36);column:fs_id"`
	CacheDir    string         `json:"cacheDir" gorm:"type:varchar(4096);column:cache_dir"`
	NodeName    string         `json:"nodename" gorm:"type:varchar(256);column:nodename"`
	UsedSize    int            `json:"usedSize" gorm:"type:bigint(20);column:usedsize"`
	ClusterID   string         `json:"-"   gorm:"column:cluster_id;default:''"`
	CreatedAt   time.Time      `json:"-"`
	UpdatedAt   time.Time      `json:"-"`
	DeletedAt   gorm.DeletedAt `json:"-"`
}

func (c *FSCache) TableName() string {
	return FsCacheTableName
}

func (c *FSCache) BeforeSave(*gorm.DB) error {
	if c.CacheID == "" {
		c.CacheID = CacheID(c.ClusterID, c.NodeName, c.CacheDir, c.FsID)
	}
	return nil
}

func CacheID(clusterID, nodeName, CacheDir, fsID string) string {
	hash := md5.Sum([]byte(clusterID + nodeName + CacheDir + fsID))
	return hex.EncodeToString(hash[:])
}
