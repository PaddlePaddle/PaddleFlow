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
	"sync"
	"time"

	"gorm.io/gorm"
)

type FSCache struct {
	PK          int64          `json:"-" gorm:"primaryKey;autoIncrement"`
	CacheID     string         `json:"cacheID" gorm:"type:varchar(36);column:cache_id"`
	CacheHashID string         `json:"cacheHashID" gorm:"type:varchar(36);column:cache_hash_id"`
	FSID        string         `json:"fsID" gorm:"type:varchar(36);column:fs_id"`
	CacheDir    string         `json:"cacheDir" gorm:"type:varchar(4096);column:cache_dir"`
	NodeName    string         `json:"nodename" gorm:"type:varchar(256);column:nodename"`
	MountPoint  string         `json:"mountpoint" gorm:"type:varchar(1024);column:mountpoint"`
	UsedSize    int            `json:"usedSize" gorm:"type:bigint(20);column:usedsize"`
	CreatedAt   time.Time      `json:"-"`
	UpdatedAt   time.Time      `json:"-"`
	DeletedAt   gorm.DeletedAt `json:"-"`
}

type FSCacheStore interface {
	AddFSCache(value *FSCache) error
	GetFSCache(fsID string, cacheID string) (*FSCache, error)
	DeleteFSCache(fsID, cacheID string) error
	ListFSCaches(fsID, cacheID string) ([]FSCache, error)
	UpdateFSCache() error
}

var instance FSCacheStore
var once sync.Once

func GetFSCacheStore() FSCacheStore {
	once.Do(func() {
		// default use db storage, mem used in the future maybe as the cache for db
		instance = newDBFSCache()
	})
	return instance
}
