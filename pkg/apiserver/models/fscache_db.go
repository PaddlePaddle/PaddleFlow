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
	"fmt"

	"gorm.io/gorm"

	"paddleflow/pkg/common/database"
)

type DBFSCache struct {
	db *gorm.DB
}

func newDBFSCache() FSCacheStore {
	n := new(DBFSCache)
	n.db = database.DB
	return n
}

func (s *FSCache) TableName() string {
	return "fs_cache"
}

func (f *DBFSCache) AddFSCache(value *FSCache) error {
	return f.db.Create(value).Error
}

func (f *DBFSCache) GetFSCache(fsID string, cacheID string) (*FSCache, error) {
	var fsCache FSCache
	tx := database.DB.Table("fs_cache").Where("fs_id = ?", fsID).Where("cache_id = ?", cacheID).First(&fsCache)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return &fsCache, nil
}

func (f *DBFSCache) DeleteFSCache(fsID, cacheID string) error {
	result := f.db
	if fsID != "" {
		result.Where(fmt.Sprintf(QueryEqualWithParam, FsID), fsID)
	}
	if cacheID != "" {
		result.Where(fmt.Sprintf(QueryEqualWithParam, FSCacheID), cacheID)
	}
	// todo:// change to soft delete , update deleteAt = xx
	return result.Delete(&FSCache{}).Error
}

func (f *DBFSCache) ListFSCaches(fsID, cacheID string) ([]FSCache, error) {
	if fsID != "" {
		f.db.Where(fmt.Sprintf(QueryEqualWithParam, FsID), fsID)
	}
	if cacheID != "" {
		f.db.Where(fmt.Sprintf(QueryEqualWithParam, FSCacheID), cacheID)
	}
	var fsCaches []FSCache
	err := f.db.Table("fs_cache").Find(&fsCaches).Error
	if err != nil {
		return nil, err
	} else {
		return fsCaches, nil
	}
}

func (f *DBFSCache) UpdateFSCache() error {
	return nil
}
