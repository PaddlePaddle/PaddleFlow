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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
)

const FsCacheTableName = "fs_cache"

type DBFSCache struct {
	db *gorm.DB
}

func newDBFSCache() FSCacheStore {
	n := new(DBFSCache)
	n.db = database.DB
	return n
}

func (s *FSCache) TableName() string {
	return FsCacheTableName
}

func (f *DBFSCache) Add(value *FSCache) error {
	return f.db.Create(value).Error
}

func (f *DBFSCache) Get(fsID string, cacheID string) (*FSCache, error) {
	var fsCache FSCache
	tx := f.db.Where(&FSCache{FsID: fsID, CacheID: cacheID}).First(&fsCache)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return &fsCache, nil
}

func (f *DBFSCache) Delete(fsID, cacheID string) error {
	result := f.db
	if fsID != "" {
		result.Where(fmt.Sprintf(QueryEqualWithParam, FsID), fsID)
	}
	if cacheID != "" {
		result.Where(fmt.Sprintf(QueryEqualWithParam, FsCacheID), cacheID)
	}
	// todo:// change to soft delete , update deleteAt = xx
	return result.Delete(&FSCache{}).Error
}

func (f *DBFSCache) List(fsID, cacheID string) ([]FSCache, error) {
	if fsID != "" {
		f.db.Where(fmt.Sprintf(QueryEqualWithParam, FsID), fsID)
	}
	if cacheID != "" {
		f.db.Where(fmt.Sprintf(QueryEqualWithParam, FsCacheID), cacheID)
	}
	var fsCaches []FSCache
	err := f.db.Find(&fsCaches).Error
	if err != nil {
		return nil, err
	}
	return fsCaches, nil
}

func (f *DBFSCache) Update(value *FSCache) (int64, error) {
	result := f.db.Where(&FSCache{FsID: value.FsID, CacheID: value.CacheID}).Updates(value)
	return result.RowsAffected, result.Error
}
