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

package storage

import (
	"fmt"

	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func newDBFSCache(db *gorm.DB) FsCacheStoreInterface {
	n := new(DBFSCache)
	n.db = db
	return n
}

type DBFSCache struct {
	db *gorm.DB
}

func (f *DBFSCache) Add(value *model.FSCache) error {
	return f.db.Create(value).Error
}

func (f *DBFSCache) Get(fsID string, cacheID string) (*model.FSCache, error) {
	var fsCache model.FSCache
	tx := f.db.Where(&model.FSCache{FsID: fsID, CacheID: cacheID}).First(&fsCache)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return &fsCache, nil
}

func (f *DBFSCache) Delete(fsID, cacheID string) error {
	tx := f.db
	if fsID != "" {
		tx = tx.Where(fmt.Sprintf(QueryEqualWithParam, FsID), fsID)
	}
	if cacheID != "" {
		tx = tx.Where(fmt.Sprintf(QueryEqualWithParam, FsCacheID), cacheID)
	}
	return tx.Unscoped().Delete(&model.FSCache{}).Error
}

func (f *DBFSCache) List(fsID, cacheID string) ([]model.FSCache, error) {
	tx := f.db
	if fsID != "" {
		tx = tx.Where(fmt.Sprintf(QueryEqualWithParam, FsID), fsID)
	}
	if cacheID != "" {
		tx = tx.Where(fmt.Sprintf(QueryEqualWithParam, FsCacheID), cacheID)
	}
	var fsCaches []model.FSCache
	err := tx.Find(&fsCaches).Error
	if err != nil {
		return nil, err
	}
	return fsCaches, nil
}

func (f *DBFSCache) ListNodes(fsIDs []string) ([]string, error) {
	nodeList := make([]string, 0)
	result := f.db.Model(&model.FSCache{}).Where(fmt.Sprintf(QueryInWithParam, FsID), fsIDs).Select(NodeName).Group(NodeName).Find(&nodeList)
	return nodeList, result.Error
}

func (f *DBFSCache) Update(value *model.FSCache) (int64, error) {
	result := f.db.Where(&model.FSCache{FsID: value.FsID, CacheID: value.CacheID}).Updates(value)
	return result.RowsAffected, result.Error
}
