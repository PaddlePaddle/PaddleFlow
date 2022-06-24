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
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
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

func (c *RunCache) decode() {
	// format time
	c.CreateTime = c.CreatedAt.Format("2006-01-02 15:04:05")
	c.UpdateTime = c.UpdatedAt.Format("2006-01-02 15:04:05")
}

func CreateRunCache(logEntry *log.Entry, cache *RunCache) (string, error) {
	logEntry.Debugf("begin create cache:%+v", cache)
	err := WithTransaction(storage.DB, func(tx *gorm.DB) error {
		result := tx.Model(&RunCache{}).Create(cache)
		if result.Error != nil {
			logEntry.Errorf("create cache failed. cache:%v, error:%v", cache, result.Error)
			return result.Error
		}
		cache.ID = common.PrefixCache + fmt.Sprintf("%06d", cache.Pk)
		logEntry.Debugf("created cache with pk[%d], cacheID[%s]", cache.Pk, cache.ID)
		// update ID by pk
		result = tx.Model(&RunCache{}).Where("pk = ?", cache.Pk).Update("id", cache.ID)
		if result.Error != nil {
			logEntry.Errorf("back filling cacheID failed. pk[%d], error:%v", cache.Pk, result.Error)
			return result.Error
		}
		return nil
	})
	return cache.ID, err
}

func ListRunCacheByFirstFp(logEntry *log.Entry, firstFp, fsID, step, source string) ([]RunCache, error) {
	var cacheList []RunCache
	tx := storage.DB.Model(&RunCache{}).Where(
		"first_fp = ? and fs_id = ? and step = ? and source = ?",
		firstFp, fsID, step, source).Order("created_at DESC").Find(&cacheList)
	if tx.Error != nil {
		logEntry.Errorf("ListRunCacheByFirstFp failed. firstFp[%s] fsID[%s] step[%s] source[%s]. error:%v",
			firstFp, fsID, step, source, tx.Error)
		return nil, tx.Error
	}
	return cacheList, nil
}

func UpdateCache(logEntry *log.Entry, cacheID string, cache RunCache) error {
	logEntry.Debugf("begin update cache. cacheID:%s, new cache:%v", cache.ID, cache)
	tx := storage.DB.Model(&RunCache{}).Where("id = ?", cacheID).Updates(cache)
	if tx.Error != nil {
		logEntry.Errorf("update cache status failed. cacheID:%s, error:%v", cacheID, tx.Error)
		return tx.Error
	}
	return nil
}

func GetRunCache(logEntry *log.Entry, cacheID string) (RunCache, error) {
	logEntry.Debugf("begin get cache. cacheID:%s", cacheID)
	var cache RunCache
	tx := storage.DB.Model(&RunCache{}).Where("id = ?", cacheID).First(&cache)
	if tx.Error != nil {
		logEntry.Errorf("get cache failed. cacheID:%s, error:%v", cacheID, tx.Error)
		return RunCache{}, tx.Error
	}
	cache.decode()
	return cache, nil
}

func DeleteRunCache(logEntry *log.Entry, cacheID string) error {
	logEntry.Debugf("begin delete cache. cacheID:%s", cacheID)
	tx := storage.DB.Model(&RunCache{}).Unscoped().Where("id = ?", cacheID).Delete(&RunCache{})
	if tx.Error != nil {
		logEntry.Errorf("delete cache failed. cacheID:%s, error:%v", cacheID, tx.Error)
		return tx.Error
	}
	return nil
}

func ListRunCache(logEntry *log.Entry, pk int64, maxKeys int, userFilter, fsFilter, runFilter []string) ([]RunCache, error) {
	logEntry.Debugf("begin list cache")
	tx := storage.DB.Model(&RunCache{}).Where("pk > ?", pk)
	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
	}
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}
	if len(runFilter) > 0 {
		tx = tx.Where("run_id IN (?)", runFilter)
	}
	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var cacheList []RunCache
	tx = tx.Find(&cacheList)
	if tx.Error != nil {
		logEntry.Errorf("list cache failed. Filters: user{%v}, fs{%v}, run{%v}. error:%v",
			userFilter, fsFilter, runFilter, tx.Error)
		return []RunCache{}, tx.Error
	}
	for index, _ := range cacheList {
		cacheList[index].decode()
	}
	return cacheList, nil
}

func GetLastCacheForRun(logEntry *log.Entry, runID string) (RunCache, error) {
	logEntry.Debugf("get last cache for run:%s", runID)
	cache := RunCache{}
	tx := storage.DB.Model(&RunCache{}).Where(&RunCache{RunID: runID}).Last(&cache)
	if tx.Error != nil {
		logEntry.Errorf("get last cache for run:%s failed. error:%v", runID, tx.Error)
		return RunCache{}, tx.Error
	}
	return cache, nil
}

func GetLastRunCache(logEntry *log.Entry) (RunCache, error) {
	logEntry.Debugf("get last runCache")
	runCache := RunCache{}
	tx := storage.DB.Model(&RunCache{}).Last(&runCache)
	if tx.Error != nil {
		logEntry.Errorf("get last runCache failed. error:%s", tx.Error.Error())
		return RunCache{}, tx.Error
	}
	return runCache, nil
}

func GetCacheCount(logEntry *log.Entry, runID string) (int64, error) {
	logEntry.Debugf("get cache count. runID: %s", runID)
	var count int64
	query := storage.DB.Model(&RunCache{})
	if runID != "" {
		query = query.Where(&RunCache{RunID: runID})
	}
	tx := query.Count(&count)
	if tx.Error != nil {
		logEntry.Errorf("get cache count failed. error:%s", tx.Error.Error())
		return 0, tx.Error
	}
	return count, nil
}
