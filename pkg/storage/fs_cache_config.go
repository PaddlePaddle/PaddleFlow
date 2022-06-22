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
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type FsCacheConfigStoreInterface interface {
	CreateFSCacheConfig(logEntry *log.Entry, fsCacheConfig *model.FSCacheConfig) error
	UpdateFSCacheConfig(logEntry *log.Entry, fsCacheConfig model.FSCacheConfig) error
	DeleteFSCacheConfig(tx *gorm.DB, fsID string) error
	GetFSCacheConfig(logEntry *log.Entry, fsID string) (model.FSCacheConfig, error)
}

type FsCacheConfigStorage struct {
	db *gorm.DB
}

func NewFsCacheConfigStore(db *gorm.DB) *FsCacheConfigStorage {
	return &FsCacheConfigStorage{db: db}
}

func (fccs *FsCacheConfigStorage) CreateFSCacheConfig(logEntry *log.Entry, fsCacheConfig *model.FSCacheConfig) error {
	logEntry.Debugf("begin create fsCacheConfig:%+v", fsCacheConfig)
	err := fccs.db.Model(&model.FSCacheConfig{}).Create(fsCacheConfig).Error
	if err != nil {
		logEntry.Errorf("create fsCacheConfig failed. fsCacheConfig:%v, error:%s",
			fsCacheConfig, err.Error())
		return err
	}
	return nil
}

func (fccs *FsCacheConfigStorage) UpdateFSCacheConfig(logEntry *log.Entry, fsCacheConfig model.FSCacheConfig) error {
	logEntry.Debugf("begin update fsCacheConfig fsCacheConfig. fsID:%s", fsCacheConfig.FsID)
	tx := fccs.db.Model(&model.FSCacheConfig{}).Where(&model.FSCacheConfig{FsID: fsCacheConfig.FsID}).Updates(fsCacheConfig)
	if tx.Error != nil {
		logEntry.Errorf("update fsCacheConfig failed. fsCacheConfig.ID:%s, error:%s",
			fsCacheConfig.FsID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func (fccs *FsCacheConfigStorage) DeleteFSCacheConfig(tx *gorm.DB, fsID string) error {
	if tx == nil {
		tx = fccs.db
	}
	return tx.Model(&model.FSCacheConfig{}).Unscoped().Where(&model.FSCacheConfig{FsID: fsID}).Delete(&model.FSCacheConfig{}).Error
}

func (fccs *FsCacheConfigStorage) GetFSCacheConfig(logEntry *log.Entry, fsID string) (model.FSCacheConfig, error) {
	logEntry.Debugf("begin get fsCacheConfig. fsID:%s", fsID)
	var fsCacheConfig model.FSCacheConfig
	tx := fccs.db.Model(&model.FSCacheConfig{}).Where(&model.FSCacheConfig{FsID: fsID}).First(&fsCacheConfig)
	if tx.Error != nil {
		logEntry.Errorf("get fsCacheConfig failed. fsID:%s, error:%s",
			fsID, tx.Error.Error())
		return model.FSCacheConfig{}, tx.Error
	}
	return fsCacheConfig, nil
}
