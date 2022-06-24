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

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type FilesystemStore struct {
	db *gorm.DB
}

func newFilesystemStore(db *gorm.DB) *FilesystemStore {
	return &FilesystemStore{db: db}
}

// ============================================================= table filesystem ============================================================= //

func (fss *FilesystemStore) CreatFileSystem(fs *model.FileSystem) error {
	return fss.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(fs).Error; err != nil {
			return err
		}
		return nil
	})
}

func (fss *FilesystemStore) GetFileSystemWithFsID(fsID string) (model.FileSystem, error) {
	var fileSystem model.FileSystem
	result := fss.db.Where(&model.FileSystem{Model: model.Model{ID: fsID}}).First(&fileSystem)
	return fileSystem, result.Error
}

func (fss *FilesystemStore) DeleteFileSystem(tx *gorm.DB, id string) error {
	if tx == nil {
		tx = fss.db
	}
	return tx.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(&model.FileSystem{Model: model.Model{ID: id}}).Error; err != nil {
			return err
		}
		if err := tx.Where(fmt.Sprintf(QueryEqualWithParam, FsID), id).Delete(&model.Link{}).Error; err != nil {
			return err
		}
		return nil
	})
}

// ListFileSystem get file systems with marker and limit sort by create_at desc
func (fss *FilesystemStore) ListFileSystem(limit int, userName, marker, fsName string) ([]model.FileSystem, error) {
	var fileSystems []model.FileSystem
	result := fss.db.Where(&model.FileSystem{UserName: userName, Name: fsName}).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
		Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&fileSystems)
	return fileSystems, result.Error
}

// GetSimilarityAddressList find fs where have same type and serverAddress
func (fss *FilesystemStore) GetSimilarityAddressList(fsType string, ips []string) ([]model.FileSystem, error) {
	var fileSystems []model.FileSystem
	// local has no ip
	if len(ips) == 0 {
		result := fss.db.Where(fmt.Sprintf("%s = ?", Type), fsType).Find(&fileSystems)
		return fileSystems, result.Error
	}
	tx := fss.db
	for k, ip := range ips {
		if k == 0 {
			tx = tx.Where(fmt.Sprintf(QueryLikeWithParam, ServerAddress), fmt.Sprintf("%%%s%%", ip))
		} else {
			tx = tx.Or(fmt.Sprintf(QueryLikeWithParam, ServerAddress), fmt.Sprintf("%%%s%%", ip))
		}
	}
	result := tx.Where(fmt.Sprintf("%s = ?", Type), fsType).Find(&fileSystems)
	return fileSystems, result.Error
}

// ============================================================= table link ============================================================= //

func (fss *FilesystemStore) CreateLink(link *model.Link) error {
	return fss.db.Create(link).Error
}

func (fss *FilesystemStore) FsNameLinks(fsID string) ([]model.Link, error) {
	var links []model.Link
	result := fss.db.Where(&model.Link{FsID: fsID}).Find(&links)
	return links, result.Error
}

func (fss *FilesystemStore) LinkWithFsIDAndFsPath(fsID, fsPath string) (model.Link, error) {
	var link model.Link
	result := fss.db.Where(&model.Link{FsID: fsID, FsPath: fsPath}).Find(&link)
	return link, result.Error
}

// DeleteLinkWithFsIDAndFsPath delete a file system link
func (fss *FilesystemStore) DeleteLinkWithFsIDAndFsPath(fsID, fsPath string) error {
	result := fss.db.Where(fmt.Sprintf(QueryEqualWithParam, FsID), fsID).Where(fmt.Sprintf(QueryEqualWithParam, FsPath), fsPath).Delete(&model.Link{})
	return result.Error
}

// ListLink get links with marker and limit sort by create_at desc
func (fss *FilesystemStore) ListLink(limit int, marker, fsID string) ([]model.Link, error) {
	var links []model.Link
	result := fss.db.Where(&model.Link{FsID: fsID}).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
		Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&links)
	return links, result.Error
}

func (fss *FilesystemStore) GetLinkWithFsIDAndPath(fsID, fsPath string) ([]model.Link, error) {
	var links []model.Link
	result := fss.db.Where(&model.Link{FsID: fsID, FsPath: fsPath}).First(&links)
	return links, result.Error
}

// ============================================================= table fs_cache_config ============================================================= //

func (fss *FilesystemStore) CreateFSCacheConfig(logEntry *log.Entry, fsCacheConfig *model.FSCacheConfig) error {
	logEntry.Debugf("begin create fsCacheConfig:%+v", fsCacheConfig)
	err := fss.db.Model(&model.FSCacheConfig{}).Create(fsCacheConfig).Error
	if err != nil {
		logEntry.Errorf("create fsCacheConfig failed. fsCacheConfig:%v, error:%s",
			fsCacheConfig, err.Error())
		return err
	}
	return nil
}

func (fss *FilesystemStore) UpdateFSCacheConfig(logEntry *log.Entry, fsCacheConfig model.FSCacheConfig) error {
	logEntry.Debugf("begin update fsCacheConfig fsCacheConfig. fsID:%s", fsCacheConfig.FsID)
	tx := fss.db.Model(&model.FSCacheConfig{}).Where(&model.FSCacheConfig{FsID: fsCacheConfig.FsID}).Updates(fsCacheConfig)
	if tx.Error != nil {
		logEntry.Errorf("update fsCacheConfig failed. fsCacheConfig.ID:%s, error:%s",
			fsCacheConfig.FsID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func (fss *FilesystemStore) DeleteFSCacheConfig(tx *gorm.DB, fsID string) error {
	if tx == nil {
		tx = fss.db
	}
	return tx.Model(&model.FSCacheConfig{}).Unscoped().Where(&model.FSCacheConfig{FsID: fsID}).Delete(&model.FSCacheConfig{}).Error
}

func (fss *FilesystemStore) GetFSCacheConfig(logEntry *log.Entry, fsID string) (model.FSCacheConfig, error) {
	logEntry.Debugf("begin get fsCacheConfig. fsID:%s", fsID)
	var fsCacheConfig model.FSCacheConfig
	tx := fss.db.Model(&model.FSCacheConfig{}).Where(&model.FSCacheConfig{FsID: fsID}).First(&fsCacheConfig)
	if tx.Error != nil {
		logEntry.Errorf("get fsCacheConfig failed. fsID:%s, error:%s",
			fsID, tx.Error.Error())
		return model.FSCacheConfig{}, tx.Error
	}
	return fsCacheConfig, nil
}
