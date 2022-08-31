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
	"encoding/json"
	"fmt"

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
	return tx.Delete(&model.FileSystem{Model: model.Model{ID: id}}).Error
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

// DeleteLinkWithFsID delete all filesystem links associated with fsID
func (fss *FilesystemStore) DeleteLinkWithFsID(tx *gorm.DB, fsID string) error {
	if tx == nil {
		tx = fss.db
	}
	return tx.Where(fmt.Sprintf(QueryEqualWithParam, FsID), fsID).Delete(&model.Link{}).Error
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

func (fss *FilesystemStore) CreateFSCacheConfig(fsCacheConfig *model.FSCacheConfig) error {
	fmt.Println("create fs cscahe", *fsCacheConfig)
	err := fss.db.Model(&model.FSCacheConfig{}).Create(fsCacheConfig).Error
	if err != nil {
		return err
	}
	return nil
}

func (fss *FilesystemStore) UpdateFSCacheConfig(fsCacheConfig *model.FSCacheConfig) error {
	nodeAffinityMap, err := json.Marshal(&fsCacheConfig.NodeAffinity)
	if err != nil {
		return err
	}
	fsCacheConfig.NodeAffinityJson = string(nodeAffinityMap)

	nodeTaintMap, err := json.Marshal(&fsCacheConfig.NodeTaintTolerationMap)
	if err != nil {
		return err
	}
	fsCacheConfig.NodeTaintTolerationJson = string(nodeTaintMap)

	extraConfigMap, err := json.Marshal(&fsCacheConfig.ExtraConfigMap)
	if err != nil {
		return err
	}
	fsCacheConfig.ExtraConfigJson = string(extraConfigMap)
	tx := fss.db.Model(&model.FSCacheConfig{}).Where(&model.FSCacheConfig{FsID: fsCacheConfig.FsID}).Updates(fsCacheConfig)
	if tx.Error != nil {
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

func (fss *FilesystemStore) GetFSCacheConfig(fsID string) (model.FSCacheConfig, error) {
	var fsCacheConfig model.FSCacheConfig
	tx := fss.db.Model(&model.FSCacheConfig{}).Where(&model.FSCacheConfig{FsID: fsID}).First(&fsCacheConfig)
	if tx.Error != nil {
		return model.FSCacheConfig{}, tx.Error
	}
	return fsCacheConfig, nil
}

func (fss *FilesystemStore) ListFSCacheConfig(fsIDs []string) ([]model.FSCacheConfig, error) {
	var fsCacheConfigs []model.FSCacheConfig
	tx := fss.db.Model(&model.FSCacheConfig{}).Where("fs_id in ?", fsIDs).Find(&fsCacheConfigs)
	return fsCacheConfigs, tx.Error
}
