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

type FileSystemStoreInterface interface {
	CreatFileSystem(fs *model.FileSystem) error
	GetFileSystemWithFsID(fsID string) (model.FileSystem, error)
	DeleteFileSystem(tx *gorm.DB, id string) error
	ListFileSystem(limit int, userName, marker, fsName string) ([]model.FileSystem, error)
	GetSimilarityAddressList(fsType string, ips []string) ([]model.FileSystem, error)
}

type FileSystemStorage struct {
	db *gorm.DB
}

func NewFileSystemStore(db *gorm.DB) *FileSystemStorage {
	return &FileSystemStorage{db: db}
}

func (fss *FileSystemStorage) CreatFileSystem(fs *model.FileSystem) error {
	return fss.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(fs).Error; err != nil {
			return err
		}
		return nil
	})
}

func (fss *FileSystemStorage) GetFileSystemWithFsID(fsID string) (model.FileSystem, error) {
	var fileSystem model.FileSystem
	result := fss.db.Where(&model.FileSystem{Model: model.Model{ID: fsID}}).First(&fileSystem)
	return fileSystem, result.Error
}

func (fss *FileSystemStorage) DeleteFileSystem(tx *gorm.DB, id string) error {
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
func (fss *FileSystemStorage) ListFileSystem(limit int, userName, marker, fsName string) ([]model.FileSystem, error) {
	var fileSystems []model.FileSystem
	tx := fss.db
	if fsName == "" {
		tx = tx.Where(&model.FileSystem{UserName: userName}).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
			Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&fileSystems)
	} else {
		tx = tx.Where(&model.FileSystem{UserName: userName, Name: fsName}).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
			Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&fileSystems)
	}
	return fileSystems, tx.Error
}

// GetSimilarityAddressList find fs where have same type and serverAddress
func (fss *FileSystemStorage) GetSimilarityAddressList(fsType string, ips []string) ([]model.FileSystem, error) {
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
	tx = tx.Where(fmt.Sprintf("%s = ?", Type), fsType).Find(&fileSystems)

	return fileSystems, tx.Error
}
