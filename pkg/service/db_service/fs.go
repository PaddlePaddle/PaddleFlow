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

package db_service

import (
	"fmt"

	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/models"
)

func CreatFileSystem(fs *models.FileSystem) error {
	return database.DB.Model(&models.FileSystem{}).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(fs).Error; err != nil {
			return err
		}
		return nil
	})
}

func GetFileSystemWithFsID(fsID string) (models.FileSystem, error) {
	var fileSystem models.FileSystem
	result := database.DB.Model(&models.FileSystem{}).Where(&models.FileSystem{Model: models.Model{ID: fsID}}).First(&fileSystem)
	return fileSystem, result.Error
}

func GetFileSystemWithFsIDAndUserName(fsID, userName string) (models.FileSystem, error) {
	var fileSystem models.FileSystem
	result := database.DB.Model(&models.FileSystem{}).Where(&models.FileSystem{Model: models.Model{ID: fsID}, UserName: userName}).Find(&fileSystem)
	return fileSystem, result.Error
}

func DeleteFileSystem(tx *gorm.DB, id string) error {
	if tx == nil {
		tx = database.DB
	}
	return tx.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(&models.FileSystem{Model: models.Model{ID: id}}).Error; err != nil {
			return err
		}

		if err := tx.Where(fmt.Sprintf(QueryEqualWithParam, FsID), id).Delete(&models.Link{}).Error; err != nil {
			return err
		}
		return nil
	})
}

// ListFileSystem get file systems with marker and limit sort by create_at desc
func ListFileSystem(limit int, userName, marker, fsName string) ([]models.FileSystem, error) {
	var fileSystems []models.FileSystem
	tx := database.DB.Model(&models.FileSystem{})
	if fsName == "" {
		tx = tx.Where(&models.FileSystem{UserName: userName}).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
			Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&fileSystems)
	} else {
		tx = tx.Where(&models.FileSystem{UserName: userName, Name: fsName}).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
			Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&fileSystems)
	}
	return fileSystems, tx.Error
}

// GetFsWithIDs get file system detail from ids
func GetFsWithIDs(fsID []string) ([]models.FileSystem, error) {
	var fileSystems []models.FileSystem
	result := database.DB.Model(&models.FileSystem{}).Where(fmt.Sprintf(QueryInWithParam, ID), fsID).Find(&fileSystems)
	return fileSystems, result.Error
}

// GetFsWithNameAndUserName get file system detail from name and userID
func GetFsWithNameAndUserName(fsName, userName string) (models.FileSystem, error) {
	var fileSystem models.FileSystem
	result := database.DB.Model(&models.FileSystem{}).Where(&models.FileSystem{UserName: userName, Name: fsName}).Find(&fileSystem)
	return fileSystem, result.Error
}

// GetSimilarityAddressList find fs where have same type and serverAddress
func GetSimilarityAddressList(fsType string, ips []string) ([]models.FileSystem, error) {
	var fileSystems []models.FileSystem
	// local has no ip
	if len(ips) == 0 {
		result := database.DB.Model(&models.FileSystem{}).Where(fmt.Sprintf("%s = ?", Type), fsType).Find(&fileSystems)
		return fileSystems, result.Error
	}

	tx := database.DB.Model(&models.FileSystem{})
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
