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

package db_service

import (
	"fmt"

	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/models"
)

// CreateLink creates a new link
func CreateLink(link *models.Link) error {
	return database.DB.Model(&models.Link{}).Create(link).Error
}

func FsNameLinks(fsID string) ([]models.Link, error) {
	var links []models.Link
	result := &gorm.DB{}
	result = database.DB.Model(&models.Link{}).Where(&models.Link{FsID: fsID}).Find(&links)
	return links, result.Error
}

func LinkWithFsIDAndFsPath(fsID, fsPath string) (models.Link, error) {
	var link models.Link
	result := &gorm.DB{}
	result = database.DB.Model(&models.Link{}).Where(&models.Link{FsID: fsID, FsPath: fsPath}).Find(&link)
	return link, result.Error
}

// DeleteLinkWithFsIDAndFsPath delete a file system link
func DeleteLinkWithFsIDAndFsPath(fsID, fsPath string) error {
	tx := database.DB.Model(&models.Link{}).Where(fmt.Sprintf(QueryEqualWithParam, FsID), fsID).Where(fmt.Sprintf(QueryEqualWithParam, FsPath), fsPath)
	return tx.Delete(&models.Link{}).Error
}

// ListLink get links with marker and limit sort by create_at desc
func ListLink(limit int, marker, fsID string) ([]models.Link, error) {
	var links []models.Link
	result := &gorm.DB{}
	result = database.DB.Model(&models.Link{}).Where(&models.Link{FsID: fsID}).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
		Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&links)
	return links, result.Error
}

func GetLinkWithFsIDAndPath(fsID, fsPath string) ([]models.Link, error) {
	var links []models.Link
	result := database.DB.Model(&models.Link{}).Where(&models.Link{FsID: fsID, FsPath: fsPath}).First(&links)
	return links, result.Error
}
