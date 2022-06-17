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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/models"
)

func Add(value *models.FsMount) error {
	return database.DB.Model(&models.FsMount{}).Create(value).Error
}

func GetMount(where *models.FsMount) (*models.FsMount, error) {
	fsMount := &models.FsMount{}
	tx := database.DB.Model(&models.FsMount{}).Where(where).First(fsMount)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return fsMount, nil
}

func GetMountWithDelete(where *models.FsMount) (*models.FsMount, error) {
	fsMount := &models.FsMount{}
	tx := database.DB.Model(&models.FsMount{}).Unscoped().Model(&models.FsMount{}).Where(where).First(fsMount)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return fsMount, nil
}

func UpdateMount(fsMount *models.FsMount) error {
	tx := database.DB.Model(&models.FsMount{}).Where(&models.FsMount{MountID: fsMount.MountID}).Updates(*fsMount)
	if tx.Error != nil {
		return tx.Error
	}
	return nil
}

func ListMount(where *models.FsMount, limit int, marker string) (fsMounts []models.FsMount, err error) {
	tx := database.DB.Model(&models.FsMount{}).Where(where).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
		Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&fsMounts)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return fsMounts, nil
}

func DeleteMount(mountID string) error {
	tx := database.DB.Model(&models.FsMount{}).Where(&models.FsMount{MountID: mountID}).Delete(&models.FsMount{})
	return tx.Error
}

func ListMountNodesByID(fsID string) ([]string, error) {
	nodenames := make([]string, 0)
	tx := database.DB.Model(&models.FsMount{}).Where(&models.FsMount{FsID: fsID}).Distinct("nodename").Select("nodename").Find(&nodenames)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return nodenames, nil
}
