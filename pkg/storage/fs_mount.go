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
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"gorm.io/gorm"
)

type FsMountStoreInterface interface {
	AddMount(value *model.FsMount) error
	GetMount(where *model.FsMount) (*model.FsMount, error)
	GetMountWithDelete(where *model.FsMount) (*model.FsMount, error)
	UpdateMount(fsMount *model.FsMount) error
	ListMount(where *model.FsMount, limit int, marker string) (fsMounts []model.FsMount, err error)
	DeleteMount(mountID string) error
	ListMountNodesByID(fsID string) ([]string, error)
}

type FsMountStorage struct {
	db *gorm.DB
}

func NewFsMountStore(db *gorm.DB) *FsMountStorage {
	return &FsMountStorage{db: db}
}

func (fms *FsMountStorage) AddMount(value *model.FsMount) error {
	return fms.db.Create(value).Error
}

func (fms *FsMountStorage) GetMount(where *model.FsMount) (*model.FsMount, error) {
	fsMount := &model.FsMount{}
	tx := fms.db.Where(where).First(fsMount)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return fsMount, nil
}

func (fms *FsMountStorage) GetMountWithDelete(where *model.FsMount) (*model.FsMount, error) {
	fsMount := &model.FsMount{}
	tx := fms.db.Unscoped().Model(&model.FsMount{}).Where(where).First(fsMount)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return fsMount, nil
}

func (fms *FsMountStorage) UpdateMount(fsMount *model.FsMount) error {
	tx := fms.db.Model(&model.FsMount{}).Where(&model.FsMount{MountID: fsMount.MountID}).Updates(*fsMount)
	if tx.Error != nil {
		return tx.Error
	}
	return nil
}

func (fms *FsMountStorage) ListMount(where *model.FsMount, limit int, marker string) (fsMounts []model.FsMount, err error) {
	tx := fms.db.Where(where).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
		Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&fsMounts)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return fsMounts, nil
}

func (fms *FsMountStorage) DeleteMount(mountID string) error {
	tx := fms.db.Where(&model.FsMount{MountID: mountID}).Delete(&model.FsMount{})
	return tx.Error
}

func (fms *FsMountStorage) ListMountNodesByID(fsID string) ([]string, error) {
	nodenames := make([]string, 0)
	tx := fms.db.Model(&model.FsMount{}).Where(&model.FsMount{FsID: fsID}).Distinct("nodename").Select("nodename").Find(&nodenames)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return nodenames, nil
}
