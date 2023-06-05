/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type ResourcePoolStore struct {
	db *gorm.DB
}

func newResourcePoolStore(db *gorm.DB) *ResourcePoolStore {
	return &ResourcePoolStore{db: db}
}

func (rps *ResourcePoolStore) Create(rp *model.ResourcePool) error {
	log.Debugf("begin create resource pool, name: %s", rp.Name)
	if rp.ID == "" {
		rp.ID = uuid.GenerateID(common.PrefixResourcePool)
	}
	return rps.db.Create(rp).Error
}

func (rps *ResourcePoolStore) Get(name string) (model.ResourcePool, error) {
	log.Debugf("begin get resource pool, name: %s", name)

	var rPool model.ResourcePool
	tx := rps.db.Where(&model.ResourcePool{Name: name}).Where("deleted_at = ''").First(&rPool)
	if tx.Error != nil {
		log.Errorf("get resource pool failed. name: %s, error:%s", name, tx.Error.Error())
		return model.ResourcePool{}, tx.Error
	}
	return rPool, nil
}

func (rps *ResourcePoolStore) List(pk int64, maxKeys int, filters map[string]string) ([]model.ResourcePool, error) {
	log.Debugf("list resource pools, pk: %d, maxKeys: %d, filters: %#v", pk, maxKeys, filters)
	tx := rps.db.Model(&model.ResourcePool{}).Where("deleted_at = ''").Where("pk > ?", pk)
	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	// TODO: add more filters
	var rpList []model.ResourcePool
	tx = tx.Find(&rpList)
	if tx.Error != nil {
		return []model.ResourcePool{}, tx.Error
	}
	return rpList, nil
}

func (rps *ResourcePoolStore) Update(rp *model.ResourcePool) error {
	log.Debugf("update resource pool, name: %s, value: %#v", rp.Name, rp)
	tx := rps.db.Model(rp).Where("id = ?", rp.ID).Updates(rp)
	return tx.Error
}

func (rps *ResourcePoolStore) Delete(name string) error {
	log.Debugf("delete resource pool, name: %s", name)
	t := rps.db.Model(&model.ResourcePool{}).Where("name = ?", name).
		Where("deleted_at = ''").
		UpdateColumn("deleted_at", time.Now().Format(model.TimeFormat))
	return t.Error
}
