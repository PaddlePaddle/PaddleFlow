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
	"errors"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type ObjectLabelCache struct {
	dbCache *gorm.DB
}

var labelInfo = &model.LabelInfo{}

func newLabelCache(db *gorm.DB) *ObjectLabelCache {
	return &ObjectLabelCache{dbCache: db}
}

func (lc *ObjectLabelCache) Table() *gorm.DB {
	return lc.dbCache.Table(labelInfo.TableName())
}

func (lc *ObjectLabelCache) AddLabel(lInfo *model.LabelInfo) error {
	log.Debugf("begin to add labels, info: %v", lInfo)
	tx := lc.Table().Create(lInfo)
	if tx.Error != nil {
		log.Errorf("add node failed, label name: %s, error:%s", lInfo.Name, tx.Error)
		return tx.Error
	}
	return nil
}

func (lc *ObjectLabelCache) DeleteLabel(objID, objType string) error {
	log.Infof("begin to delete labels. object id:%s, and type: %s", objID, objType)

	lInfo := &model.LabelInfo{}
	tx := lc.Table().Unscoped().Where("object_id = ? AND object_type = ?", objID, objType).Delete(lInfo)
	if tx.Error != nil && errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		log.Errorf("delete labels failed. object id:%s, error:%s", objID, tx.Error)
		return tx.Error
	}
	return nil
}
