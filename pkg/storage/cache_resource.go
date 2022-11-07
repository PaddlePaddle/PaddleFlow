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

type ClusterResourceCache struct {
	dbCache *gorm.DB
}

var (
	trInfo = &model.ResourceInfo{}
)

func newClusterResourceCache(db *gorm.DB) *ClusterResourceCache {
	return &ClusterResourceCache{dbCache: db}
}

func (nc *ClusterResourceCache) Table() *gorm.DB {
	return nc.dbCache.Table(trInfo.TableName())
}

func (nc *ClusterResourceCache) AddResource(rInfo *model.ResourceInfo) error {
	log.Debugf("begin to add pod resources, pod id:%s, name:%s", rInfo.PodID, rInfo.Name)
	tx := nc.Table().Create(rInfo)
	if tx.Error != nil {
		log.Errorf("add pod resources failed, pod id: %s, error:%s", rInfo.PodID, tx.Error)
		return tx.Error
	}
	return nil
}

func (nc *ClusterResourceCache) BatchAddResource(rInfo []model.ResourceInfo) error {
	log.Debugf("begin to batch add %d pod resources, info: %v", len(rInfo), rInfo)
	tx := nc.Table().Create(rInfo)
	if tx.Error != nil {
		log.Errorf("batch add pod resources failed, error:%s", tx.Error)
		return tx.Error
	}
	return nil
}

func (nc *ClusterResourceCache) DeleteResource(podID string) error {
	log.Infof("begin to delete pod resources. pod id:%s", podID)
	rInfo := &model.ResourceInfo{}
	tx := nc.Table().Unscoped().Where("pod_id = ?", podID).Delete(rInfo)
	if tx.Error != nil && errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		log.Errorf("delete pod resources failed. pod id:%s, error:%s", podID, tx.Error)
		return tx.Error
	}
	return nil
}

func (nc *ClusterResourceCache) UpdateResource(podID string, rName string, podInfo *model.ResourceInfo) error {
	log.Debugf("begin to update pod resource. pod id:%s", podID)
	tx := nc.Table().Where("pod_id = ? AND resource_name = ?", podID, rName).Updates(podInfo)
	if tx.Error != nil {
		log.Errorf("update pod resource failed. pod id:%s, error:%s", podID, tx.Error)
		return tx.Error
	}
	return nil
}
