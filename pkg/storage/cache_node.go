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

type ClusterNodeCache struct {
	dbCache *gorm.DB
}

var (
	nInfo     = model.NodeInfo{}
	labelInfo = model.LabelInfo{}
)

func newClusterNodeCache(db *gorm.DB) *ClusterNodeCache {
	return &ClusterNodeCache{dbCache: db}
}

func (nc *ClusterNodeCache) Table() *gorm.DB {
	return nc.dbCache.Table(nInfo.TableName())
}

func (nc *ClusterNodeCache) GetNode(nodeID string) (model.NodeInfo, error) {
	log.Debugf("begin to get node. node id: %s", nodeID)

	var clusterNodeInfo model.NodeInfo
	tx := nc.Table().Where("id = ?", nodeID).First(&clusterNodeInfo)
	if tx.Error != nil {
		log.Errorf("get node failed. node id: %s, error:%s",
			nodeID, tx.Error)
		return model.NodeInfo{}, tx.Error
	}
	return clusterNodeInfo, nil
}

func (nc *ClusterNodeCache) AddNode(nodeInfo *model.NodeInfo) error {
	log.Debugf("begin to add node, node id:%s, name:%s", nodeInfo.ID, nodeInfo.Name)
	tx := nc.Table().Create(nodeInfo)
	if tx.Error != nil {
		log.Errorf("add node failed, node name: %s, error:%s",
			nodeInfo.Name, tx.Error)
		return tx.Error
	}
	return nil
}

func (nc *ClusterNodeCache) DeleteNode(nodeID string) error {
	log.Infof("begin to delete node. node id:%s", nodeID)
	nodeInfo := &model.NodeInfo{}
	tx := nc.Table().Unscoped().Where("id = ?", nodeID).Delete(nodeInfo)
	if tx.Error != nil && errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		log.Errorf("delete node failed. node id:%s, error:%s",
			nodeID, tx.Error)
		return tx.Error
	}
	return nil
}

func (nc *ClusterNodeCache) UpdateNode(nodeID string, nodeInfo *model.NodeInfo) error {
	log.Debugf("begin to update node. node id:%s", nodeID)
	tx := nc.Table().Where("id = ?", nodeID).Updates(nodeInfo)
	if tx.Error != nil {
		log.Errorf("update node failed. node id:%s, error:%s",
			nodeID, tx.Error)
		return tx.Error
	}
	return nil
}

type ObjectLabelCache struct {
	dbCache *gorm.DB
}

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
