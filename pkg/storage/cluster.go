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
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type ClusterStore struct {
	db *gorm.DB
}

func newClusterStore(db *gorm.DB) *ClusterStore {
	return &ClusterStore{db: db}
}

func generateDeletedUuidStr() string {
	return uuid.GenerateIDWithLength("deleted", 24)
}

func (cs *ClusterStore) CreateCluster(clusterInfo *model.ClusterInfo) error {
	log.Debugf("begin create cluster, cluster name:%s", clusterInfo.Name)
	tx := cs.db.Table("cluster_info").Create(clusterInfo)
	if tx.Error != nil {
		log.Errorf("create cluster failed. queue:%v, error:%s",
			clusterInfo.Name, tx.Error.Error())
		return tx.Error
	}

	return nil
}

func (cs *ClusterStore) ListCluster(pk int64, maxKeys int, clusterNameList []string, clusterStatus string) ([]model.ClusterInfo, error) {
	log.Debugf("list cluster, pk: %d, maxKeys: %d", pk, maxKeys)

	var clusterList []model.ClusterInfo
	query := cs.db.Table("cluster_info").Where("deleted_at = '' AND pk > ?", pk)

	if len(clusterNameList) > 0 {
		query = query.Where(" name in ?", clusterNameList)
	}
	if clusterStatus != "" {
		query = query.Where(" status = ?", clusterStatus)
	}
	if maxKeys > 0 {
		query = query.Limit(maxKeys)
	}

	err := query.Find(&clusterList).Error
	if err != nil {
		log.Errorf("list cluster failed. error : %s ", err.Error())
		return nil, err
	}

	return clusterList, nil
}

func (cs *ClusterStore) GetLastCluster() (model.ClusterInfo, error) {
	log.Debugf("model get last cluster. ")

	clusterInfo := model.ClusterInfo{}
	tx := cs.db.Table("cluster_info").Where("deleted_at = ''").Last(&clusterInfo)
	if tx.Error != nil {
		log.Errorf("get last cluster failed. error:%s", tx.Error.Error())
		return model.ClusterInfo{}, tx.Error
	}
	return clusterInfo, nil
}

func (cs *ClusterStore) GetClusterByName(clusterName string) (model.ClusterInfo, error) {
	log.Debugf("start to get cluster. clusterName: %s", clusterName)

	var clusterInfo model.ClusterInfo
	tx := cs.db.Table("cluster_info").Where("name = ? AND deleted_at = ''", clusterName)
	tx = tx.First(&clusterInfo)

	if tx.Error != nil {
		log.Errorf("get cluster failed. clusterName: %s, error:%s",
			clusterName, tx.Error.Error())
		return model.ClusterInfo{}, tx.Error
	}

	return clusterInfo, nil
}

func (cs *ClusterStore) GetClusterById(clusterId string) (model.ClusterInfo, error) {
	log.Debugf("start to get cluster. clusterId: %s", clusterId)

	var clusterInfo model.ClusterInfo
	tx := cs.db.Table("cluster_info").Where("id = ? AND deleted_at = '' ", clusterId).First(&clusterInfo)
	if tx.Error != nil {
		log.Errorf("get cluster failed. clusterId: %s, error:%s",
			clusterId, tx.Error.Error())
		return model.ClusterInfo{}, tx.Error
	}

	return clusterInfo, nil
}

func (cs *ClusterStore) DeleteCluster(clusterName string) error {
	log.Infof("start to delete cluster. clusterName:%s", clusterName)

	// 检查clusterName是否存在
	clusterInfo, err := cs.GetClusterByName(clusterName)
	if err != nil {
		return err
	}
	// 更新 DeletedAt 字段为uuid字符串，表示逻辑删除
	clusterInfo.DeletedAt = generateDeletedUuidStr()
	clusterInfo.UpdatedAt = time.Now()
	err = cs.UpdateCluster(clusterInfo.ID, &clusterInfo)
	if err != nil {
		log.Errorf("delete cluster failed. clusterName:%s, error:%s",
			clusterName, err.Error())
		return err
	}
	return nil
}

func (cs *ClusterStore) UpdateCluster(clusterId string, clusterInfo *model.ClusterInfo) error {
	log.Debugf("start to update cluster. clusterId:%s", clusterId)
	err := cs.db.Table("cluster_info").Where("id = ?", clusterId).Updates(clusterInfo).Error
	if err != nil {
		log.Errorf("update cluster failed. clusterId:%s, error:%s",
			clusterId, err.Error())
		return err
	}
	return nil
}

func (cs *ClusterStore) ActiveClusters() []model.ClusterInfo {
	tx := cs.db.Table("cluster_info").Where("deleted_at = '' ")

	var clusterList []model.ClusterInfo
	err := tx.Find(&clusterList).Error
	if err != nil {
		return clusterList
	}
	return clusterList
}
