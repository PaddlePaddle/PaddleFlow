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

package models

import (
	"encoding/json"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	DefaultClusterSource = "OnPremise"
	ClusterStatusOnLine  = "online"
	ClusterStatusOffLine = "offline"
	DefaultClusterStatus = ClusterStatusOnLine
)

type ClusterInfo struct {
	Model            `gorm:"embedded"  json:",inline"`
	Pk               int64    `gorm:"primaryKey;autoIncrement" json:"-"`      // 自增主键
	Name             string   `gorm:"column:name" json:"clusterName"`         // 集群名字
	Description      string   `gorm:"column:description" json:"description"`  // 集群描述
	Endpoint         string   `gorm:"column:endpoint" json:"endpoint"`        // 集群endpoint, 比如 http://10.11.11.47:8080
	Source           string   `gorm:"column:source" json:"source"`            // 来源, 比如 OnPremise （内部部署）、AWS、CCE
	ClusterType      string   `gorm:"column:cluster_type" json:"clusterType"` // 集群类型，比如Kubernetes/Local
	Version          string   `gorm:"column:version" json:"version"`          // 集群版本，比如v1.16
	Status           string   `gorm:"column:status" json:"status"`            // 集群状态，可选值为online, offline
	Credential       string   `gorm:"column:credential" json:"credential"`    // 用于存储集群的凭证信息，比如k8s的kube_config配置
	Setting          string   `gorm:"column:setting" json:"setting"`          // 存储额外配置信息
	RawNamespaceList string   `gorm:"column:namespace_list" json:"-"`         // 命名空间列表，json类型，如["ns1", "ns2"]
	NamespaceList    []string `gorm:"-" json:"namespaceList"`                 // 命名空间列表，json类型，如["ns1", "ns2"]
	DeletedAt        string   `gorm:"column:deleted_at" json:"-"`             // 删除标识，非空表示软删除
}

func (ClusterInfo) TableName() string {
	return "cluster_info"
}

func (clusterInfo ClusterInfo) MarshalJSON() ([]byte, error) {
	type Alias ClusterInfo
	return json.Marshal(&struct {
		*Alias
		CreatedAt string `json:"createTime"`
		UpdatedAt string `json:"updateTime"`
	}{
		CreatedAt: clusterInfo.CreatedAt.Format(TimeFormat),
		UpdatedAt: clusterInfo.UpdatedAt.Format(TimeFormat),
		Alias:     (*Alias)(&clusterInfo),
	})
}

func (clusterInfo *ClusterInfo) BeforeSave(*gorm.DB) error {
	if len(clusterInfo.NamespaceList) != 0 {
		namespaceList, err := json.Marshal(clusterInfo.NamespaceList)
		if err != nil {
			log.Errorf("json Marshal clusterInfo.NamespaceList[%v] failed: %v", clusterInfo.NamespaceList, err)
			return err
		}
		clusterInfo.RawNamespaceList = string(namespaceList)
	}
	return nil
}

func (clusterInfo *ClusterInfo) AfterFind(*gorm.DB) error {
	if clusterInfo.RawNamespaceList != "" {
		if err := json.Unmarshal([]byte(clusterInfo.RawNamespaceList), &clusterInfo.NamespaceList); err != nil {
			log.Errorf("json Unmarshal RawNamespaceList[%s] failed: %v", clusterInfo.RawNamespaceList, err)
			return err
		}
	}
	return nil
}

func generateDeletedUuidStr() string {
	return uuid.GenerateIDWithLength("deleted", 24)
}

func CreateCluster(clusterInfo *ClusterInfo) error {
	log.Debugf("begin create cluster, cluster name:%s", clusterInfo.Name)
	tx := storage.DB.Table("cluster_info").Create(clusterInfo)
	if tx.Error != nil {
		log.Errorf("create cluster failed. queue:%v, error:%s",
			clusterInfo.Name, tx.Error.Error())
		return tx.Error
	}

	return nil
}

func ListCluster(pk int64, maxKeys int, clusterNameList []string, clusterStatus string) ([]ClusterInfo, error) {
	log.Debugf("list cluster, pk: %d, maxKeys: %d", pk, maxKeys)

	var clusterList []ClusterInfo
	query := storage.DB.Table("cluster_info").Where("deleted_at = '' AND pk > ?", pk)

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

func GetLastCluster() (ClusterInfo, error) {
	log.Debugf("model get last cluster. ")

	clusterInfo := ClusterInfo{}
	tx := storage.DB.Table("cluster_info").Where("deleted_at = ''").Last(&clusterInfo)
	if tx.Error != nil {
		log.Errorf("get last cluster failed. error:%s", tx.Error.Error())
		return ClusterInfo{}, tx.Error
	}
	return clusterInfo, nil
}

func GetClusterByName(clusterName string) (ClusterInfo, error) {
	log.Debugf("start to get cluster. clusterName: %s", clusterName)

	var clusterInfo ClusterInfo
	tx := storage.DB.Table("cluster_info").Where("name = ? AND deleted_at = ''", clusterName)
	tx = tx.First(&clusterInfo)

	if tx.Error != nil {
		log.Errorf("get cluster failed. clusterName: %s, error:%s",
			clusterName, tx.Error.Error())
		return ClusterInfo{}, tx.Error
	}

	return clusterInfo, nil
}

func GetClusterById(clusterId string) (ClusterInfo, error) {
	log.Debugf("start to get cluster. clusterId: %s", clusterId)

	var clusterInfo ClusterInfo
	tx := storage.DB.Table("cluster_info").Where("id = ? AND deleted_at = '' ", clusterId)
	tx = tx.First(&clusterInfo)

	if tx.Error != nil {
		log.Errorf("get cluster failed. clusterId: %s, error:%s",
			clusterId, tx.Error.Error())
		return ClusterInfo{}, tx.Error
	}

	return clusterInfo, nil
}

func DeleteCluster(clusterName string) error {
	log.Infof("start to delete cluster. clusterName:%s", clusterName)

	// 检查clusterName是否存在
	clusterInfo, err := GetClusterByName(clusterName)
	if err != nil {
		return err
	}
	// 更新 DeletedAt 字段为uuid字符串，表示逻辑删除
	clusterInfo.DeletedAt = generateDeletedUuidStr()
	clusterInfo.UpdatedAt = time.Now()
	err = UpdateCluster(clusterInfo.ID, &clusterInfo)
	if err != nil {
		log.Errorf("delete cluster failed. clusterName:%s, error:%s",
			clusterName, err.Error())
		return err
	}
	return nil
}

func UpdateCluster(clusterId string, clusterInfo *ClusterInfo) error {
	log.Debugf("start to update cluster. clusterId:%s", clusterId)
	err := storage.DB.Table("cluster_info").Where("id = ?", clusterId).Updates(clusterInfo).Error
	if err != nil {
		log.Errorf("update cluster failed. clusterId:%s, error:%s",
			clusterId, err.Error())
		return err
	}
	return nil
}

func ActiveClusters() []ClusterInfo {
	db := storage.DB.Table("cluster_info").Where("deleted_at = '' ")

	var clusterList []ClusterInfo
	err := db.Find(&clusterList).Error
	if err != nil {
		return clusterList
	}
	return clusterList
}
