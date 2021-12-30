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
	"fmt"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/uuid"
	"time"
)

const (
	DefaultClusterSource = "OnPremise"
	ClusterStatusOnLine  = "online"
	ClusterStatusOffLine = "offline"
	DefaultClusterStatus = ClusterStatusOnLine
)

type ClusterInfo struct {
	Pk               int64     `gorm:"column:pk" json:"-"`                            // 自增主键
	ID               string    `gorm:"column:id" json:"clusterId"`                    // 集群id
	Name             string    `gorm:"column:name" json:"clusterName"`                // 集群名字
	Description      string    `gorm:"column:description" json:"description"`         // 集群描述
	Endpoint         string    `gorm:"column:endpoint" json:"endpoint"`               // 集群endpoint, 比如 http://10.11.11.47:8080
	Source           string    `gorm:"column:source" json:"source"`                   // 来源, 比如 OnPremise （内部部署）、AWS、CCE
	ClusterType      string    `gorm:"column:cluster_type" json:"clusterType"`        // 集群类型，比如kubernetes-v1.16
	Status           string    `gorm:"column:status" json:"status"`                   // 集群状态，可选值为online, offline
	Credential       string    `gorm:"column:credential" json:"credential"`           // 用于存储集群的凭证信息，比如k8s的kube_config配置
	Setting          string    `gorm:"column:setting" json:"setting"`                 // 存储额外配置信息
	RawNamespaceList string    `gorm:"column:namespace_list" json:"-"`                // 命名空间列表，json类型，如["ns1", "ns2"]
	NamespaceList    []string  `gorm:"-", json:"namespaceList"`                       // 命名空间列表，json类型，如["ns1", "ns2"]
	CreatedAt        time.Time `gorm:"column:created_at" json:"createTime"`           // 创建时间
	UpdatedAt        time.Time `gorm:"column:updated_at" json:"updateTime,omitempty"` // 更新时间
	DeletedAt        string    `gorm:"column:deleted_at" json:"-"`                    // 删除标识，非空表示软删除
}

func (ClusterInfo) TableName() string {
	return "cluster_info"
}

func (clusterInfo *ClusterInfo) Encode() {
	namespaceList, _ := json.Marshal(clusterInfo.NamespaceList)
	clusterInfo.RawNamespaceList = string(namespaceList)
}

func (clusterInfo *ClusterInfo) Decode() error {
	if clusterInfo.RawNamespaceList != "" {
		if err := json.Unmarshal([]byte(clusterInfo.RawNamespaceList), &clusterInfo.NamespaceList); err != nil {
			return err
		}
	}
	return nil
}

func generateDeletedUuidStr() string {
	return uuid.GenerateIDWithLength("deleted", 24)
}

func CreateCluster(ctx *logger.RequestContext, clusterInfo *ClusterInfo) error {
	clusterInfo.Encode()

	ctx.Logging().Debugf("begin create cluster, cluster name:%s", clusterInfo.Name)
	tx := database.DB.Table("cluster_info").Create(clusterInfo)
	if tx.Error != nil {
		ctx.Logging().Errorf("create cluster failed. queue:%v, error:%s",
			clusterInfo.Name, tx.Error.Error())
		return tx.Error
	}

	if err := clusterInfo.Decode(); err != nil {
		return err
	}
	return nil
}

func ListCluster(ctx *logger.RequestContext, pk, maxKeys int64,
	clusterNameList []string, clusterStatus string) ([]ClusterInfo, error) {
	ctx.Logging().Debugf("list cluster, pk: %d, maxKeys: %d", pk, maxKeys)

	var clusterList []ClusterInfo
	query := database.DB.Table("cluster_info").Where("deleted_at = '' AND pk > ?", pk)

	if len(clusterNameList) > 0 {
		query = query.Where(" name in ?", clusterNameList)
	}
	if clusterStatus != "" {
		query = query.Where(" status = ?", clusterStatus)
	}
	if maxKeys > 0 {
		query = query.Limit(int(maxKeys))
	}

	err := query.Find(&clusterList).Error
	if err != nil {
		ctx.Logging().Errorf("list cluster failed. error : %s ", err.Error())
		return nil, err
	}

	return clusterList, nil
}

func GetLastCluster(ctx *logger.RequestContext) (ClusterInfo, error) {
	ctx.Logging().Debugf("model get last cluster. ")

	clusterInfo := ClusterInfo{}
	tx := database.DB.Table("cluster_info").Where("deleted_at = ''").Last(&clusterInfo)
	if tx.Error != nil {
		ctx.Logging().Errorf("get last cluster failed. error:%s", tx.Error.Error())
		return ClusterInfo{}, tx.Error
	}
	return clusterInfo, nil
}

func GetClusterByName(ctx *logger.RequestContext, clusterName string) (ClusterInfo, error) {
	ctx.Logging().Debugf("start to get cluster. clusterName: %s", clusterName)

	var clusterInfo ClusterInfo
	tx := database.DB.Table("cluster_info").Where("name = ? AND deleted_at = ''", clusterName)
	tx = tx.First(&clusterInfo)

	if tx.Error != nil {
		ctx.Logging().Errorf("get cluster failed. clusterName: %s, error:%s",
			clusterName, tx.Error.Error())
		return ClusterInfo{}, fmt.Errorf("get cluster by clusterName[%s] failed", clusterName)
	}
	if err := clusterInfo.Decode(); err != nil {
		return ClusterInfo{}, err
	}

	return clusterInfo, nil
}

func GetClusterById(ctx *logger.RequestContext, clusterId string) (ClusterInfo, error) {
	ctx.Logging().Debugf("start to get cluster. clusterId: %s", clusterId)

	var clusterInfo ClusterInfo
	tx := database.DB.Table("cluster_info").Where("id = ? AND deleted_at = '' ", clusterId)
	tx = tx.First(&clusterInfo)

	if tx.Error != nil {
		ctx.Logging().Errorf("get cluster failed. clusterId: %s, error:%s",
			clusterId, tx.Error.Error())
		return ClusterInfo{}, tx.Error
	}
	if err := clusterInfo.Decode(); err != nil {
		return ClusterInfo{}, nil
	}
	return clusterInfo, nil
}

func DeleteCluster(ctx *logger.RequestContext, clusterName string) error {
	ctx.Logging().Debugf("start to delete cluster. clusterName:%s", clusterName)

	// 检查clusterName是否存在
	clusterInfo, err := GetClusterByName(ctx, clusterName)
	if err != nil {
		return err
	}

	// 更新 DeletedAt 字段为uuid字符串，表示逻辑删除
	clusterInfo.DeletedAt = generateDeletedUuidStr()
	clusterInfo.UpdatedAt = time.Now()
	err = UpdateCluster(ctx, clusterInfo.ID, &clusterInfo)

	if err != nil {
		ctx.Logging().Errorf("delete cluster failed. clusterName:%s, error:%s",
			clusterName, err.Error())
		return err
	}
	return nil
}

func UpdateCluster(ctx *logger.RequestContext, clusterId string, clusterInfo *ClusterInfo) error {
	ctx.Logging().Debugf("start to update cluster. clusterId:%s", clusterId)
	clusterInfo.Encode()
	err := database.DB.Table("cluster_info").Where("id = ?", clusterId).Updates(*clusterInfo).Error
	if err == nil {
		err = clusterInfo.Decode()
	}
	return err
}
