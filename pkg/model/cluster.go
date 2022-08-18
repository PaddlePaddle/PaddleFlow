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

package model

import (
	"encoding/json"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
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
