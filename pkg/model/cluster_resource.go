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

const (
	ObjectTypeNode = "node"
	ObjectTypePod  = "pod"
)

type NodeInfo struct {
	Pk        int64  `gorm:"primaryKey;autoIncrement" json:"-"`
	ID        string `gorm:"column:id;index:idx_id" json:"nodeID"`
	Name      string `gorm:"column:name" json:"nodeName"`
	ClusterID string `gorm:"column:cluster_id" json:"-"`
	Status    string `gorm:"column:status" json:"nodeStatus"`
	Capacity  string `gorm:"column:capacity" json:"nodeCapacity"`
}

func (NodeInfo) TableName() string {
	return "node_info"
}

type PodInfo struct {
	Pk     int64  `gorm:"primaryKey;autoIncrement" json:"-"`
	ID     string `gorm:"column:id;index:idx_id" json:"podID"`
	Name   string `gorm:"column:name" json:"podName"`
	NodeID string `gorm:"column:node_id" json:"nodeID"`
	Status string `gorm:"column:status" json:"podStatus"`
}

func (PodInfo) TableName() string {
	return "pod_info"
}

type ResourceInfo struct {
	Pk       int64  `gorm:"primaryKey;autoIncrement" json:"-"`
	PodID    string `gorm:"column:pod_id;index:idx_pod_id" json:"podID"`
	NodeID   string `gorm:"column:node_id;index:idx_node_id" json:"nodeID"`
	NodeName string `gorm:"column:node_name" json:"nodeName"`
	Name     string `gorm:"column:resource_name;index:idx_resource_name" json:"resourceName"`
	Value    int64  `gorm:"column:resource_value" json:"resourceValue"`
	IsShared int    `gorm:"column:is_shared" json:"-"`
}

func (ResourceInfo) TableName() string {
	return "resource_info"
}

type LabelInfo struct {
	Pk         int64  `gorm:"primaryKey;autoIncrement" json:"-"`
	ID         string `gorm:"column:id" json:"id"`
	Name       string `gorm:"column:label_name;index:idx_name" json:"labelName"`
	Value      string `gorm:"column:label_value" json:"labelValue"`
	ObjectID   string `gorm:"column:object_id;index:idx_obj_id" json:"objectID"`
	ObjectType string `gorm:"column:object_type" json:"objectType"`
}

func (LabelInfo) TableName() string {
	return "label_info"
}
