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

	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
)

const (
	ObjectTypeNode = "node"
	ObjectTypePod  = "pod"
)

type NodeInfo struct {
	Pk           int64             `gorm:"primaryKey;autoIncrement" json:"-"`
	ID           string            `gorm:"column:id" json:"nodeID"`
	Name         string            `gorm:"column:name" json:"nodeName"`
	ClusterID    string            `gorm:"column:cluster_id" json:"-"`
	Status       string            `gorm:"column:status" json:"nodeStatus"`
	CapacityJSON string            `gorm:"column:capacity" json:"-"`
	Capacity     interface{}       `gorm:"-" json:"nodeCapacity"`
	Labels       map[string]string `gorm:"-" json:"nodeLabels"`
}

func (NodeInfo) TableName() string {
	return "node_info"
}

func (node *NodeInfo) BeforeSave(tx *gorm.DB) error {
	if node.Capacity != nil {
		infoJson, err := json.Marshal(node.Capacity)
		if err != nil {
			return err
		}
		node.CapacityJSON = string(infoJson)
	}
	return nil
}

func (node *NodeInfo) AfterFind(tx *gorm.DB) error {
	if node.CapacityJSON != "" {
		var capacity interface{}
		err := json.Unmarshal([]byte(node.CapacityJSON), &capacity)
		if err != nil {
			return err
		}
		node.Capacity = capacity
	}
	return nil
}

type PodInfo struct {
	Pk        int64             `gorm:"primaryKey;autoIncrement" json:"-"`
	ID        string            `gorm:"column:id" json:"id"`
	Name      string            `gorm:"column:name" json:"name"`
	NodeID    string            `gorm:"column:node_id" json:"nodeID"`
	NodeName  string            `gorm:"column:node_name" json:"nodeName"`
	Status    string            `gorm:"column:status" json:"status"`
	Labels    map[string]string `gorm:"-" json:"labels"`
	Resources map[string]int64  `gorm:"-" json:"resources"`
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

func NewResources(podID, nodeID, nodeName string, resources map[string]int64) []ResourceInfo {
	if resources == nil {
		return []ResourceInfo{}
	}

	var rInfos []ResourceInfo
	for rName, rValue := range resources {
		rInfos = append(rInfos, ResourceInfo{
			PodID:    podID,
			NodeID:   nodeID,
			NodeName: nodeName,
			Name:     rName,
			Value:    rValue,
		})
	}
	return rInfos
}

type LabelInfo struct {
	Pk         int64  `gorm:"primaryKey;autoIncrement" json:"-"`
	ID         string `gorm:"column:id" json:"id"`
	Name       string `gorm:"column:label_name;index:idx_name" json:"labelName"`
	Value      string `gorm:"column:label_value" json:"labelValue"`
	ObjectID   string `gorm:"column:object_id;index:idx_obj,priority:2" json:"objectID"`
	ObjectType string `gorm:"column:object_type;index:idx_obj,priority:1" json:"objectType"`
}

func (LabelInfo) TableName() string {
	return "label_info"
}

func NewLabels(objID, objType string, labels map[string]string) []LabelInfo {
	if labels == nil {
		return []LabelInfo{}
	}
	var labelsInfo []LabelInfo
	for label, value := range labels {
		labelsInfo = append(labelsInfo, LabelInfo{
			ID:         uuid.GenerateID("label"),
			Name:       label,
			Value:      value,
			ObjectID:   objID,
			ObjectType: objType,
		})
	}
	return labelsInfo
}
