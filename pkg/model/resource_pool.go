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

package model

type ResourcePool struct {
	Model          `gorm:"embedded"`
	Pk             int64    `json:"-" gorm:"primaryKey;autoIncrement"`
	Name           string   `json:"name" gorm:"index:idx_id,unique"`
	Namespace      string   `json:"namespace" gorm:"column:"`
	Type           string   `json:"type" gorm:"column:resource_type"`
	Provider       string   `json:"provider" gorm:"column:resource_provider"`
	OrgID          string   `json:"orgID" gorm:"column:org_id"`
	CreatorID      string   `json:"creator" gorm:"column:creator_id"`
	Description    string   `json:"description" gorm:"column:description"`
	ClusterId      string   `json:"-" gorm:"column:cluster_id"`
	ClusterName    string   `json:"clusterName" gorm:"column:cluster_name;->"`
	QuotaType      string   `json:"quotaType"`
	MinResources   Resource `json:"minResources" gorm:"column:min_resources"`
	TotalResources Resource `json:"totalResources" gorm:"column:total_resources;default:'{}'"`
	Location       Map      `json:"location" gorm:"column:location;type:text"`
	Status         string   `json:"status"`
	DeletedAt      string   `json:"-" gorm:"index:idx_id"`
}

func (ResourcePool) TableName() string {
	return "resource_pool"
}
