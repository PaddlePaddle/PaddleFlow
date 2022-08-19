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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const FlavourTableName = "flavour"

// Flavour records request resource info for each job
type Flavour struct {
	Model              `gorm:"embedded"  json:",inline"`
	Pk                 int64                      `json:"-"           gorm:"primaryKey;autoIncrement"`
	Name               string                     `json:"name"        gorm:"uniqueIndex"`
	ClusterID          string                     `json:"-"   gorm:"column:cluster_id;default:''"`
	ClusterName        string                     `json:"-" gorm:"column:cluster_name;->"`
	CPU                string                     `json:"cpu"         gorm:"column:cpu"`
	Mem                string                     `json:"mem"         gorm:"column:mem"`
	RawScalarResources string                     `json:"-"           gorm:"column:scalar_resources;type:text;default:'{}'"`
	ScalarResources    schema.ScalarResourcesType `json:"scalarResources" gorm:"-"`
	UserName           string                     `json:"-" gorm:"column:user_name"`
	DeletedAt          gorm.DeletedAt             `json:"-" gorm:"index"`
}

// TableName indicate table name of Flavour
func (Flavour) TableName() string {
	return FlavourTableName
}

// MarshalJSON decorate format of time
func (flavour Flavour) MarshalJSON() ([]byte, error) {
	type Alias Flavour
	return json.Marshal(&struct {
		*Alias
		CreatedAt string `json:"createTime"`
		UpdatedAt string `json:"updateTime"`
	}{
		CreatedAt: flavour.CreatedAt.Format(TimeFormat),
		UpdatedAt: flavour.UpdatedAt.Format(TimeFormat),
		Alias:     (*Alias)(&flavour),
	})
}

// AfterFind triggered when query sql
func (flavour *Flavour) AfterFind(*gorm.DB) error {
	if flavour.RawScalarResources != "" {
		flavour.ScalarResources = make(schema.ScalarResourcesType)
		if err := json.Unmarshal([]byte(flavour.RawScalarResources), &flavour.ScalarResources); err != nil {
			log.Errorf("json Unmarshal ScalarResources[%s] failed: %v", flavour.RawScalarResources, err)
			return err
		}
	}
	return nil
}

// BeforeSave is the callback methods for saving flavour
func (flavour *Flavour) BeforeSave(*gorm.DB) error {
	if len(flavour.ScalarResources) != 0 {
		scalarResourcesJSON, err := json.Marshal(&flavour.ScalarResources)
		if err != nil {
			log.Errorf("json Marshal scalarResources[%v] failed: %v", flavour.ScalarResources, err)
			return err
		}
		flavour.RawScalarResources = string(scalarResourcesJSON)
	}
	return nil
}
