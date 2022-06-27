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

package models

import (
	"encoding/json"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const flavourTableName = "flavour"

var (
	flavourSelectColumn = `flavour.pk as pk, flavour.id as id, flavour.name as name, flavour.cpu as cpu, flavour.mem as mem, 
flavour.scalar_resources as scalar_resources, flavour.cluster_id as cluster_id, cluster_info.name as cluster_name,
flavour.created_at as created_at, flavour.updated_at as updated_at, flavour.deleted_at as deleted_at`
	flavourJoinCluster = "left join `cluster_info` on `cluster_info`.id = `flavour`.cluster_id"
)

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
	return flavourTableName
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

	if flavour.ClusterName == "" && flavour.ClusterID != "" {
		// only single query is necessary, function of list query by join table cluster_info
		log.Debugf("query flavour[%s] in db, fill clusterName by clusterID[%s]", flavour.Name, flavour.ClusterID)
		var cluster ClusterInfo
		result := storage.DB.Where(&ClusterInfo{Model: Model{ID: flavour.ClusterID}}).First(&cluster)
		if result.Error != nil {
			log.Errorf("flavour[%s] query cluster by clusterId[%s] failed: %v", flavour.Name, flavour.ClusterID, result.Error)
			return result.Error
		}
		flavour.ClusterName = cluster.Name
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

// CreateFlavour create flavour
func CreateFlavour(flavour *Flavour) error {
	log.Infof("begin create flavour, flavour name:%v", flavour)
	if flavour.ID == "" {
		flavour.ID = uuid.GenerateID(common.PrefixFlavour)
	}
	flavour.CreatedAt = time.Now()
	tx := storage.DB.Table(flavourTableName).Create(flavour)
	if tx.Error != nil {
		log.Errorf("create flavour failed. flavour:%v, error:%s", flavour, tx.Error.Error())
		return tx.Error
	}

	return nil
}

// DeleteFlavour delete flavour
func DeleteFlavour(flavourName string) error {
	log.Infof("begin delete flavour, flavour name:%s", flavourName)
	t := storage.DB.Table(flavourTableName).Unscoped().Where("name = ?", flavourName).Delete(&Flavour{})
	if t.Error != nil {
		log.Errorf("delete flavour failed. flavour name:%s, error:%v", flavourName, t.Error)
		return t.Error
	}
	return nil
}

// GetFlavour get flavour
func GetFlavour(flavourName string) (Flavour, error) {
	log.Debugf("begin get flavour, flavour name:%s", flavourName)
	var flavour Flavour
	tx := storage.DB.Table(flavourTableName)
	result := tx.Where("name = ?", flavourName).First(&flavour)
	if result.Error != nil {
		log.Errorf("get flavour failed. flavour name:%s, error:%s", flavourName, result.Error.Error())
		return flavour, result.Error
	}

	return flavour, nil
}

// ListFlavour all params is nullable, and support fuzzy query of flavour's name by queryKey
func ListFlavour(pk int64, maxKeys int, clusterID, queryKey string) ([]Flavour, error) {
	log.Debugf("list flavour, pk: %d, maxKeys: %d, clusterID: %s", pk, maxKeys, clusterID)

	var flavours []Flavour
	query := storage.DB.Table(flavourTableName).Where("flavour.pk > ?", pk).Select(flavourSelectColumn).Joins(flavourJoinCluster)

	if clusterID != "" {
		query.Where("`flavour`.`cluster_id` = ? or `flavour`.`cluster_id` = ''", clusterID)
	} else {
		query.Where("`flavour`.`cluster_id` = ''")
	}

	if !strings.EqualFold(queryKey, "") {
		query = query.Where("flavour.name like ?", "%"+queryKey+"%")
	}
	if maxKeys > 0 {
		query = query.Limit(int(maxKeys))
	}

	err := query.Find(&flavours).Error
	if err != nil {
		log.Errorf("list flavour failed. error: %s ", err.Error())
		return nil, err
	}

	return flavours, nil
}

// UpdateFlavour update flavour
func UpdateFlavour(flavour *Flavour) error {
	flavour.UpdatedAt = time.Now()
	tx := storage.DB.Model(flavour).Updates(flavour)
	return tx.Error
}

// GetLastFlavour get last flavour that usually be used for indicating last page
func GetLastFlavour() (Flavour, error) {
	log.Debugf("get last flavour.")
	flavour := Flavour{}
	tx := storage.DB.Table(flavourTableName).Last(&flavour)
	if tx.Error != nil {
		log.Errorf("get last flavour failed. error:%s", tx.Error.Error())
		return Flavour{}, tx.Error
	}
	return flavour, nil
}
