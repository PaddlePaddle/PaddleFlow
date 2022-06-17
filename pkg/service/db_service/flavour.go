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

package db_service

import (
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/models"
)

const flavourTableName = "flavour"

var (
	flavourSelectColumn = `flavour.pk as pk, flavour.id as id, flavour.name as name, flavour.cpu as cpu, flavour.mem as mem, 
flavour.scalar_resources as scalar_resources, flavour.cluster_id as cluster_id, cluster_info.name as cluster_name,
flavour.created_at as created_at, flavour.updated_at as updated_at, flavour.deleted_at as deleted_at`
	flavourJoinCluster = "left join `cluster_info` on `cluster_info`.id = `flavour`.cluster_id"
)

// CreateFlavour create flavour
func CreateFlavour(flavour *models.Flavour) error {
	log.Infof("begin create flavour, flavour name:%v", flavour)
	if flavour.ID == "" {
		flavour.ID = uuid.GenerateID(schema.PrefixFlavour)
	}
	flavour.CreatedAt = time.Now()
	tx := database.DB.Table(flavourTableName).Create(flavour)
	if tx.Error != nil {
		log.Errorf("create flavour failed. flavour:%v, error:%s", flavour, tx.Error.Error())
		return tx.Error
	}

	return nil
}

// DeleteFlavour delete flavour
func DeleteFlavour(flavourName string) error {
	log.Infof("begin delete flavour, flavour name:%s", flavourName)
	t := database.DB.Table(flavourTableName).Unscoped().Where("name = ?", flavourName).Delete(&models.Flavour{})
	if t.Error != nil {
		log.Errorf("delete flavour failed. flavour name:%s, error:%v", flavourName, t.Error)
		return t.Error
	}
	return nil
}

// GetFlavour get flavour
func GetFlavour(flavourName string) (models.Flavour, error) {
	log.Debugf("begin get flavour, flavour name:%s", flavourName)
	var flavour models.Flavour
	tx := database.DB.Table(flavourTableName)
	result := tx.Where("name = ?", flavourName).First(&flavour)
	if result.Error != nil {
		log.Errorf("get flavour failed. flavour name:%s, error:%s", flavourName, result.Error.Error())
		return flavour, result.Error
	}
	if err := patchFlavourCluster(&flavour); err != nil {
		log.Errorf("patchFlavourCluster[%s] err: %v", flavour.ClusterID, err)
		return models.Flavour{}, err
	}
	return flavour, nil
}

// ListFlavour all params is nullable, and support fuzzy query of flavour's name by queryKey
func ListFlavour(pk int64, maxKeys int, clusterID, queryKey string) ([]models.Flavour, error) {
	log.Debugf("list flavour, pk: %d, maxKeys: %d, clusterID: %s", pk, maxKeys, clusterID)

	flavours := make([]models.Flavour, 0)
	query := database.DB.Table(flavourTableName).Where("flavour.pk > ?", pk).Select(flavourSelectColumn).Joins(flavourJoinCluster)

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

	for idx, _ := range flavours {
		if err := patchFlavourCluster(&flavours[idx]); err != nil {
			log.Errorf("patchFlavourCluster[%s] err: %v", flavours[idx].ClusterID, err)
			return flavours, err
		}
	}

	return flavours, nil
}

// UpdateFlavour update flavour
func UpdateFlavour(flavour *models.Flavour) error {
	flavour.UpdatedAt = time.Now()
	tx := database.DB.Model(flavour).Updates(flavour)
	return tx.Error
}

// GetLastFlavour get last flavour that usually be used for indicating last page
func GetLastFlavour() (models.Flavour, error) {
	log.Debugf("get last flavour.")
	flavour := models.Flavour{}
	tx := database.DB.Table(flavourTableName).Last(&flavour)
	if tx.Error != nil {
		log.Errorf("get last flavour failed. error:%s", tx.Error.Error())
		return models.Flavour{}, tx.Error
	}
	if err := patchFlavourCluster(&flavour); err != nil {
		log.Errorf("patchFlavourCluster[%s] err: %v", flavour.ClusterID, err)
		return models.Flavour{}, err
	}
	return flavour, nil
}

func patchFlavourCluster(flavour *models.Flavour) error {
	if flavour.ClusterName == "" && flavour.ClusterID != "" {
		// only single query is necessary, function of list query by join table cluster_info
		log.Debugf("query flavour[%s] in db, fill clusterName by clusterID[%s]", flavour.Name, flavour.ClusterID)
		cluster, err := GetClusterById(flavour.ClusterID)
		if err != nil {
			log.Errorf("patchFlavourCluster[%s] err: %v", flavour.ClusterID, err)
		}
		flavour.ClusterName = cluster.Name
	}
	return nil
}
