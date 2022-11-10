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
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func (nc *PodResourceCache) ListResouces(clusterNameList []string, labels, labelType string, pageNo, pageSize int) ([]model.ResourceInfoResponse, error) {
	log.Debugf("begin to list cluster resource.")

	var result []model.ResourceInfoResponse
	tx := nc.dbCache.Model(&model.ResourceInfo{})
	tx = tx.Select("`resource_info`.`node_name`, `resource_info`.`resource_name`, " +
		"sum(`resource_info`.`resource_value`) as resource_value, `n`.`cluster_name`, `n`.`capacity` ").
		Joins("left join node_info as n on `resource_info`.node_id = `n`.`id` ")
	switch labelType {
	case model.ObjectTypeNode:
		tx = tx.Joins("left join `label_info` as l on resource_info.node_id = `l`.object_id").
			Where("`l`.object_type=?", labelType)
	case model.ObjectTypePod:
		tx = tx.Joins("left join `l` on resource_info.pod_id = `l`.object_id").
			Where("`l`.object_type=?", labelType)
	case "":
	default:
		return result, fmt.Errorf("no such label type %s", labelType)
	}
	// filter by labels
	ne := strings.Split(labels, "!=")
	eq := strings.Split(labels, "=")
	if labelType != "" && labels != "" {
		if len(ne) == 2 {
			tx = tx.Where("`l`.label_name = ? and `l`.label_value <> ?", ne[0], ne[1])
		} else if len(eq) == 2 {
			tx = tx.Where("`l`.label_name = ? and `l`.label_value = ?", eq[0], eq[1])
		}
	}
	if len(clusterNameList) != 0 {
		tx = tx.Where("`n`.`cluster_name` IN ?", clusterNameList)
	}
	// group by
	tx.Group("`resource_info`.node_id, resource_info.resource_value")
	// limit
	// submit query
	if tx.Find(&result); tx.Error != nil {
		log.Errorf("list resource failed, error:%s", tx.Error)
		return result, tx.Error
	}
	return result, nil
}
