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
	"errors"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type ClusterPodCache struct {
	dbCache *gorm.DB
}

func newClusterPodCache(db *gorm.DB) *ClusterPodCache {
	return &ClusterPodCache{dbCache: db}
}

func (cpc *ClusterPodCache) GetPod(podID string) (model.PodInfo, error) {
	log.Debugf("begin to get pod, pod id: %s", podID)

	var PodInfo model.PodInfo
	tx := cpc.dbCache.Where("id = ?", podID).First(&PodInfo)
	if tx.Error != nil {
		log.Errorf("get pod failed, pod id: %s, error:%s", podID, tx.Error)
		return model.PodInfo{}, tx.Error
	}
	return PodInfo, nil
}

func (cpc *ClusterPodCache) AddPod(podInfo *model.PodInfo) error {
	log.Debugf("begin to add pod, pod id: %s, name:%s", podInfo.ID, podInfo.Name)
	return WithTransaction(cpc.dbCache, func(tx *gorm.DB) error {
		err := tx.Create(podInfo).Error
		if err != nil {
			log.Errorf("add pod failed, pod id: %s, error:%s", podInfo.ID, tx.Error)
			return err
		}
		if podInfo.Labels != nil && len(podInfo.Labels) > 0 {
			nodeLabels := model.NewLabels(podInfo.ID, model.ObjectTypePod, podInfo.Labels)
			err = tx.Create(&nodeLabels).Error
			if err != nil {
				log.Errorf("add pod labels failed, labels: %v, error:%s", podInfo.Labels, err)
				return err
			}
		}
		if podInfo.Resources != nil && len(podInfo.Resources) > 0 {
			rInfos := model.NewResources(podInfo.ID, podInfo.NodeID, podInfo.NodeName, podInfo.Resources)
			err = tx.Create(&rInfos).Error
			if err != nil {
				log.Errorf("add pod resources failed, resource: %v, error:%s", podInfo.Resources, err)
				return err
			}
		}
		return nil
	})
}

func (cpc *ClusterPodCache) DeletePod(podID string) error {
	log.Infof("begin to delete pod. pod id:%s", podID)
	return WithTransaction(cpc.dbCache, func(tx *gorm.DB) error {
		err := tx.Unscoped().Where("id = ?", podID).Delete(&model.PodInfo{}).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Errorf("delete pod failed. pod id:%s, error:%s", podID, err)
			return err
		}

		err = tx.Unscoped().Where("object_type = ? AND object_id = ?",
			model.ObjectTypePod, podID).Delete(&model.LabelInfo{}).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Errorf("delete pod labels failed. pod id:%s, error:%s", podID, err)
			return err
		}

		err = tx.Unscoped().Where("pod_id = ?", podID).Delete(&model.ResourceInfo{}).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Errorf("delete pod resources failed. pod id:%s, error:%s", podID, err)
			return err
		}
		return nil
	})
}

func (cpc *ClusterPodCache) UpdatePod(podID string, podInfo *model.PodInfo) error {
	log.Debugf("begin to update pod. pod id:%s", podID)
	return WithTransaction(cpc.dbCache, func(tx *gorm.DB) error {
		err := tx.Model(podInfo).Where("id = ?", podID).Updates(podInfo).Error
		if err != nil {
			log.Errorf("update pod failed. pod id:%s, error:%s", podID, err)
			return err
		}
		if podInfo.Labels != nil && len(podInfo.Labels) > 0 {
			// This might be never called
			err = tx.Unscoped().Where("object_type = ? AND object_id = ?",
				model.ObjectTypePod, podID).Delete(&model.LabelInfo{}).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				log.Errorf("delete pod labels failed. pod id:%s, error:%s", podID, err)
				return err
			}
			labels := model.NewLabels(podID, model.ObjectTypePod, podInfo.Labels)
			err = tx.Create(&labels).Error
			if err != nil {
				log.Errorf("add pod labels failed, labels: %v, error:%s", podInfo.Labels, err)
				return err
			}
		}
		if podInfo.Status == int(model.TaskRunning) && podInfo.Resources != nil && len(podInfo.Resources) > 0 {
			log.Debugf("begin to update pod resource. pod id:%s", podID)
			err = tx.Unscoped().Where("pod_id = ?", podID).Delete(&model.ResourceInfo{}).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				log.Errorf("delete pod resources failed. pod id:%s, error:%s", podID, err)
				return err
			}
			// add pod resources
			rInfos := model.NewResources(podInfo.ID, podInfo.NodeID, podInfo.NodeName, podInfo.Resources)
			err = tx.Create(&rInfos).Error
			if err != nil {
				log.Errorf("add pod resources failed, resource: %v, error:%s", podInfo.Resources, err)
				return err
			}
		}
		return nil
	})
}

type PodResourceCache struct {
	dbCache *gorm.DB
}

func newResourceCache(db *gorm.DB) *PodResourceCache {
	return &PodResourceCache{dbCache: db}
}

func (nc *PodResourceCache) AddResource(rInfo *model.ResourceInfo) error {
	log.Debugf("begin to add pod resources, pod id:%s, name:%s", rInfo.PodID, rInfo.Name)
	tx := nc.dbCache.Create(rInfo)
	if tx.Error != nil {
		log.Errorf("add pod resources failed, pod id: %s, error:%s", rInfo.PodID, tx.Error)
		return tx.Error
	}
	return nil
}

func (nc *PodResourceCache) UpdateResource(podID string, rName string, podInfo *model.ResourceInfo) error {
	log.Debugf("begin to update pod resource. pod id:%s", podID)
	tx := nc.dbCache.Model(&model.ResourceInfo{}).Where("pod_id = ? AND resource_name = ?",
		podID, rName).Updates(podInfo)
	if tx.Error != nil {
		log.Errorf("update pod resource failed. pod id:%s, error:%s", podID, tx.Error)
		return tx.Error
	}
	return nil
}

func (nc *PodResourceCache) ListNodeResources(nodeIDList []string) ([]model.ResourceInfo, error) {
	log.Debugf("begin to list node resources, nodeIDList: %v.", nodeIDList)

	var result []model.ResourceInfo
	tx := nc.dbCache.Model(&model.ResourceInfo{})
	tx = tx.Select("`resource_info`.`node_id`, `resource_info`.`node_name`, `resource_info`.`resource_name`, "+
		"sum(`resource_info`.`resource_value`) as resource_value ").Where("resource_info.node_id IN ?", nodeIDList)
	// group by
	tx.Group("`resource_info`.resource_name, resource_info.node_id")
	// order by
	tx.Order("resource_info.node_id")

	// query
	if tx.Find(&result); tx.Error != nil {
		log.Errorf("list resource failed, error:%s", tx.Error)
		return result, tx.Error
	}
	return result, nil
}
