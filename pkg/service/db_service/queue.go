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

package db_service

import (
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/models"
)

const (
	queueJoinCluster  = "join `cluster_info` on `cluster_info`.id = queue.cluster_id"
	queueSelectColumn = `queue.pk as pk, queue.id as id, queue.name as name, queue.namespace as namespace, queue.cluster_id as cluster_id,
cluster_info.name as cluster_name, queue.quota_type as quota_type, queue.max_resources as max_resources, queue.min_resources as min_resources, queue.location as location,
queue.status as status, queue.created_at as created_at, queue.updated_at as updated_at, queue.deleted_at as deleted_at`
)

func CreateQueue(queue *models.Queue) error {
	log.Debugf("begin create queue. queueName: %s", queue.Name)

	if queue.ID == "" {
		queue.ID = uuid.GenerateID(schema.PrefixQueue)
	}

	tx := database.DB.Table("queue").Create(queue)
	if tx.Error != nil {
		log.Errorf("create queue failed. queue:%v, error:%s",
			queue, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func UpdateQueue(queue *models.Queue) error {
	log.Debugf("update queue:[%s], queue:%#v", queue.Name, queue)
	tx := database.DB.Model(queue).Updates(queue)
	return tx.Error
}

func UpdateQueueStatus(queueName string, queueStatus string) error {
	log.Debugf("update queue status. queueName:[%s] newStatus:[%s]", queueName, queueStatus)
	if !common.IsValidQueueStatus(queueStatus) {
		log.Errorf("Invalid queue status. queueName:[%s] queueStatus:[%s]", queueName, queueStatus)
		return fmt.Errorf("Invalid queue status. queueName:[%s] queueStatus:[%s]\n", queueName, queueStatus)
	}
	tx := database.DB.Table("queue").Where("name = ?", queueName).Update("status", strings.ToLower(queueStatus))
	if tx.Error != nil {
		log.Errorf("update queue status failed. queueName:[%s], queueStatus:[%s] error:[%s]",
			queueName, queueStatus, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func CloseQueue(queueName string) error {
	log.Debugf("begin close queue. queueName:%s", queueName)
	tx := database.DB.Table("queue").Where("name = ?", queueName).Update("status", schema.StatusQueueClosed)
	if tx.Error != nil {
		log.Errorf("close queue failed. queueName:%s, error:%s",
			queueName, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func DeleteQueue(queueName string) error {
	log.Infof("begin delete queue. queueName:%s", queueName)
	database.DB.Transaction(func(tx *gorm.DB) error {
		t := tx.Table("queue").Unscoped().Where("name = ?", queueName).Delete(&models.Queue{})
		if t.Error != nil {
			log.Errorf("delete queue failed. queueName:%s, error:%s",
				queueName, tx.Error.Error())
			return t.Error
		}
		t = tx.Table("grant").Unscoped().Where("resource_id = ?",
			queueName).Where("resource_type = ?", common.ResourceTypeQueue).Delete(&Grant{})
		if t.Error != nil {
			log.Errorf("delete queue failed. queueName:%s, error:%s",
				queueName, tx.Error.Error())
			return t.Error
		}
		return nil
	})

	return nil
}

func IsQueueExist(queueName string) bool {
	log.Debugf("begin check queue exist. queueName:%s", queueName)
	var queueCount int64
	tx := database.DB.Table("queue").Where("name = ?", queueName).Count(&queueCount)
	if tx.Error != nil {
		log.Errorf("count queue failed. queueName:%s, error:%s",
			queueName, tx.Error.Error())
		return false
	}
	if queueCount > 0 {
		return true
	}
	return false
}

func GetQueueByName(queueName string) (models.Queue, error) {
	log.Debugf("begin get queue. queueName:%s", queueName)

	var queue models.Queue
	tx := database.DB.Table("queue").Where("name = ?", queueName)
	tx = tx.First(&queue)
	if tx.Error != nil {
		log.Errorf("get queue failed. queueName:%s, error:%s",
			queueName, tx.Error.Error())
		return models.Queue{}, tx.Error
	}
	if queue.ClusterName == "" {
		if err := patchQueueCluster(&queue); err != nil {
			log.Errorf("GetQueueByName[%s] query cluster by clusterId[%s] failed: %v", queue.Name, queue.ClusterId, err)
			return models.Queue{}, err
		}
	}
	return queue, nil
}

func GetQueueByID(queueID string) (models.Queue, error) {
	log.Debugf("begin get queue. queueID:%s", queueID)

	var queue models.Queue
	tx := database.DB.Table("queue").Where("id = ?", queueID)
	tx = tx.First(&queue)
	if tx.Error != nil {
		log.Errorf("get queue failed. queueID:%s, error:%s",
			queueID, tx.Error.Error())
		return models.Queue{}, tx.Error
	}
	if queue.ClusterName == "" {
		if err := patchQueueCluster(&queue); err != nil {
			log.Errorf("GetQueueByID[%s] query cluster by clusterId[%s] failed: %v", queue.ID, queue.ClusterId, err)
			return models.Queue{}, err
		}
	}
	return queue, nil
}

func ListQueue(pk int64, maxKeys int, queueName string, userName string) ([]models.Queue, error) {
	log.Debugf("begin list queue. ")
	var tx *gorm.DB
	tx = database.DB.Table("queue").Select(queueSelectColumn).Joins(queueJoinCluster).Where("queue.pk > ?", pk)
	if !common.IsRootUser(userName) {
		tx = tx.Joins("join `grant` on `grant`.resource_id = queue.name").Where(
			"`grant`.user_name = ?", userName)
	}
	if !strings.EqualFold(queueName, "") {
		tx = tx.Where("queue.name = ?", queueName)
	}

	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var queueList []models.Queue
	tx = tx.Find(&queueList)
	if tx.Error != nil {
		log.Errorf("list queue failed. error:%s", tx.Error.Error())
		return []models.Queue{}, tx.Error
	}
	for _, queue := range queueList {
		if queue.ClusterName == "" {
			if err := patchQueueCluster(&queue); err != nil {
				log.Errorf("GetLastQueue[%s] query cluster by clusterId[%s] failed: %v", queue.Name, queue.ClusterId, err)
				return []models.Queue{}, err
			}
		}
	}
	return queueList, nil
}

func GetLastQueue() (models.Queue, error) {
	log.Debugf("get last queue.")
	queue := models.Queue{}
	tx := database.DB.Table("queue").Last(&queue)
	if tx.Error != nil {
		log.Errorf("get last queue failed. error:%s", tx.Error.Error())
		return models.Queue{}, tx.Error
	}
	if queue.ClusterName == "" {
		if err := patchQueueCluster(&queue); err != nil {
			log.Errorf("GetLastQueue[%s] query cluster by clusterId[%s] failed: %v", queue.Name, queue.ClusterId, err)
			return models.Queue{}, err
		}
	}
	return queue, nil
}

func ListQueuesByCluster(clusterID string) []models.Queue {
	db := database.DB.Table("queue").Where("cluster_id = ?", clusterID)

	var queues []models.Queue
	err := db.Find(&queues).Error
	if err != nil {
		return []models.Queue{}
	}
	for _, queue := range queues {
		if queue.ClusterName == "" {
			if err := patchQueueCluster(&queue); err != nil {
				log.Errorf("ListQueuesByCluster[%s] query cluster by clusterId[%s] failed: %v", clusterID, queue.ClusterId, err)
				return []models.Queue{}
			}
		}
	}
	return queues
}

func IsQueueInUse(queueID string) (bool, map[string]schema.JobStatus) {
	queueInUseJobStatus := []schema.JobStatus{
		schema.StatusJobInit,
		schema.StatusJobPending,
		schema.StatusJobTerminating,
		schema.StatusJobRunning,
	}
	jobsInfo := make(map[string]schema.JobStatus)
	jobs := ListQueueJob(queueID, queueInUseJobStatus)
	if len(jobs) == 0 {
		return false, jobsInfo
	}
	for _, job := range jobs {
		jobsInfo[job.ID] = job.Status
	}
	return true, jobsInfo
}

// DeepCopyQueue returns a deep copy of the queue
func DeepCopyQueue(queueSrc models.Queue, queueDesc *models.Queue) {
	queueStr, _ := json.Marshal(queueSrc)
	json.Unmarshal(queueStr, &queueDesc)
	queueDesc.Pk = queueSrc.Pk
	queueDesc.ClusterId = queueSrc.ClusterId
	queueDesc.RawMinResources = queueSrc.RawMinResources
	queueDesc.RawMaxResources = queueSrc.RawMaxResources
	queueDesc.RawLocation = queueSrc.RawLocation
	queueDesc.RawSchedulingPolicy = queueSrc.RawSchedulingPolicy
}

func patchQueueCluster(queue *models.Queue) error {
	// only single query is necessary, function of list query by join table cluster_info
	log.Debugf("queue[%s] ClusterName is nil, query db to get cluster", queue.Name)
	cluster, err := GetClusterById(queue.ClusterId)
	if err != nil {
		log.Errorf("patchFlavourCluster[%s] err: %v", queue.ClusterId, err)
	}
	queue.ClusterName = cluster.Name
	return nil
}
