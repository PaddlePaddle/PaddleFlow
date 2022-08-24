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
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

const (
	queueJoinCluster  = "join `cluster_info` on `cluster_info`.id = queue.cluster_id"
	queueSelectColumn = `queue.pk as pk, queue.id as id, queue.name as name, queue.namespace as namespace, queue.cluster_id as cluster_id,
cluster_info.name as cluster_name, queue.quota_type as quota_type, queue.max_resources as max_resources, queue.min_resources as min_resources, queue.location as location,
queue.scheduling_policy as scheduling_policy, queue.status as status, queue.created_at as created_at, queue.updated_at as updated_at, queue.deleted_at as deleted_at`
)

type QueueStore struct {
	db *gorm.DB
}

func newQueueStore(db *gorm.DB) *QueueStore {
	return &QueueStore{db: db}
}

func (qs *QueueStore) CreateQueue(queue *model.Queue) error {
	log.Debugf("begin create queue. queueName: %s", queue.Name)

	if queue.ID == "" {
		queue.ID = uuid.GenerateID(common.PrefixQueue)
	}

	tx := qs.db.Table("queue").Create(queue)
	if tx.Error != nil {
		log.Errorf("create queue failed. queue:%v, error:%s",
			queue, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func (qs *QueueStore) CreateOrUpdateQueue(queue *model.Queue) error {
	log.Debugf("create or update queue: %s, info:%#v", queue.Name, queue)
	if queue.ID == "" {
		queue.ID = uuid.GenerateID(common.PrefixQueue)
	}
	tx := qs.db.Table("queue").Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "name"}},
		DoUpdates: clause.AssignmentColumns([]string{"status", "namespace", "cluster_id",
			"max_resources", "min_resources", "location"}),
	}).Create(queue)
	if tx.Error != nil {
		log.Errorf("create or update queue %v failed. err: %s", queue, tx.Error.Error())
		return tx.Error
	}
	return nil

}

func (qs *QueueStore) UpdateQueue(queue *model.Queue) error {
	log.Debugf("update queue:[%s], queue:%#v", queue.Name, queue)
	tx := qs.db.Model(queue).Updates(queue)
	return tx.Error
}

func (qs *QueueStore) UpdateQueueStatus(queueName string, queueStatus string) error {
	log.Debugf("update queue status. queueName:[%s] newStatus:[%s]", queueName, queueStatus)
	if !common.IsValidQueueStatus(queueStatus) {
		log.Errorf("Invalid queue status. queueName:[%s] queueStatus:[%s]", queueName, queueStatus)
		return fmt.Errorf("Invalid queue status. queueName:[%s] queueStatus:[%s]\n", queueName, queueStatus)
	}
	tx := qs.db.Table("queue").Where("name = ?", queueName).Update("status", strings.ToLower(queueStatus))
	if tx.Error != nil {
		log.Errorf("update queue status failed. queueName:[%s], queueStatus:[%s] error:[%s]",
			queueName, queueStatus, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func (qs *QueueStore) UpdateQueueInfo(name, status string, max, min *resources.Resource) error {
	queue, err := qs.GetQueueByName(name)
	if err != nil {
		return err
	}
	if status != "" && common.IsValidQueueStatus(status) {
		queue.Status = status
	}
	if max != nil {
		queue.MaxResources = max
	}
	if min != nil {
		queue.MinResources = min
	}
	tx := qs.db.Table("queue").Where("name = ?", name).Updates(&queue)
	if tx.Error != nil {
		log.Errorf("update queue failed, err %v", tx.Error)
		return tx.Error
	}
	return nil
}

func (qs *QueueStore) DeleteQueue(queueName string) error {
	log.Infof("begin delete queue. queueName:%s", queueName)
	return qs.db.Transaction(func(tx *gorm.DB) error {
		t := tx.Table("queue").Unscoped().Where("name = ?", queueName).Delete(&model.Queue{})
		if t.Error != nil {
			log.Errorf("delete queue failed. queueName:%s, error:%s",
				queueName, tx.Error.Error())
			return t.Error
		}
		t = tx.Table("grant").Unscoped().Where("resource_id = ?",
			queueName).Where("resource_type = ?", common.ResourceTypeQueue).Delete(&model.Grant{})
		if t.Error != nil {
			log.Errorf("delete queue failed. queueName:%s, error:%s",
				queueName, tx.Error.Error())
			return t.Error
		}
		return nil
	})
}

func (qs *QueueStore) IsQueueExist(queueName string) bool {
	log.Debugf("begin check queue exist. queueName:%s", queueName)
	var queueCount int64
	tx := qs.db.Table("queue").Where("name = ?", queueName).Count(&queueCount)
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

func (qs *QueueStore) GetQueueByName(queueName string) (model.Queue, error) {
	log.Debugf("begin get queue. queueName:%s", queueName)

	var queue model.Queue
	tx := qs.db.Table("queue").Select(queueSelectColumn).Joins(queueJoinCluster).Where("queue.name = ?", queueName)
	tx = tx.First(&queue)
	if tx.Error != nil {
		log.Errorf("get queue failed. queueName:%s, error:%s",
			queueName, tx.Error.Error())
		return model.Queue{}, tx.Error
	}
	return queue, nil
}

func (qs *QueueStore) GetQueueByID(queueID string) (model.Queue, error) {
	log.Debugf("begin get queue. queueID:%s", queueID)

	var queue model.Queue
	tx := qs.db.Table("queue").Select(queueSelectColumn).Joins(queueJoinCluster).Where("queue.id = ?", queueID)
	tx = tx.First(&queue)
	if tx.Error != nil {
		log.Errorf("get queue failed. queueID:%s, error:%s",
			queueID, tx.Error.Error())
		return model.Queue{}, tx.Error
	}
	return queue, nil
}

func (qs *QueueStore) ListQueue(pk int64, maxKeys int, queueName string, userName string) ([]model.Queue, error) {
	log.Debugf("begin list queue. ")
	var tx *gorm.DB
	tx = qs.db.Table("queue").Select(queueSelectColumn).Joins(queueJoinCluster).Where("queue.pk > ?", pk)
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
	var queueList []model.Queue
	tx = tx.Find(&queueList)
	if tx.Error != nil {
		log.Errorf("list queue failed. error:%s", tx.Error.Error())
		return []model.Queue{}, tx.Error
	}
	return queueList, nil
}

func (qs *QueueStore) GetLastQueue() (model.Queue, error) {
	log.Debugf("get last queue.")
	queue := model.Queue{}
	tx := qs.db.Table("queue").Last(&queue)
	if tx.Error != nil {
		log.Errorf("get last queue failed. error:%s", tx.Error.Error())
		return model.Queue{}, tx.Error
	}
	return queue, nil
}

func (qs *QueueStore) ListQueuesByCluster(clusterID string) []model.Queue {
	db := qs.db.Table("queue").Where("cluster_id = ?", clusterID)

	var queues []model.Queue
	err := db.Find(&queues).Error
	if err != nil {
		return []model.Queue{}
	}
	return queues
}

func (qs *QueueStore) IsQueueInUse(queueID string) (bool, map[string]schema.JobStatus) {
	queueInUseJobStatus := []schema.JobStatus{
		schema.StatusJobInit,
		schema.StatusJobPending,
		schema.StatusJobTerminating,
		schema.StatusJobRunning,
	}
	jobsInfo := make(map[string]schema.JobStatus)
	jobs := Job.ListQueueJob(queueID, queueInUseJobStatus)
	if len(jobs) == 0 {
		return false, jobsInfo
	}
	for _, job := range jobs {
		jobsInfo[job.ID] = job.Status
	}
	return true, jobsInfo
}

// DeepCopyQueue returns a deep copy of the queue
func (qs *QueueStore) DeepCopyQueue(queueSrc model.Queue, queueDesc *model.Queue) {
	queueStr, _ := json.Marshal(queueSrc)
	json.Unmarshal(queueStr, &queueDesc)
	queueDesc.Pk = queueSrc.Pk
	queueDesc.ClusterId = queueSrc.ClusterId
	queueDesc.RawMinResources = queueSrc.RawMinResources
	queueDesc.RawMaxResources = queueSrc.RawMaxResources
	queueDesc.RawLocation = queueSrc.RawLocation
	queueDesc.RawSchedulingPolicy = queueSrc.RawSchedulingPolicy
}
