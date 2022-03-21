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

package models

import (
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/common/uuid"
)

const (
	queueJoinCluster  = "join `cluster_info` on `cluster_info`.id = queue.cluster_id"
	queueSelectColumn = `queue.pk as pk, queue.id as id, queue.name as name, queue.namespace as namespace, queue.cluster_id as cluster_id,
cluster_info.name as cluster_name, queue.type as type, queue.max_resources as max_resources, queue.min_resources as min_resources, queue.location as location,
queue.status as status, queue.created_at as created_at, queue.updated_at as updated_at, queue.deleted_at as deleted_at`
)

type Queue struct {
	Model           `gorm:"embedded"`
	Pk              int64               `json:"-" gorm:"primaryKey;autoIncrement"`
	Name            string              `json:"name" gorm:"uniqueIndex"`
	Namespace       string              `json:"namespace" gorm:"column:"`
	ClusterId       string              `json:"-" gorm:"column:cluster_id"`
	ClusterName     string              `json:"clusterName" gorm:"column:cluster_name;->"`
	QuotaType       string              `json:"quotaType"`
	RawMinResources string              `json:"-" gorm:"column:min_resources;type:text;default:'{}'"`
	MinResources    schema.ResourceInfo `json:"minResources" gorm:"-"`
	RawMaxResources string              `json:"-" gorm:"column:max_resources;type:text;default:'{}'"`
	MaxResources    schema.ResourceInfo `json:"maxResources" gorm:"-"`
	RawLocation     string              `json:"-" gorm:"column:location;type:text;default:'{}'"`
	Location        map[string]string   `json:"location" gorm:"-"`
	// 任务调度策略
	RawSchedulingPolicy string         `json:"-" gorm:"column:scheduling_policy"`
	SchedulingPolicy    []string       `json:"schedulingPolicy,omitempty" gorm:"-"`
	Status              string         `json:"status"`
	DeletedAt           gorm.DeletedAt `json:"-" gorm:"index"`
}

func (Queue) TableName() string {
	return "queue"
}

func (queue Queue) MarshalJSON() ([]byte, error) {
	type Alias Queue
	return json.Marshal(&struct {
		*Alias
		CreatedAt string `json:"createTime"`
		UpdatedAt string `json:"updateTime"`
	}{
		CreatedAt: queue.CreatedAt.Format(TimeFormat),
		UpdatedAt: queue.UpdatedAt.Format(TimeFormat),
		Alias:     (*Alias)(&queue),
	})
}

func (queue *Queue) AfterFind(*gorm.DB) error {
	if queue.RawMinResources != "" {
		queue.MinResources = schema.ResourceInfo{
			ScalarResources: make(schema.ScalarResourcesType),
		}
		if err := json.Unmarshal([]byte(queue.RawMinResources), &queue.MinResources); err != nil {
			log.Errorf("json Unmarshal MinResources[%s] failed: %v", queue.RawMinResources, err)
			return err
		}
	}

	if queue.RawMaxResources != "" {
		queue.MaxResources = schema.ResourceInfo{
			ScalarResources: make(schema.ScalarResourcesType),
		}
		if err := json.Unmarshal([]byte(queue.RawMaxResources), &queue.MaxResources); err != nil {
			log.Errorf("json Unmarshal MinResources[%s] failed: %v", queue.RawMaxResources, err)
			return err
		}
	}

	if queue.RawLocation != "" {
		queue.Location = make(map[string]string)
		if err := json.Unmarshal([]byte(queue.RawLocation), &queue.Location); err != nil {
			log.Errorf("json Unmarshal Location[%s] failed: %v", queue.RawLocation, err)
			return err
		}
	}

	if queue.RawSchedulingPolicy != "" {
		queue.SchedulingPolicy = make([]string, 0)
		if err := json.Unmarshal([]byte(queue.RawSchedulingPolicy), &queue.SchedulingPolicy); err != nil {
			log.Errorf("json Unmarshal SchedulingPolicy[%s] failed: %v", queue.RawSchedulingPolicy, err)
			return err
		}
	}
	if queue.ClusterName == "" {
		// only single query is necessary, function of list query by join table cluster_info
		log.Debugf("queue[%s] ClusterName is nil", queue.Name)
		var cluster ClusterInfo
		db := database.DB.Table("cluster_info").Where("id = ?", queue.ClusterId).Where("deleted_at = '' ")
		if err := db.First(&cluster).Error; err != nil {
			log.Errorf("queue[%s] query cluster by clusterId[%s] failed: %v", queue.Name, queue.ClusterId, err)
			return err
		}
		queue.ClusterName = cluster.Name
	}
	return nil
}

// BeforeSave is the callback methods for saving file system
func (queue *Queue) BeforeSave(*gorm.DB) error {
	minResourcesJson, err := json.Marshal(queue.MinResources)
	if err != nil {
		log.Errorf("json Marshal MinResources[%v] failed: %v", queue.MinResources, err)
		return err
	}
	queue.RawMinResources = string(minResourcesJson)

	maxResourcesJson, err := json.Marshal(queue.MaxResources)
	if err != nil {
		log.Errorf("json Marshal MaxResources[%v] failed: %v", queue.MaxResources, err)
		return err
	}
	queue.RawMaxResources = string(maxResourcesJson)

	if len(queue.Location) != 0 {
		locationJson, err := json.Marshal(queue.Location)
		if err != nil {
			log.Errorf("json Marshal Location[%s] failed: %v", queue.Location, err)
			return err
		}
		queue.RawLocation = string(locationJson)
	}

	if len(queue.SchedulingPolicy) != 0 {
		schedulingPolicyJson, err := json.Marshal(&queue.SchedulingPolicy)
		log.Debugf("queue.SchedulingPolicy=%+v", queue.SchedulingPolicy)
		if err != nil {
			log.Errorf("json Marshal schedulingPolicy[%v] failed: %v", queue.SchedulingPolicy, err)
			return err
		}
		queue.RawSchedulingPolicy = string(schedulingPolicyJson)
	}
	return nil
}

func CreateQueue(ctx *logger.RequestContext, queue *Queue) error {
	ctx.Logging().Debugf("begin create queue. queueName: %s", queue.Name)

	if queue.ID != "" {
		queue.ID = uuid.GenerateID(common.PrefixQueue)
	}

	tx := database.DB.Table("queue").Create(queue)
	if tx.Error != nil {
		ctx.Logging().Errorf("create queue failed. queue:%v, error:%s",
			queue, tx.Error.Error())
		return tx.Error
	}
	return nil
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

func CloseQueue(ctx *logger.RequestContext, queueName string) error {
	ctx.Logging().Debugf("begin close queue. queueName:%s", queueName)
	tx := database.DB.Table("queue").Where("name = ?", queueName).Update("status", schema.StatusQueueClosed)
	if tx.Error != nil {
		ctx.Logging().Errorf("close queue failed. queueName:%s, error:%s",
			queueName, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func DeleteQueue(ctx *logger.RequestContext, queueName string) error {
	ctx.Logging().Debugf("begin delete queue. queueName:%s", queueName)
	database.DB.Transaction(func(tx *gorm.DB) error {
		t := tx.Table("queue").Unscoped().Where("name = ?", queueName).Delete(&Queue{})
		if t.Error != nil {
			ctx.Logging().Errorf("delete queue failed. queueName:%s, error:%s",
				queueName, tx.Error.Error())
			return t.Error
		}
		t = tx.Table("grant").Unscoped().Where("resource_id = ?",
			queueName).Where("resource_type = ?", common.ResourceTypeQueue).Delete(&Grant{})
		if t.Error != nil {
			ctx.Logging().Errorf("delete queue failed. queueName:%s, error:%s",
				queueName, tx.Error.Error())
			return t.Error
		}
		return nil
	})

	return nil
}

func IsQueueExist(ctx *logger.RequestContext, queueName string) bool {
	ctx.Logging().Debugf("begin check queue exist. queueName:%s", queueName)
	var queueCount int64
	tx := database.DB.Table("queue").Where("name = ?", queueName).Count(&queueCount)
	if tx.Error != nil {
		ctx.Logging().Errorf("count queue failed. queueName:%s, error:%s",
			queueName, tx.Error.Error())
		return false
	}
	if queueCount > 0 {
		return true
	}
	return false
}

func GetQueueByName(ctx *logger.RequestContext, queueName string) (Queue, error) {
	ctx.Logging().Debugf("begin get queue. queueName:%s", queueName)

	var queue Queue
	tx := database.DB.Table("queue").Where("name = ?", queueName)
	tx = tx.First(&queue)
	if tx.Error != nil {
		ctx.Logging().Errorf("get queue failed. queueName:%s, error:%s",
			queueName, tx.Error.Error())
		return Queue{}, tx.Error
	}
	return queue, nil
}

func GetQueueByID(ctx *logger.RequestContext, queueID string) (Queue, error) {
	ctx.Logging().Debugf("begin get queue. queueID:%s", queueID)

	var queue Queue
	tx := database.DB.Table("queue").Where("id = ?", queueID)
	tx = tx.First(&queue)
	if tx.Error != nil {
		ctx.Logging().Errorf("get queue failed. queueID:%s, error:%s",
			queueID, tx.Error.Error())
		return Queue{}, tx.Error
	}
	return queue, nil
}

func ListQueue(ctx *logger.RequestContext, pk int64, maxKeys int, queueName string) ([]Queue, error) {
	ctx.Logging().Debugf("begin list queue. ")
	var tx *gorm.DB
	tx = database.DB.Table("queue").Select(queueSelectColumn).Joins(queueJoinCluster).Where("queue.pk > ?", pk)
	if !common.IsRootUser(ctx.UserName) {
		tx = tx.Joins("join `grant` on `grant`.resource_id = queue.name").Where(
			"`grant`.user_name = ?", ctx.UserName)
	}
	if !strings.EqualFold(queueName, "") {
		tx = tx.Where("queue.name = ?", queueName)
	}

	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var queueList []Queue
	tx = tx.Find(&queueList)
	if tx.Error != nil {
		ctx.Logging().Errorf("list queue failed. error:%s", tx.Error.Error())
		return []Queue{}, tx.Error
	}
	return queueList, nil
}

func GetLastQueue(ctx *logger.RequestContext) (Queue, error) {
	ctx.Logging().Debugf("get last queue.")
	queue := Queue{}
	tx := database.DB.Table("queue").Last(&queue)
	if tx.Error != nil {
		ctx.Logging().Errorf("get last queue failed. error:%s", tx.Error.Error())
		return Queue{}, tx.Error
	}
	return queue, nil
}

func ActiveQueues() []Queue {
	db := database.DB.Table("queue").Where("status = ?", schema.StatusQueueOpen)

	var queues []Queue
	err := db.Find(&queues).Error
	if err != nil {
		return []Queue{}
	}
	return queues
}

func ListQueuesByCluster(clusterID string) []Queue {
	db := database.DB.Table("queue").Where("cluster_id = ?", clusterID)

	var queues []Queue
	err := db.Find(&queues).Error
	if err != nil {
		return []Queue{}
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
