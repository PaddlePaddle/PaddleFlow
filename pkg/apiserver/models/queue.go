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
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	v1 "k8s.io/api/core/v1"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
)

type ScalarResourcesType map[v1.ResourceName]string

type QueueInfo struct {
	Namespace       string              `json:"namespace"`
	ClusterName     string              `json:"clusterName,omitempty" gorm:"cluster_name"`
	Name            string              `json:"name" gorm:"uniqueIndex"`
	Cpu             string              `json:"cpu"`
	Mem             string              `json:"mem"`
	ScalarResources ScalarResourcesType `json:"scalarResources,omitempty" gorm:"type:text"`
}

type Queue struct {
	QueueInfo `gorm:"embedded"`
	Pk        int64          `json:"-" gorm:"primaryKey;autoIncrement"`
	Status    string         `json:"status"`
	CreatedAt time.Time      `json:"createTime"`
	UpdatedAt time.Time      `json:"updateTime,omitempty"`
	DeletedAt gorm.DeletedAt `json:"-" gorm:"index"`
}

func (Queue) TableName() string {
	return "queue"
}

func (s *ScalarResourcesType) Scan(value interface{}) error {
	if value == nil {
		log.Debugln("Scan ScalarResourcesType value is nil")
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		log.Errorf("ScalarResourcesType value is not []byte, value:%v", value)
		return fmt.Errorf("ScalarResourcesType scan failed")
	}
	err := json.Unmarshal(b, s)
	if err != nil {
		log.Errorf("Scan ScalarResourcesType failed. err:[%s]", err.Error())
		return err
	}
	log.Debugf("Scan ScalarResourcesType s:%v", s)
	return nil
}

func (s ScalarResourcesType) Value() (driver.Value, error) {
	if s == nil {
		log.Debugln("Value ScalarResourcesType s is nil")
		return nil, nil
	}
	log.Debugf("marshal s:%v", s)
	value, err := json.Marshal(s)
	if err != nil {
		log.Errorf("Value ScalarResourcesType s:%v failed.err:[%s]", s, err.Error())
		return nil, err
	}
	return value, nil
}

func CreateQueue(ctx *logger.RequestContext, queue *Queue) error {
	ctx.Logging().Debugf("begin create queue. queueID:%s", queue.Name)

	// 如果ClusterName不空，检查clusterName是否存在
	if queue.ClusterName != "" {
		_, err := GetClusterByName(ctx, queue.ClusterName)
		if err != nil {
			ctx.Logging().Errorf("GetClusterByName failed, clusterName: %s, queueName: %s, errorMsg: %s",
				queue.ClusterName, queue.Name, err.Error())
			return err
		}
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
	tx := database.DB.Table("queue").Where("name = ?", queueName).Update("status", common.StatusQueueClosed)
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

func ListQueue(ctx *logger.RequestContext, pk int64, maxKeys int, queueName string) ([]Queue, error) {
	ctx.Logging().Debugf("begin list queue. ")

	var tx *gorm.DB
	if !common.IsRootUser(ctx.UserName) {
		tx = database.DB.Table("queue").Select("queue.pk as pk, queue.name as name, "+
			"queue.namespace as namespace, queue.cluster_name as cluster_name, queue.cpu as cpu, queue.mem as mem, "+
			"queue.scalar_resources as scalar_resources, queue.status as status, "+
			"queue.created_at as created_at, queue.updated_at as updated_at, "+
			"queue.deleted_at as deleted_at").Joins("join `grant` on `grant`.resource_id = queue.name").Where(
			"`grant`.user_name = ?", ctx.UserName).Where("queue.pk > ?", pk)
		if !strings.EqualFold(queueName, "") {
			tx = tx.Where("queue.name = ?", queueName)
		}
	} else {
		tx = database.DB.Table("queue").Where("pk > ?", pk)
		if !strings.EqualFold(queueName, "") {
			tx = tx.Where("name = ?", queueName)
		}
	}

	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var queueList []Queue
	tx = tx.Find(&queueList)
	fmt.Println(queueList)
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
