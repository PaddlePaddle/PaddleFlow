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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
)

type Queue struct {
	Model           `gorm:"embedded"`
	Pk              int64               `json:"-" gorm:"primaryKey;autoIncrement"`
	Name            string              `json:"name" gorm:"uniqueIndex"`
	Namespace       string              `json:"namespace" gorm:"column:"`
	ClusterId       string              `json:"-" gorm:"column:cluster_id"`
	ClusterName     string              `json:"clusterName" gorm:"column:cluster_name;->"`
	QuotaType       string              `json:"quotaType"`
	RawMinResources string              `json:"-" gorm:"column:min_resources;default:'{}'"`
	MinResources    *resources.Resource `json:"minResources" gorm:"-"`
	RawMaxResources string              `json:"-" gorm:"column:max_resources;default:'{}'"`
	MaxResources    *resources.Resource `json:"maxResources" gorm:"-"`
	RawLocation     string              `json:"-" gorm:"column:location;type:text;default:'{}'"`
	Location        map[string]string   `json:"location" gorm:"-"`
	// 任务调度策略
	RawSchedulingPolicy string         `json:"-" gorm:"column:scheduling_policy"`
	SchedulingPolicy    []string       `json:"schedulingPolicy,omitempty" gorm:"-"`
	Status              string         `json:"status"`
	DeletedAt           gorm.DeletedAt `json:"-" gorm:"index"`

	UsedResources *resources.Resource `json:"usedResources,omitempty" gorm:"-"`
	IdleResources *resources.Resource `json:"idleResources,omitempty" gorm:"-"`
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
		queue.MinResources = resources.EmptyResource()
		if err := json.Unmarshal([]byte(queue.RawMinResources), queue.MinResources); err != nil {
			log.Errorf("json Unmarshal MinResources[%s] failed: %v", queue.RawMinResources, err)
			return err
		}
	}

	if queue.RawMaxResources != "" {
		queue.MaxResources = resources.EmptyResource()
		if err := json.Unmarshal([]byte(queue.RawMaxResources), queue.MaxResources); err != nil {
			log.Errorf("json Unmarshal MinResources[%s] failed: %v", queue.RawMaxResources, err)
			return err
		}
	}

	queue.Location = make(map[string]string)
	if queue.RawLocation != "" {
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
	return nil
}

// BeforeSave is the callback methods for saving file system
func (queue *Queue) BeforeSave(*gorm.DB) error {
	log.Debugf("queue[%s] BeforeSave, queue:%#v", queue.Name, queue)
	if queue.MinResources != nil {
		minResourcesJson, err := json.Marshal(queue.MinResources)
		if err != nil {
			log.Errorf("json Marshal MinResources[%v] failed: %v", queue.MinResources, err)
			return err
		}
		queue.RawMinResources = string(minResourcesJson)
	}

	if queue.MaxResources != nil {
		maxResourcesJson, err := json.Marshal(queue.MaxResources)
		if err != nil {
			log.Errorf("json Marshal MaxResources[%v] failed: %v", queue.MaxResources, err)
			return err
		}
		queue.RawMaxResources = string(maxResourcesJson)
	}

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
	log.Debugf("queue[%s] BeforeSave finished, queue:%#v", queue.Name, queue)

	return nil
}