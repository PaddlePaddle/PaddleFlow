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
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type JobLabel struct {
	Pk        int64  `gorm:"primaryKey;autoIncrement"`
	ID        string `gorm:"type:varchar(36);uniqueIndex"`
	Label     string `gorm:"type:varchar(255);NOT NULL"`
	JobID     string `gorm:"type:varchar(60);NOT NULL"`
	CreatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

func (JobLabel) TableName() string {
	return "job_label"
}

// list job process multi label get and result
func ListJobIDByLabels(labels map[string]string) ([]string, error) {
	jobIDs := make([]string, 0)
	var jobLabels []JobLabel
	labelsListStr := make([]string, 0)
	for k, v := range labels {
		item := k + "=" + v
		labelsListStr = append(labelsListStr, item)
	}
	err := storage.DB.Table("job_label").Where("label IN (?)", labelsListStr).Find(&jobLabels).Error
	if err != nil {
		log.Errorf("list jobID by labels failed, error:[%s]", err.Error())
		return nil, err
	}
	jobLabelsMap := make(map[string]map[string]interface{}, 0)
	for _, v := range jobLabels {
		if _, ok := jobLabelsMap[v.JobID]; !ok {
			jobLabelsMap[v.JobID] = make(map[string]interface{}, 0)
			jobLabelsMap[v.JobID][v.Label] = nil
		} else {
			jobLabelsMap[v.JobID][v.Label] = nil
		}
	}
	for k, v := range jobLabelsMap {
		flag := true
		for _, label := range labelsListStr {
			if _, ok := v[label]; !ok {
				flag = false
				break
			}
		}
		if flag {
			jobIDs = append(jobIDs, k)
		}
	}
	return jobIDs, nil
}
