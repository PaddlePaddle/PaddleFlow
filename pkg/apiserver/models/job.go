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
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"paddleflow/pkg/common/schema"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	sparkoperatorv1beta2 "paddleflow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
)

type Conf struct {
	Name    string            `json:"name"`
	Env     map[string]string `json:"env"`
	Command string            `json:"command"`
	Image   string            `json:"image"`
}

type Job struct {
	Pk              int64            `json:"-" gorm:"primaryKey;autoIncrement"`
	ID              string           `json:"jobID" gorm:"uniqueIndex"`
	UserName        string           `json:"userName"`
	Type            string           `json:"type"`
	Config          Conf             `json:"config"`
	RuntimeInfoJson string           `json:"-" gorm:"column:runtime_info;default:'{}'"`
	RuntimeInfo     interface{}      `json:"runtimeInfo" gorm:"-"`
	Status          schema.JobStatus `json:"status"`
	Message         string           `json:"message"`
	CreatedAt       time.Time        `json:"createTime"`
	ActivatedAt     sql.NullTime     `json:"activateTime"`
	UpdatedAt       time.Time        `json:"updateTime,omitempty"`
	DeletedAt       gorm.DeletedAt   `json:"-" gorm:"index"`
}

func (Job) TableName() string {
	return "job"
}

func (job *Job) AfterFind(tx *gorm.DB) error {
	switch job.Type {
	case string(schema.TypeVcJob):
		vcJob := batchv1alpha1.Job{}
		if err := json.Unmarshal([]byte(job.RuntimeInfoJson), &vcJob); err != nil {
			return err
		}
		job.RuntimeInfo = vcJob
	case string(schema.TypeSparkJob):
		sparkApp := sparkoperatorv1beta2.SparkApplication{}
		if err := json.Unmarshal([]byte(job.RuntimeInfoJson), &sparkApp); err != nil {
			return err
		}
		job.RuntimeInfo = sparkApp
	default:
		log.Debugf("unknown job type %s, skip unmarshall runtime info for job %s", job.Type, job.ID)
		return nil
	}
	return nil
}

func (job *Job) BeforeSave(tx *gorm.DB) error {
	if job.RuntimeInfo != nil {
		infoJson, err := json.Marshal(job.RuntimeInfo)
		if err != nil {
			return err
		}
		job.RuntimeInfoJson = string(infoJson)
	}
	return nil
}

func (s *Conf) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		log.Errorf("Conf value is not []byte, value:%v", value)
		return fmt.Errorf("Conf scan failed")
	}
	err := json.Unmarshal(b, s)
	if err != nil {
		log.Errorf("Scan Conf failed. err:[%s]", err.Error())
		return err
	}
	return nil
}

func (s Conf) Value() (driver.Value, error) {
	value, err := json.Marshal(s)
	if err != nil {
		log.Errorf("Value Conf s:%v failed.err:[%s]", s, err.Error())
		return nil, err
	}
	return value, nil
}
