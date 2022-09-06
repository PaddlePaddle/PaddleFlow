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
	"database/sql"
	"encoding/json"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type Job struct {
	Pk                int64               `json:"-" gorm:"primaryKey;autoIncrement"`
	ID                string              `json:"jobID" gorm:"type:varchar(60);index:idx_id,unique;NOT NULL"`
	Name              string              `json:"jobName" gorm:"type:varchar(512);default:''"`
	UserName          string              `json:"userName" gorm:"NOT NULL"`
	QueueID           string              `json:"queueID" gorm:"NOT NULL"`
	Type              string              `json:"type" gorm:"type:varchar(20);NOT NULL"`
	ConfigJson        string              `json:"-" gorm:"column:config;type:text"`
	Config            *schema.Conf        `json:"config" gorm:"-"`
	RuntimeInfoJson   string              `json:"-" gorm:"column:runtime_info;default:'{}'"`
	RuntimeInfo       interface{}         `json:"runtimeInfo" gorm:"-"`
	RuntimeStatusJson string              `json:"-" gorm:"column:runtime_status;default:'{}'"`
	RuntimeStatus     interface{}         `json:"runtimeStatus" gorm:"-"`
	Status            schema.JobStatus    `json:"status" gorm:"type:varchar(32);"`
	Message           string              `json:"message"`
	ResourceJson      string              `json:"-" gorm:"column:resource;type:text;default:'{}'"`
	Resource          *resources.Resource `json:"resource" gorm:"-"`
	Framework         schema.Framework    `json:"framework" gorm:"type:varchar(30)"`
	MembersJson       string              `json:"-" gorm:"column:members;type:text"`
	Members           []schema.Member     `json:"members" gorm:"-"`
	ExtensionTemplate string              `json:"-" gorm:"type:text"`
	ParentJob         string              `json:"-" gorm:"type:varchar(60)"`
	CreatedAt         time.Time           `json:"createTime"`
	ActivatedAt       sql.NullTime        `json:"activateTime"`
	UpdatedAt         time.Time           `json:"updateTime,omitempty"`
	DeletedAt         string              `json:"-" gorm:"index:idx_id"`
}

func (Job) TableName() string {
	return "job"
}

func (job *Job) BeforeSave(tx *gorm.DB) error {
	if job.RuntimeInfo != nil {
		infoJson, err := json.Marshal(job.RuntimeInfo)
		if err != nil {
			return err
		}
		job.RuntimeInfoJson = string(infoJson)
	}
	if job.RuntimeStatus != nil {
		statusJson, err := json.Marshal(job.RuntimeStatus)
		if err != nil {
			return err
		}
		job.RuntimeStatusJson = string(statusJson)
	}
	if len(job.Members) != 0 {
		infoJson, err := json.Marshal(job.Members)
		if err != nil {
			return err
		}
		job.MembersJson = string(infoJson)
	}
	if job.Resource != nil {
		infoJson, err := json.Marshal(job.Resource)
		if err != nil {
			return err
		}
		job.ResourceJson = string(infoJson)
	}
	if job.Config != nil {
		infoJson, err := json.Marshal(job.Config)
		if err != nil {
			return err
		}
		job.ConfigJson = string(infoJson)
	}
	return nil
}

func (job *Job) AfterFind(tx *gorm.DB) error {
	if len(job.RuntimeInfoJson) > 0 {
		var runtime interface{}
		err := json.Unmarshal([]byte(job.RuntimeInfoJson), &runtime)
		if err != nil {
			log.Errorf("job[%s] json unmarshal runtime failed, error: %s", job.ID, err.Error())
			return err
		}
		job.RuntimeInfo = runtime
	}
	if len(job.RuntimeStatusJson) > 0 {
		var runtimeStatus interface{}
		err := json.Unmarshal([]byte(job.RuntimeStatusJson), &runtimeStatus)
		if err != nil {
			log.Errorf("job[%s] json unmarshal runtime status failed, error: %s", job.ID, err.Error())
			return err
		}
		job.RuntimeStatus = runtimeStatus
	}
	if len(job.MembersJson) > 0 {
		var members []schema.Member
		err := json.Unmarshal([]byte(job.MembersJson), &members)
		if err != nil {
			log.Errorf("job[%s] json unmarshal member failed, error: %s", job.ID, err.Error())
			return err
		}
		job.Members = members
	}
	if len(job.ConfigJson) > 0 {
		conf := schema.Conf{}
		err := json.Unmarshal([]byte(job.ConfigJson), &conf)
		if err != nil {
			log.Errorf("job[%s] json unmarshal config failed, error: %s", job.ID, err.Error())
			return err
		}
		job.Config = &conf
	}
	return nil
}
