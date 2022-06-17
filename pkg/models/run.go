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
	"encoding/json"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type Run struct {
	Pk             int64                  `gorm:"primaryKey;autoIncrement;not null" json:"-"`
	ID             string                 `gorm:"type:varchar(60);not null"         json:"runID"`
	Name           string                 `gorm:"type:varchar(60);not null"         json:"name"`
	Source         string                 `gorm:"type:varchar(256);not null"        json:"source"` // pipelineID or yamlPath
	UserName       string                 `gorm:"type:varchar(60);not null"         json:"username"`
	FsID           string                 `gorm:"type:varchar(60);not null"         json:"-"`
	FsName         string                 `gorm:"type:varchar(60);not null"         json:"fsname"`
	Description    string                 `gorm:"type:text;size:65535;not null"     json:"description"`
	ParametersJson string                 `gorm:"type:text;size:65535"              json:"-"`
	Parameters     map[string]interface{} `gorm:"-"                                 json:"parameters"`
	RunYaml        string                 `gorm:"type:text;size:65535"              json:"runYaml"`
	WorkflowSource schema.WorkflowSource  `gorm:"-"                                 json:"-"` // RunYaml's dynamic struct
	Runtime        schema.RuntimeView     `gorm:"-"                                 json:"runtime"`
	PostProcess    schema.PostProcessView `gorm:"-"                                 json:"postProcess"`
	FailureOptions schema.FailureOptions  `gorm:"-"                                 json:"failureOptions"`
	DockerEnv      string                 `gorm:"type:varchar(128)"                 json:"dockerEnv"`
	Entry          string                 `gorm:"type:varchar(256)"                 json:"entry"`
	Disabled       string                 `gorm:"type:text;size:65535"              json:"disabled"`
	Message        string                 `gorm:"type:text;size:65535"              json:"runMsg"`
	Status         string                 `gorm:"type:varchar(32)"                  json:"status"` // StatusRun%%%
	RunCachedIDs   string                 `gorm:"type:text;size:65535"              json:"runCachedIDs"`
	CreateTime     string                 `gorm:"-"                                 json:"createTime"`
	ActivateTime   string                 `gorm:"-"                                 json:"activateTime"`
	UpdateTime     string                 `gorm:"-"                                 json:"updateTime,omitempty"`
	CreatedAt      time.Time              `                                         json:"-"`
	ActivatedAt    sql.NullTime           `                                         json:"-"`
	UpdatedAt      time.Time              `                                         json:"-"`
	DeletedAt      gorm.DeletedAt         `gorm:"index"                             json:"-"`
}

func (Run) TableName() string {
	return "run"
}

func (r *Run) GetRunCacheIDList() []string {
	res := strings.Split(r.RunCachedIDs, common.SeparatorComma)
	// 去掉空字符串
	for i := 0; i < len(res); i++ {
		if res[i] == "" {
			res = append(res[:i], res[i+1:]...)
			i--
		}
	}
	return res
}

func (r *Run) BeforeSave(*gorm.DB) error {
	// encode param
	if r.Parameters != nil {
		paramRaw, err := json.Marshal(r.Parameters)
		if err != nil {
			logger.LoggerForRun(r.ID).Errorf("encode run param failed. error:%v", err)
			return err
		}
		r.ParametersJson = string(paramRaw)
	}
	return nil
}

func (r *Run) AfterFind(*gorm.DB) error {
	// decode WorkflowSource
	workflowSource, err := schema.ParseWorkflowSource([]byte(r.RunYaml))
	if err != nil {
		return err
	}
	r.WorkflowSource = workflowSource

	r.validateFailureOptions()

	// decode param
	if len(r.ParametersJson) > 0 {
		param := map[string]interface{}{}
		if err := json.Unmarshal([]byte(r.ParametersJson), &param); err != nil {
			logger.LoggerForRun(r.ID).Errorf("decode run param failed. error:%v", err)
			return err
		}
		r.Parameters = param
	}
	// format time
	r.CreateTime = r.CreatedAt.Format("2006-01-02 15:04:05")
	r.UpdateTime = r.UpdatedAt.Format("2006-01-02 15:04:05")
	if r.ActivatedAt.Valid {
		r.ActivateTime = r.ActivatedAt.Time.Format("2006-01-02 15:04:05")
	}
	return nil
}

func (r *Run) validateFailureOptions() {
	logger.Logger().Debugf("Strategy is %v", r.WorkflowSource.FailureOptions.Strategy)
	if r.WorkflowSource.FailureOptions.Strategy == "" {
		r.FailureOptions.Strategy = schema.FailureStrategyFailFast
	} else {
		r.FailureOptions.Strategy = r.WorkflowSource.FailureOptions.Strategy
	}
}
