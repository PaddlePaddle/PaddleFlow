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
	"time"

	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

type RunJob struct {
	Pk             int64             `gorm:"primaryKey;autoIncrement;not null" json:"-"`
	ID             string            `gorm:"type:varchar(60);not null"         json:"jobID"`
	RunID          string            `gorm:"type:varchar(60);not null"         json:"runID"`
	Name           string            `gorm:"type:varchar(60);not null"         json:"name"`
	StepName       string            `gorm:"type:varchar(60);not null"         json:"step_name"`
	Command        string            `gorm:"type:text;size:65535;not null"     json:"command"`
	Parameters     map[string]string `gorm:"-"                                 json:"parameters"`
	ParametersJson string            `gorm:"type:text;size:65535;not null"     json:"-"`
	Artifacts      schema.Artifacts  `gorm:"-"                                 json:"artifacts"`
	ArtifactsJson  string            `gorm:"type:text;size:65535;not null"     json:"-"`
	Env            map[string]string `gorm:"-"                                 json:"env"`
	EnvJson        string            `gorm:"type:text;size:65535;not null"     json:"-"`
	DockerEnv      string            `gorm:"type:varchar(128);not null"        json:"docker_env"`
	Status         schema.JobStatus  `gorm:"type:varchar(32);not null"         json:"status"`
	Message        string            `gorm:"type:text;size:65535;not null"     json:"message"`
	Cache          schema.Cache      `gorm:"-"                                 json:"cache"`
	CacheJson      string            `gorm:"type:text;size:65535;not null"     json:"-"`
	CacheRunID     string            `gorm:"type:varchar(60);not null"         json:"cache_run_id"`
	CreateTime     string            `gorm:"-"                                 json:"createTime"`
	ActivateTime   string            `gorm:"-"                                 json:"activateTime"`
	UpdateTime     string            `gorm:"-"                                 json:"updateTime,omitempty"`
	CreatedAt      time.Time         `                                         json:"-"`
	ActivatedAt    sql.NullTime      `                                         json:"-"`
	UpdatedAt      time.Time         `                                         json:"-"`
	DeletedAt      gorm.DeletedAt    `gorm:"index"                             json:"-"`
}

func (rj *RunJob) BeforeSave(*gorm.DB) error {
	artifactJson, err := json.Marshal(rj.Artifacts)
	if err != nil {
		logger.Logger().Errorf("encode run job artifact failed. error:%v", err)
		return err
	}
	rj.ArtifactsJson = string(artifactJson)

	cacheJson, err := json.Marshal(rj.Cache)
	if err != nil {
		logger.Logger().Errorf("encode run job cache failed. error:%v", err)
		return err
	}
	rj.CacheJson = string(cacheJson)

	parametersJson, err := json.Marshal(rj.Parameters)
	if err != nil {
		logger.Logger().Errorf("encode run job parameters failed. error:%v", err)
		return err
	}
	rj.ParametersJson = string(parametersJson)

	envJson, err := json.Marshal(rj.Env)
	if err != nil {
		logger.Logger().Errorf("encode run job env failed. error: %v", err)
		return err
	}
	rj.EnvJson = string(envJson)

	if rj.ActivateTime != "" {
		activatedAt := sql.NullTime{}
		activatedAt.Time, err = time.ParseInLocation("2006-01-02 15:04:05", rj.ActivateTime, time.Local)
		activatedAt.Valid = true
		if err != nil {
			logger.Logger().Errorf("encode run job activateTime failed. error: %v", err)
			return err
		}
		rj.ActivatedAt = activatedAt
	}

	return nil
}

func (rj *RunJob) AfterFind(*gorm.DB) error {
	if len(rj.ArtifactsJson) > 0 {
		artifacts := schema.Artifacts{}
		if err := json.Unmarshal([]byte(rj.ArtifactsJson), &artifacts); err != nil {
			logger.Logger().Errorf("decode run job artifacts failed. error: %v", err)
		}
		rj.Artifacts = artifacts
	}

	if len(rj.CacheJson) > 0 {
		cache := schema.Cache{}
		if err := json.Unmarshal([]byte(rj.CacheJson), &cache); err != nil {
			logger.Logger().Errorf("decode run job cache failed. error: %v", err)
		}
		rj.Cache = cache
	}

	if len(rj.ParametersJson) > 0 {
		parameters := map[string]string{}
		if err := json.Unmarshal([]byte(rj.ParametersJson), &parameters); err != nil {
			logger.Logger().Errorf("decode run job parameters failed. error: %v", err)
		}
		rj.Parameters = parameters
	}

	if len(rj.EnvJson) > 0 {
		env := map[string]string{}
		if err := json.Unmarshal([]byte(rj.EnvJson), &env); err != nil {
			logger.Logger().Errorf("decode run job env failed. error: %v", err)
		}
		rj.Env = env
	}

	// format time
	rj.CreateTime = rj.CreatedAt.Format("2006-01-02 15:04:05")
	rj.UpdateTime = rj.UpdatedAt.Format("2006-01-02 15:04:05")
	if rj.ActivatedAt.Valid {
		rj.ActivateTime = rj.ActivatedAt.Time.Format("2006-01-02 15:04:05")
	}
	return nil
}

func (rj *RunJob) ParseJobView(step *schema.WorkflowSourceStep) schema.JobView {
	// 对map进行深拷贝
	newParameters := map[string]string{}
	for k, v := range rj.Parameters {
		newParameters[k] = v
	}
	newEnv := map[string]string{}
	for k, v := range rj.Env {
		newEnv[k] = v
	}
	newEndTime := ""
	if rj.Status == schema.StatusJobCancelled || rj.Status == schema.StatusJobFailed || rj.Status == schema.StatusJobSucceeded || rj.Status == schema.StatusJobSkipped {
		newEndTime = rj.UpdateTime
	}
	return schema.JobView{
		JobID:      rj.ID,
		JobName:    rj.Name,
		Command:    rj.Command,
		Parameters: newParameters,
		Env:        newEnv,
		StartTime:  rj.ActivateTime,
		EndTime:    newEndTime,
		Status:     rj.Status,
		Deps:       step.Deps,
		DockerEnv:  rj.DockerEnv,
		Artifacts:  rj.Artifacts,
		Cache:      rj.Cache,
		JobMessage: rj.Message,
		CacheRunID: rj.CacheRunID,
	}
}

func ParseRunJob(jobView *schema.JobView) RunJob {
	newParameters := map[string]string{}
	for k, v := range jobView.Parameters {
		newParameters[k] = v
	}

	newEnv := map[string]string{}
	for k, v := range jobView.Env {
		// PF_RUN_TIME 不存数据库，读取Job时动态解析生成
		if k == common.SysParamNamePFRuntime {
			continue
		}
		newEnv[k] = v
	}

	return RunJob{
		ID:           jobView.JobID,
		Name:         jobView.JobName,
		Command:      jobView.Command,
		Parameters:   newParameters,
		Artifacts:    jobView.Artifacts,
		Env:          newEnv,
		DockerEnv:    jobView.DockerEnv,
		Status:       jobView.Status,
		Message:      jobView.JobMessage,
		Cache:        jobView.Cache,
		CacheRunID:   jobView.CacheRunID,
		ActivateTime: jobView.StartTime,
	}
}
