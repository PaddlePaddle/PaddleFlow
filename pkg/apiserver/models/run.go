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
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	pplcommon "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
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
	ParametersJson string                 `gorm:"type:text;size:65535;not null"     json:"-"`
	Parameters     map[string]interface{} `gorm:"-"                                 json:"parameters"`
	RunYaml        string                 `gorm:"type:text;size:65535;not null"     json:"runYaml"`
	WorkflowSource schema.WorkflowSource  `gorm:"-"                                 json:"-"` // RunYaml's dynamic struct
	Runtime        schema.RuntimeView     `gorm:"-"                                 json:"runtime"`
	PostProcess    schema.PostProcessView `gorm:"-"                                 json:"postProcess"`
	FailureOptions schema.FailureOptions  `gorm:"-"                                 json:"failureOptions"`
	DockerEnv      string                 `gorm:"type:varchar(128);not null"        json:"dockerEnv"`
	Entry          string                 `gorm:"type:varchar(256);not null"        json:"entry"`
	Disabled       string                 `gorm:"type:text;size:65535;not null"     json:"disabled"`
	ScheduleID     string                 `gorm:"type:varchar(60);not null"         json:"scheduleID"`
	Message        string                 `gorm:"type:text;size:65535;not null"     json:"runMsg"`
	Status         string                 `gorm:"type:varchar(32);not null"         json:"status"` // StatusRun%%%
	RunCachedIDs   string                 `gorm:"type:text;size:65535;not null"     json:"runCachedIDs"`
	ScheduledAt    sql.NullTime           `                                         json:"-"`
	CreateTime     string                 `gorm:"-"                                 json:"createTime"`
	ActivateTime   string                 `gorm:"-"                                 json:"activateTime"`
	UpdateTime     string                 `gorm:"-"                                 json:"updateTime,omitempty"`
	CreatedAt      time.Time              `                                         json:"-"`
	ActivatedAt    sql.NullTime           `                                         json:"-"`
	UpdatedAt      time.Time              `                                         json:"-"`
	DeletedAt      gorm.DeletedAt         `                                         json:"-"`
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

func (r *Run) Encode() error {
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

func (r *Run) decode() error {
	// decode WorkflowSource
	workflowSource, err := schema.ParseWorkflowSource([]byte(r.RunYaml))
	if err != nil {
		return err
	}
	r.WorkflowSource = workflowSource

	r.validateFailureOptions()

	// 由于在所有获取Run的函数中，都需要进行decode，因此Runtime和PostProcess的赋值也在decode中进行
	if err := r.validateRuntimeAndPostProcess(); err != nil {
		return err
	}

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

// validate runtime and postProcess
func (r *Run) validateRuntimeAndPostProcess() error {
	if r.Runtime == nil {
		r.Runtime = schema.RuntimeView{}
	}
	if r.PostProcess == nil {
		r.PostProcess = schema.PostProcessView{}
	}
	// 从数据库中获取该Run的所有Step发起的Job
	runJobs, err := GetRunJobsOfRun(logger.LoggerForRun(r.ID), r.ID)
	if err != nil {
		return err
	}
	// 将所有run_job转换成JobView之后，赋值给Runtime和PostProcess
	for _, job := range runJobs {
		if step, ok := r.WorkflowSource.PostProcess[job.StepName]; ok {
			jobView := job.ParseJobView(step)
			r.PostProcess[job.StepName] = jobView
		} else if step, ok := r.WorkflowSource.EntryPoints[job.StepName]; ok {
			jobView := job.ParseJobView(step)
			r.Runtime[job.StepName] = jobView
		} else {
			entryPointNames := []string{}
			for name := range r.Runtime {
				entryPointNames = append(entryPointNames, name)
			}
			postProcessNames := []string{}
			for name := range r.PostProcess {
				postProcessNames = append(postProcessNames, name)
			}
			return fmt.Errorf("cannot find step[%s] in either entry_points[%v]\nor post_process[%v]",
				job.StepName, entryPointNames, postProcessNames)
		}
	}
	// 初始化env中的PF_RUN_TIME
	if err := r.initAllPFRuntime(); err != nil {
		return err
	}
	return nil
}

func (r *Run) initAllPFRuntime() error {
	pfRuntimeGen := pplcommon.NewPFRuntimeGenerator(r.Runtime, r.WorkflowSource)
	for name, step := range r.Runtime {
		pfRuntimeJson, err := pfRuntimeGen.GetPFRuntime(name)
		if err != nil {
			return err
		}
		step.Env[pplcommon.SysParamNamePFRuntime] = pfRuntimeJson
		r.Runtime[name] = step
	}
	for name, step := range r.PostProcess {
		pfRuntimeJson, err := pfRuntimeGen.GetPFRuntime(name)
		if err != nil {
			return err
		}
		step.Env[pplcommon.SysParamNamePFRuntime] = pfRuntimeJson
		r.PostProcess[name] = step
	}
	return nil
}

func CreateRun(logEntry *log.Entry, run *Run) (string, error) {
	logEntry.Debugf("begin create run:%+v", run)
	err := WithTransaction(storage.DB, func(tx *gorm.DB) error {
		result := tx.Model(&Run{}).Create(run)
		if result.Error != nil {
			logEntry.Errorf("create run failed. run:%v, error:%s",
				run, result.Error.Error())
			return result.Error
		}

		run.ID = common.PrefixRun + fmt.Sprintf("%06d", run.Pk)
		logEntry.Debugf("created run with pk[%d], runID[%s]", run.Pk, run.ID)
		// update ID
		result = tx.Model(&Run{}).Where("pk = ?", run.Pk).Update("id", run.ID)
		if result.Error != nil {
			logEntry.Errorf("back filling runID failed. pk[%d], error:%v",
				run.Pk, result.Error)
			return result.Error
		}
		return nil
	})

	return run.ID, err
}

func UpdateRunStatus(logEntry *log.Entry, runID, status string) error {
	logEntry.Debugf("begin update run status. runID:%s, status:%s", runID, status)
	tx := storage.DB.Model(&Run{}).Where("id = ?", runID).Update("status", status)
	if tx.Error != nil {
		logEntry.Errorf("update run status failed. runID:%s, error:%s",
			runID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func UpdateRun(logEntry *log.Entry, runID string, run Run) error {
	logEntry.Debugf("begin update run. runID:%s", runID)
	tx := storage.DB.Model(&Run{}).Where("id = ?", runID).Updates(run)
	if tx.Error != nil {
		logEntry.Errorf("update run failed. runID:%s, error:%s",
			runID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func DeleteRun(logEntry *log.Entry, runID string) error {
	logEntry.Debugf("begin delete run. runID:%s", runID)
	err := WithTransaction(storage.DB, func(tx *gorm.DB) error {
		result := storage.DB.Model(&RunJob{}).Where("run_id = ?", runID).Delete(&RunJob{})
		if result.Error != nil {
			logEntry.Errorf("delete run_job before deleting run failed. runID:%s, error:%s",
				runID, result.Error.Error())
			return result.Error
		}
		result = storage.DB.Model(&Run{}).Where("id = ?", runID).Delete(&Run{})
		if result.Error != nil {
			logEntry.Errorf("delete run failed. runID:%s, error:%s",
				runID, result.Error.Error())
			return result.Error
		}
		return nil
	})
	return err
}

func GetRunByID(logEntry *log.Entry, runID string) (Run, error) {
	logEntry.Debugf("begin get run. runID:%s", runID)
	var run Run
	tx := storage.DB.Model(&Run{}).Where("id = ?", runID).First(&run)
	if tx.Error != nil {
		logEntry.Errorf("get run failed. runID:%s, error:%s",
			runID, tx.Error.Error())
		return Run{}, tx.Error
	}
	if err := run.decode(); err != nil {
		return run, err
	}
	return run, nil
}

func ListRun(logEntry *log.Entry, pk int64, maxKeys int, userFilter, fsFilter, runFilter, nameFilter, statusFilter, scheduleIdFilter []string) ([]Run, error) {
	logEntry.Debugf("begin list run. ")
	tx := storage.DB.Model(&Run{}).Where("pk > ?", pk)
	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
	}
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}
	if len(runFilter) > 0 {
		tx = tx.Where("id IN (?)", runFilter)
	}
	if len(nameFilter) > 0 {
		tx = tx.Where("name IN (?)", nameFilter)
	}
	if len(statusFilter) > 0 {
		tx = tx.Where("status IN (?)", statusFilter)
	}
	if len(scheduleIdFilter) > 0 {
		tx = tx.Where("schedule_id IN (?)", scheduleIdFilter)
	}
	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var runList []Run
	tx = tx.Find(&runList)
	if tx.Error != nil {
		logEntry.Errorf("list run failed. Filters: user{%v}, fs{%v}, run{%v}, name{%v}, status{%v}, scheduleID{%v}. error:%s",
			userFilter, fsFilter, runFilter, nameFilter, statusFilter, scheduleIdFilter, tx.Error.Error())
		return []Run{}, tx.Error
	}
	for i := range runList {
		if err := runList[i].decode(); err != nil {
			return nil, err
		}
	}
	return runList, nil
}

func GetLastRun(logEntry *log.Entry) (Run, error) {
	logEntry.Debugf("get last run. ")
	run := Run{}
	tx := storage.DB.Model(&Run{}).Last(&run)
	if tx.Error != nil {
		logEntry.Errorf("get last run failed. error:%s", tx.Error.Error())
		return Run{}, tx.Error
	}
	if err := run.decode(); err != nil {
		return Run{}, err
	}
	return run, nil
}

func CountRun(logEntry *log.Entry, pk int64, maxKeys int, userFilter, fsFilter, runFilter, nameFilter, statusFilter, scheduleIdFilter []string) (count int64, err error) {
	logEntry.Debugf("begin count run. ")
	tx := storage.DB.Model(&Run{}).Where("pk > ?", pk)
	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
	}
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}
	if len(runFilter) > 0 {
		tx = tx.Where("id IN (?)", runFilter)
	}
	if len(nameFilter) > 0 {
		tx = tx.Where("name IN (?)", nameFilter)
	}
	if len(statusFilter) > 0 {
		tx = tx.Where("status IN (?)", statusFilter)
	}
	if len(scheduleIdFilter) > 0 {
		tx = tx.Where("schedule_id IN (?)", scheduleIdFilter)
	}
	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}

	tx = tx.Count(&count)
	if tx.Error != nil {
		logEntry.Errorf("count run failed. Filters: user{%v}, fs{%v}, run{%v}, name{%v}, status{%v}, scheduleID{%v}. error:%s",
			userFilter, fsFilter, runFilter, nameFilter, statusFilter, scheduleIdFilter, tx.Error.Error())
		return count, tx.Error
	}
	return count, nil
}

func ListRunsByStatus(logEntry *log.Entry, statusList []string) ([]Run, error) {
	logEntry.Debugf("begin list runs by status [%v]", statusList)
	runList := make([]Run, 0)
	tx := storage.DB.Model(&Run{}).Where("status IN (?)", statusList).Find(&runList)
	if tx.Error != nil {
		logEntry.Errorf("list runs by status [%v] failed. error:%s", statusList, tx.Error.Error())
		return runList, tx.Error
	}
	for i := range runList {
		if err := runList[i].decode(); err != nil {
			return nil, err
		}
	}
	return runList, nil
}
