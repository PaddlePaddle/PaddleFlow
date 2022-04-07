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
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
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
	ParamRaw       string                 `gorm:"type:text;size:65535"              json:"-"`
	Param          map[string]interface{} `gorm:"-"                                 json:"param"`
	RunYaml        string                 `gorm:"type:text;size:65535"              json:"runYaml"`
	WorkflowSource schema.WorkflowSource  `gorm:"-"                                 json:"-"` // RunYaml's dynamic struct
	RuntimeRaw     string                 `gorm:"type:text;size:65535"              json:"-"`
	Runtime        schema.RuntimeView     `gorm:"-"                                 json:"runtime"` // RuntimeRaw's struct
	ImageUrl       string                 `gorm:"type:varchar(128)"                 json:"imageUrl"`
	Entry          string                 `gorm:"type:varchar(256)"                 json:"entry"`
	Disabled       string                 `gorm:"type:text;size:65535"              json:"disabled"`
	Message        string                 `gorm:"type:text;size:65535"              json:"runMsg"`
	Status         string                 `gorm:"type:varchar(32)"                  json:"status"` // StatusRun%%%
	RunCacheIDs    string                 `gorm:"type:text;size:65535"              json:"runCacheIDs"`
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
	res := strings.Split(r.RunCacheIDs, common.SeparatorComma)
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
	// encode runtime
	if r.Runtime != nil {
		runtimeRaw, err := json.Marshal(r.Runtime)
		if err != nil {
			logger.LoggerForRun(r.ID).Errorf("encode run runtime failed. error:%v", err)
			return err
		}
		r.RuntimeRaw = string(runtimeRaw)
	}

	// encode param
	if r.Param != nil {
		paramRaw, err := json.Marshal(r.Param)
		if err != nil {
			logger.LoggerForRun(r.ID).Errorf("encode run param failed. error:%v", err)
			return err
		}
		r.ParamRaw = string(paramRaw)
	}
	return nil
}

func (r *Run) decode() error {
	// decode runtime
	if len(r.RuntimeRaw) > 0 {
		runtime := make(schema.RuntimeView, 0)
		if err := json.Unmarshal([]byte(r.RuntimeRaw), &runtime); err != nil {
			logger.LoggerForRun(r.ID).Errorf("decode run runtime failed. error:%v", err)
			return err
		}
		r.Runtime = runtime
	}

	// decode param
	if len(r.ParamRaw) > 0 {
		param := map[string]interface{}{}
		if err := json.Unmarshal([]byte(r.ParamRaw), &param); err != nil {
			logger.LoggerForRun(r.ID).Errorf("decode run param failed. error:%v", err)
			return err
		}
		r.Param = param
	}
	// format time
	r.CreateTime = r.CreatedAt.Format("2006-01-02 15:04:05")
	r.UpdateTime = r.UpdatedAt.Format("2006-01-02 15:04:05")
	if r.ActivatedAt.Valid {
		r.ActivateTime = r.ActivatedAt.Time.Format("2006-01-02 15:04:05")
	}
	return nil
}

func CreateRun(db *gorm.DB, logEntry *log.Entry, run *Run) (string, error) {
	logEntry.Debugf("begin create run:%+v", run)
	err := withTransaction(db, func(tx *gorm.DB) error {
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

func UpdateRunStatus(db *gorm.DB, logEntry *log.Entry, runID, status string) error {
	logEntry.Debugf("begin update run status. runID:%s, status:%s", runID, status)
	tx := db.Model(&Run{}).Where("id = ?", runID).Update("status", status)
	if tx.Error != nil {
		logEntry.Errorf("update run status failed. runID:%s, error:%s",
			runID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func UpdateRun(db *gorm.DB, logEntry *log.Entry, runID string, run Run) error {
	logEntry.Debugf("begin update run run. runID:%s", runID)
	tx := db.Model(&Run{}).Where("id = ?", runID).Updates(run)
	if tx.Error != nil {
		logEntry.Errorf("update run failed. runID:%s, error:%s",
			runID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func DeleteRun(db *gorm.DB, logEntry *log.Entry, runID string) error {
	logEntry.Debugf("begin delete run. runID:%s", runID)
	tx := db.Model(&Run{}).Unscoped().Where("id = ?", runID).Delete(&Run{})
	if tx.Error != nil {
		logEntry.Errorf("delete run failed. runID:%s, error:%s",
			runID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func GetRunByID(db *gorm.DB, logEntry *log.Entry, runID string) (Run, error) {
	logEntry.Debugf("begin get run. runID:%s", runID)
	var run Run
	tx := db.Model(&Run{}).Where("id = ?", runID).First(&run)
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

func ListRun(db *gorm.DB, logEntry *log.Entry, pk int64, maxKeys int, userFilter, fsFilter, runFilter, nameFilter []string) ([]Run, error) {
	logEntry.Debugf("begin list run. ")
	tx := db.Model(&Run{}).Where("pk > ?", pk)
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
	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var runList []Run
	tx = tx.Find(&runList)
	if tx.Error != nil {
		logEntry.Errorf("list run failed. Filters: user{%v}, fs{%v}, run{%v}, name{%v}. error:%s",
			userFilter, fsFilter, runFilter, nameFilter, tx.Error.Error())
		return []Run{}, tx.Error
	}
	for i := range runList {
		if err := runList[i].decode(); err != nil {
			return nil, err
		}
	}
	return runList, nil
}

func GetLastRun(db *gorm.DB, logEntry *log.Entry) (Run, error) {
	logEntry.Debugf("get last run. ")
	run := Run{}
	tx := db.Model(&Run{}).Last(&run)
	if tx.Error != nil {
		logEntry.Errorf("get last run failed. error:%s", tx.Error.Error())
		return Run{}, tx.Error
	}
	if err := run.decode(); err != nil {
		return Run{}, err
	}
	return run, nil
}

func GetRunCount(db *gorm.DB, logEntry *log.Entry) (int64, error) {
	logEntry.Debugf("get run count")
	var count int64
	tx := db.Model(&Run{}).Count(&count)
	if tx.Error != nil {
		logEntry.Errorf("get run count failed. error:%s", tx.Error.Error())
		return 0, tx.Error
	}
	return count, nil
}

func ListRunsByStatus(db *gorm.DB, logEntry *log.Entry, statusList []string) ([]Run, error) {
	logEntry.Debugf("begin list runs by status [%v]", statusList)
	runList := make([]Run, 0)
	tx := db.Model(&Run{}).Where("status IN (?)", statusList).Find(&runList)
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
