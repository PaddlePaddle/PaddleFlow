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
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	log "github.com/sirupsen/logrus"
)

type RunDag struct {
	Pk             int64             `gorm:"primaryKey;autoIncrement;not null"  json:"-"`
	ID             string            `gorm:"type:varchar(60);not null"          json:"dagID"`
	RunID          string            `gorm:"type:varchar(60);not null"          json:"runID"`
	ParentDagID    string            `gorm:"type:varchar(60);not null"          json:"parentDagID"`
	Name           string            `gorm:"type:varchar(60);not null"          json:"name"`
	DagName        string            `gorm:"type:varchar(60);not null"          json:"dag_name"`
	Parameters     map[string]string `gorm:"-"                                  json:"parameters"`
	ParametersJson string            `gorm:"type:text;size:65535;not null"      json:"-"`
	Artifacts      schema.Artifacts  `gorm:"-"                                  json:"artifacts"`
	ArtifactsJson  string            `gorm:"type:text;size:65535;not null"      json:"-"`
	LoopSeq        int               `gorm:"type:int;not null"                  json:"-"`
	Status         schema.JobStatus  `gorm:"type:varchar(32);not null"          json:"status"`
	Message        string            `gorm:"type:text;size:65535;not null"      json:"message"`
	CreateTime     string            `gorm:"-"                                  json:"createTime"`
	ActivateTime   string            `gorm:"-"                                  json:"activateTime"`
	UpdateTime     string            `gorm:"-"                                  json:"updateTime,omitempty"`
	CreatedAt      time.Time         `                                          json:"-"`
	ActivatedAt    sql.NullTime      `                                          json:"-"`
	UpdatedAt      time.Time         `                                          json:"-"`
	DeletedAt      gorm.DeletedAt    `gorm:"index"                              json:"-"`
}

func CreateRunDag(logEntry *log.Entry, runDag *RunDag) (int64, error) {
	logEntry.Debugf("begin create run_dag model: %v", runDag)
	err := storage.DB.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&RunDag{}).Create(&runDag)
		if result.Error != nil {
			logEntry.Errorf("create run_dag failed. run_dag: %v, error: %s",
				runDag, result.Error.Error())
			return result.Error
		}
		return nil
	})
	return runDag.Pk, err
}

func UpdateRunDag(logEntry *log.Entry, pk int64, runDag RunDag) error {
	logEntry.Debugf("begin update run_dag")
	tx := storage.DB.Model(&RunDag{}).Where("pk = ?", pk).Updates(runDag)
	if tx.Error != nil {
		logEntry.Errorf("update run_dag failed. pk: %v, run_dag: %v, error: %s",
			pk, runDag, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func ParseRunDag(dagView *schema.DagView) RunDag {
	newParameters := map[string]string{}
	for k, v := range dagView.Parameters {
		newParameters[k] = v
	}

	return RunDag{
		ID:           dagView.DagID,
		Name:         dagView.Name,
		ParentDagID:  dagView.ParentDagID,
		Parameters:   newParameters,
		Artifacts:    *dagView.Artifacts.DeepCopy(),
		LoopSeq:      dagView.LoopSeq,
		Status:       dagView.Status,
		Message:      dagView.Message,
		ActivateTime: dagView.StartTime,
	}
}

func (rd *RunDag) Trans2DagView() schema.DagView {
	newParameters := map[string]string{}
	for k, v := range rd.Parameters {
		newParameters[k] = v
	}
	newEndTime := ""
	if rd.Status == schema.StatusJobCancelled || rd.Status == schema.StatusJobFailed || rd.Status == schema.StatusJobSucceeded || rd.Status == schema.StatusJobSkipped {
		newEndTime = rd.UpdateTime
	}
	return schema.DagView{
		PK:          rd.Pk,
		DagID:       rd.ID,
		Name:        rd.Name,
		Type:        "dag",
		DagName:     rd.DagName,
		ParentDagID: rd.ParentDagID,
		Parameters:  newParameters,
		LoopSeq:     rd.LoopSeq,
		StartTime:   rd.ActivateTime,
		EndTime:     newEndTime,
		Status:      rd.Status,
		Artifacts:   *rd.Artifacts.DeepCopy(),
		Message:     rd.Message,
		EntryPoints: map[string][]schema.ComponentView{},
	}
}

func (rd *RunDag) Encode() error {
	artifactJson, err := json.Marshal(rd.Artifacts)
	if err != nil {
		logger.Logger().Errorf("encode run job artifact failed. error:%v", err)
		return err
	}
	rd.ArtifactsJson = string(artifactJson)

	parametersJson, err := json.Marshal(rd.Parameters)
	if err != nil {
		logger.Logger().Errorf("encode run job parameters failed. error:%v", err)
		return err
	}
	rd.ParametersJson = string(parametersJson)

	if rd.ActivateTime != "" {
		activatedAt := sql.NullTime{}
		activatedAt.Time, err = time.ParseInLocation("2006-01-02 15:04:05", rd.ActivateTime, time.Local)
		activatedAt.Valid = true
		if err != nil {
			logger.Logger().Errorf("encode run job activateTime failed. error: %v", err)
			return err
		}
		rd.ActivatedAt = activatedAt
	}

	return nil
}

func GetRunDagsOfRun(logEntry *log.Entry, runID string) ([]RunDag, error) {
	logEntry.Debugf("begin to get run_dags of run with runID[%s].", runID)
	var runDags []RunDag
	tx := storage.DB.Model(&RunDag{}).Where("run_id = ?", runID).Find(&runDags)
	if tx.Error != nil {
		logEntry.Errorf("get run_dags of run with runID[%s] failed. error:%s", runID, tx.Error.Error())
		return []RunDag{}, tx.Error
	}

	for i := range runDags {
		if err := runDags[i].decode(); err != nil {
			logEntry.Errorf("decode run_jobs failed. error: %v", err)
			return []RunDag{}, err
		}
	}
	return runDags, nil
}

func (rd *RunDag) decode() error {
	if len(rd.ArtifactsJson) > 0 {
		artifacts := schema.Artifacts{}
		if err := json.Unmarshal([]byte(rd.ArtifactsJson), &artifacts); err != nil {
			logger.Logger().Errorf("decode run dag artifacts failed. error: %v", err)
		}
		rd.Artifacts = artifacts
	}

	if len(rd.ParametersJson) > 0 {
		parameters := map[string]string{}
		if err := json.Unmarshal([]byte(rd.ParametersJson), &parameters); err != nil {
			logger.Logger().Errorf("decode run dag parameters failed. error: %v", err)
		}
		rd.Parameters = parameters
	}

	// format time
	rd.CreateTime = rd.CreatedAt.Format("2006-01-02 15:04:05")
	rd.UpdateTime = rd.UpdatedAt.Format("2006-01-02 15:04:05")
	if rd.ActivatedAt.Valid {
		rd.ActivateTime = rd.ActivatedAt.Time.Format("2006-01-02 15:04:05")
	}
	return nil
}
