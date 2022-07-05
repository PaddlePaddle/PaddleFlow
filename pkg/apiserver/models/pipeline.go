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
	"fmt"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type Pipeline struct {
	Pk        int64          `json:"-"                    gorm:"primaryKey;autoIncrement;not null"`
	ID        string         `json:"pipelineID"           gorm:"type:varchar(60);not null;uniqueIndex"`
	Name      string         `json:"name"                 gorm:"type:varchar(60);not null;index:idx_fs_name"`
	Desc      string         `json:"desc"                 gorm:"type:varchar(256);not null"`
	UserName  string         `json:"username"             gorm:"type:varchar(60);not null;index:idx_fs_name"`
	CreatedAt time.Time      `json:"-"`
	UpdatedAt time.Time      `json:"-"`
	DeletedAt gorm.DeletedAt `json:"-"`
}

func (Pipeline) TableName() string {
	return "pipeline"
}

func CreatePipeline(logEntry *log.Entry, ppl *Pipeline, pplDetail *PipelineDetail) (pplID string, pplDetailID string, err error) {
	logEntry.Debugf("begin create pipeline: %+v & pipeline detail: %+v", ppl, pplDetail)
	err = WithTransaction(storage.DB, func(tx *gorm.DB) error {
		result := tx.Model(&Pipeline{}).Create(ppl)
		if result.Error != nil {
			logEntry.Errorf("create pipeline failed. pipeline:%+v, error:%v", ppl, result.Error)
			return result.Error
		}
		// update ID by pk
		ppl.ID = common.PrefixPipeline + fmt.Sprintf("%06d", ppl.Pk)
		result = tx.Model(&Pipeline{}).Where("pk = ?", ppl.Pk).Update("id", ppl.ID)
		if result.Error != nil {
			logEntry.Errorf("backfilling pplID to pipeline[%d] failed. error:%v", ppl.Pk, result.Error)
			return result.Error
		}

		var pplDetailCount int64
		tx = tx.Unscoped().Model(&PipelineDetail{}).Where("pipeline_id = ?", ppl.ID).Count(&pplDetailCount)
		if tx.Error != nil {
			logger.Logger().Errorf("count pipeline detail failed. pipelineID[%s]. error:%s",
				ppl.ID, tx.Error.Error())
			return tx.Error
		}
		pplDetail.ID = strconv.FormatInt(pplDetailCount+1, 10)
		pplDetail.PipelineID = ppl.ID
		result = tx.Model(&PipelineDetail{}).Create(pplDetail)
		if result.Error != nil {
			logEntry.Errorf("create pipeline detail failed. pipeline detail:%+v, error:%v", pplDetail, result.Error)
			return result.Error
		}

		logEntry.Infof("created ppl with pk[%d], pplID[%s], pplDetailPk[%d], pplDetailID[%s]", ppl.Pk, ppl.ID, pplDetail.Pk, pplDetail.ID)
		return nil
	})
	return ppl.ID, pplDetail.ID, err
}

func UpdatePipeline(logEntry *log.Entry, ppl *Pipeline, pplDetail *PipelineDetail) (pplID string, pplDetailID string, err error) {
	logEntry.Debugf("begin update pipeline: %+v and pipeline detail: %+v", ppl, pplDetail)
	err = WithTransaction(storage.DB, func(tx *gorm.DB) error {
		// update desc by pk
		result := tx.Model(&Pipeline{}).Where("pk = ?", ppl.Pk).Update("desc", ppl.Desc)
		if result.Error != nil {
			logEntry.Errorf("update desc to pipeline[%d] failed. error:%v", ppl.Pk, result.Error)
			return result.Error
		}

		var pplDetailCount int64
		tx = tx.Unscoped().Model(&PipelineDetail{}).Where("pipeline_id = ?", ppl.ID).Count(&pplDetailCount)
		if tx.Error != nil {
			logger.Logger().Errorf("count pipeline detail failed. pipelineID[%s]. error:%s",
				ppl.ID, tx.Error.Error())
			return tx.Error
		}

		pplDetail.ID = strconv.FormatInt(pplDetailCount+1, 10)
		pplDetail.PipelineID = ppl.ID
		result = tx.Create(pplDetail)
		if result.Error != nil {
			logEntry.Errorf("update pipeline failed. pipeline detail:%+v, error:%v", pplDetail, result.Error)
			return result.Error
		}
		logEntry.Debugf("updated ppl with pplID[%s], new pplDetailPk[%d], pplDetailID[%s]", pplDetail.PipelineID, pplDetail.Pk, pplDetail.ID)
		return nil
	})
	return pplDetail.PipelineID, pplDetail.ID, err
}

func GetPipelineByID(id string) (Pipeline, error) {
	var ppl Pipeline
	tx := storage.DB.Model(&Pipeline{})

	if id != "" {
		tx = tx.Where("id = ?", id)
	}

	result := tx.Last(&ppl)
	return ppl, result.Error
}

func GetPipeline(name, userName string) (Pipeline, error) {
	var ppl Pipeline
	result := storage.DB.Model(&Pipeline{}).Where(&Pipeline{Name: name, UserName: userName}).Last(&ppl)
	return ppl, result.Error
}

func ListPipeline(pk int64, maxKeys int, userFilter, nameFilter []string) ([]Pipeline, error) {
	logger.Logger().Debugf("begin list pipeline. ")
	tx := storage.DB.Model(&Pipeline{}).Where("pk > ?", pk)
	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
	}
	if len(nameFilter) > 0 {
		tx = tx.Where("name IN (?)", nameFilter)
	}
	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var pplList []Pipeline
	tx = tx.Find(&pplList)
	if tx.Error != nil {
		logger.Logger().Errorf("list pipeline failed. pk:%d, maxKeys:%d, Filters: user{%v}, name{%v}. error:%s",
			pk, maxKeys, userFilter, nameFilter, tx.Error.Error())
		return []Pipeline{}, tx.Error
	}
	return pplList, nil
}

func IsLastPipelinePk(logEntry *log.Entry, pk int64, userFilter, nameFilter []string) (bool, error) {
	logger.Logger().Debugf("begin check isLastPipeline.")
	tx := storage.DB.Model(&Pipeline{})
	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
	}
	if len(nameFilter) > 0 {
		tx = tx.Where("name IN (?)", nameFilter)
	}

	ppl := Pipeline{}
	tx = tx.Last(&ppl)
	if tx.Error != nil {
		logEntry.Errorf("get last ppl failed. Filters: user{%v}, name{%v}, error:%s", userFilter, nameFilter, tx.Error.Error())
		return false, tx.Error
	}
	return pk == ppl.Pk, nil
}

func DeletePipeline(logEntry *log.Entry, id string) error {
	logEntry.Debugf("delete ppl: %s", id)
	result := storage.DB.Where("id = ?", id).Delete(&Pipeline{})
	return result.Error
}
