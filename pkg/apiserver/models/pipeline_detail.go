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
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type PipelineDetail struct {
	Pk           int64          `json:"-"                    gorm:"primaryKey;autoIncrement;not null"`
	ID           string         `json:"pipelineDetailID"     gorm:"type:varchar(60);not null"`
	PipelineID   string         `json:"pipelineID"           gorm:"type:varchar(60);not null"`
	FsID         string         `json:"-"                    gorm:"type:varchar(60);not null"`
	FsName       string         `json:"fsname"               gorm:"type:varchar(60);not null"`
	YamlPath     string         `json:"yamlPath"             gorm:"type:text;size:65535;not null"`
	PipelineYaml string         `json:"pipelineYaml"         gorm:"type:text;size:65535;not null"`
	PipelineMd5  string         `json:"pipelineMd5"          gorm:"type:varchar(32);not null"`
	UserName     string         `json:"username"             gorm:"type:varchar(60);not null"`
	CreatedAt    time.Time      `json:"-"`
	UpdatedAt    time.Time      `json:"-"`
	DeletedAt    gorm.DeletedAt `json:"-"`
}

func (PipelineDetail) TableName() string {
	return "pipeline_detail"
}

func ListPipelineDetail(pipelineID string, pk int64, maxKeys int, fsFilter []string) ([]PipelineDetail, error) {
	logger.Logger().Debugf("begin list pipeline detail. ")
	tx := storage.DB.Model(&PipelineDetail{}).Where("pk > ?", pk).Where("pipeline_id = ?", pipelineID)
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}

	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var pplDetailList []PipelineDetail
	tx = tx.Find(&pplDetailList)
	if tx.Error != nil {
		logger.Logger().Errorf("list pipeline detail failed. pk:%d, maxKeys:%d, Filters: fs{%v}. error:%s",
			pk, maxKeys, fsFilter, tx.Error.Error())
		return []PipelineDetail{}, tx.Error
	}
	return pplDetailList, nil
}

func IsLastPipelineDetailPk(logEntry *log.Entry, pipelineID string, pk int64, fsFilter []string) (bool, error) {
	logEntry.Debugf("get last ppl detail for ppl[%s], Filters: fs{%v}", pipelineID, fsFilter)
	pplDetail := PipelineDetail{}
	tx := storage.DB.Model(&PipelineDetail{}).Where("pipeline_id = ?", pipelineID)
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}

	tx = tx.Last(&pplDetail)
	if tx.Error != nil {
		logEntry.Errorf("get last ppl detail failed. error:%s", tx.Error.Error())
		return false, tx.Error
	}

	return pplDetail.Pk == pk, nil
}

func CountPipelineDetail(pipelineID string) (int64, error) {
	logger.Logger().Debugf("begin to count pipeline detail for pipeline[%s].", pipelineID)
	tx := storage.DB.Model(&PipelineDetail{}).Where("pipeline_id = ?", pipelineID)

	var count int64
	tx = tx.Count(&count)
	if tx.Error != nil {
		logger.Logger().Errorf("count pipeline detail failed. pipelineID[%s]. error:%s",
			pipelineID, tx.Error.Error())
		return count, tx.Error
	}
	return count, nil
}

func GetPipelineDetails(pipelineID string) ([]PipelineDetail, error) {
	pplDetailList := []PipelineDetail{}
	tx := storage.DB.Model(&PipelineDetail{})

	if pipelineID != "" {
		tx.Where("pipeline_id = ?", pipelineID)
	}

	tx = tx.Find(&pplDetailList)
	return pplDetailList, tx.Error
}

func GetPipelineDetail(pipelineID string, pipelineDetailID string) (PipelineDetail, error) {
	pplDetail := PipelineDetail{}
	tx := storage.DB.Model(&PipelineDetail{}).Where("pipeline_id = ?", pipelineID).Where("id = ?", pipelineDetailID).Last(&pplDetail)
	return pplDetail, tx.Error
}

func GetLastPipelineDetail(pipelineID string) (PipelineDetail, error) {
	pplDetail := PipelineDetail{}
	tx := storage.DB.Model(&PipelineDetail{}).Where("pipeline_id = ?", pipelineID).Last(&pplDetail)
	return pplDetail, tx.Error
}

func DeletePipelineDetail(logEntry *log.Entry, pipelineID string, pipelineDetailID string) error {
	logEntry.Debugf("delete pipeline[%s] detailID[%s]", pipelineID, pipelineDetailID)

	tx := storage.DB
	result := tx.Model(&PipelineDetail{}).Where("pipeline_id = ?", pipelineID).Where("id = ?", pipelineDetailID).Delete(&PipelineDetail{})
	return result.Error
}
