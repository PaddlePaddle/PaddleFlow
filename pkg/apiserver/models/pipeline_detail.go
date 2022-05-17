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
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type PipelineDetail struct {
	Pk           int64          `json:"pipelineDetailPk"     gorm:"primaryKey;autoIncrement;not null"`
	PipelineID   string         `json:"pipelineID"           gorm:"type:varchar(60);not null"`
	DetailType   string         `json:"detailType"           gorm:"type:varchar(60);not null"`
	FsID         string         `json:"-"                    gorm:"type:varchar(60);not null"`
	FsName       string         `json:"fsname"               gorm:"type:varchar(60);not null"`
	YamlPath     string         `json:"yamlPath"             gorm:"type:text;size:65535"`
	PipelineYaml string         `json:"pipelineYaml"         gorm:"type:text;size:65535"`
	PipelineMd5  string         `json:"pipelineMd5"          gorm:"type:varchar(32);not null"`
	UserName     string         `json:"username"             gorm:"type:varchar(60);not null"`
	CreatedAt    time.Time      `json:"-"`
	UpdatedAt    time.Time      `json:"-"`
	DeletedAt    gorm.DeletedAt `json:"-"                    gorm:"index"`
	Pipeline     Pipeline       `json:"-"                    gorm:"references:ID"`
}

func (PipelineDetail) TableName() string {
	return "PipelineDetail"
}

func ListPipelineDetail(PipelineID string, pk int64, maxKeys int, fsFilter, detailTypeFilter []string) ([]PipelineDetail, error) {
	logger.Logger().Debugf("begin list pipeline detail. ")
	tx := database.DB.Model(&PipelineDetail{}).Where("pk > ?", pk).Where("pipeline_id = ?", PipelineID)
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}
	if len(detailTypeFilter) > 0 {
		tx = tx.Where("detail_type IN (?)", detailTypeFilter)
	}

	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var pplDetailList []PipelineDetail
	tx = tx.Find(&pplDetailList)
	if tx.Error != nil {
		logger.Logger().Errorf("list pipeline detail failed. pk:%d, maxKeys:%d, Filters: fs{%v}, detailtype{%v}. error:%s",
			pk, maxKeys, fsFilter, detailTypeFilter, tx.Error.Error())
		return []PipelineDetail{}, tx.Error
	}
	return pplDetailList, nil
}

func CountPipelineDetail(pipelineID string, detailTypeFilter []string) (int64, error) {
	logger.Logger().Debugf("begin to count pipeline detail for pipeline[%s].", pipelineID)
	tx := database.DB.Model(&PipelineDetail{}).Where("pipeline_id = ?", pipelineID)
	if len(detailTypeFilter) > 0 {
		tx = tx.Where("detail_type IN (?)", detailTypeFilter)
	}

	var count int64
	tx = tx.Count(&count)
	if tx.Error != nil {
		logger.Logger().Errorf("count pipeline detail failed. pipelineID[%s] detailtype{%v}. error:%s",
			pipelineID, detailTypeFilter, tx.Error.Error())
		return count, tx.Error
	}
	return count, nil
}

func GetLastPipelineDetail(logEntry *log.Entry, pipelineID string) (PipelineDetail, error) {
	logEntry.Debugf("get last ppl detail for ppl[%s]", pipelineID)
	pplDetail := PipelineDetail{}
	tx := database.DB.Model(&PipelineDetail{})

	if pipelineID != "" {
		tx = tx.Where("pipeline_id = ?", pipelineID)
	}

	tx = tx.Last(&pplDetail)
	if tx.Error != nil {
		logEntry.Errorf("get last ppl detail failed. error:%s", tx.Error.Error())
		return PipelineDetail{}, tx.Error
	}
	return pplDetail, nil
}

// 此处不检查pipeline detail对应的pipelineID是否存在，单纯做查询
func GetPipelineDetailByID(pipelineDetailPk int64) (PipelineDetail, error) {
	var pplDetail PipelineDetail
	result := database.DB.Model(&PipelineDetail{}).Where("Pk = ?", pipelineDetailPk).Last(&pplDetail)
	return pplDetail, result.Error
}

func GetPipelineDetail(pipelineID string, pipelineDetailPk int64, detailType string) ([]PipelineDetail, error) {
	pplDetailList := []PipelineDetail{}
	tx := database.DB.Model(&PipelineDetail{})

	if pipelineID != "" {
		tx.Where("pipeline_id = ?", pipelineID)
	}

	if pipelineDetailPk != 0 {
		tx.Where("pk = ?", pipelineDetailPk)
	}

	if detailType != "" {
		tx = tx.Where("detail_type = ?", detailType)
	}
	tx = tx.Preload("Pipeline").Find(&pplDetailList)
	return pplDetailList, tx.Error
}

func DeletePipelineDetail(logEntry *log.Entry, pipelineDetailPk int64, hardDelete bool) error {
	logEntry.Debugf("delete pipelineDetailPk: %d, hardDelete[%t]", pipelineDetailPk, hardDelete)
	if hardDelete {
		result := database.DB.Unscoped().Where("Pk = ?", pipelineDetailPk).Delete(&PipelineDetail{})
		return result.Error
	} else {
		result := database.DB.Where("Pk = ?", pipelineDetailPk).Delete(&PipelineDetail{})
		return result.Error
	}
}
