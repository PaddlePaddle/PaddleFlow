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

type PipelineVersion struct {
	Pk           int64          `json:"-"                    gorm:"primaryKey;autoIncrement;not null"`
	ID           string         `json:"pipelineVersionID"    gorm:"type:varchar(60);not null"`
	PipelineID   string         `json:"pipelineID"           gorm:"type:varchar(60);not null"`
	FsID         string         `json:"-"                    gorm:"type:varchar(60);not null"`
	FsName       string         `json:"fsName"               gorm:"type:varchar(60);not null"`
	YamlPath     string         `json:"yamlPath"             gorm:"type:text;size:65535;not null"`
	PipelineYaml string         `json:"pipelineYaml"         gorm:"type:text;size:65535;not null"`
	PipelineMd5  string         `json:"pipelineMd5"          gorm:"type:varchar(32);not null"`
	UserName     string         `json:"username"             gorm:"type:varchar(60);not null"`
	CreatedAt    time.Time      `json:"-"`
	UpdatedAt    time.Time      `json:"-"`
	DeletedAt    gorm.DeletedAt `json:"-"`
}

func (PipelineVersion) TableName() string {
	return "pipeline_version"
}

func ListPipelineVersion(pipelineID string, pk int64, maxKeys int, fsFilter []string) ([]PipelineVersion, error) {
	logger.Logger().Debugf("begin list pipeline version. ")
	tx := storage.DB.Model(&PipelineVersion{}).Where("pk > ?", pk).Where("pipeline_id = ?", pipelineID)
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}

	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var pplVersionList []PipelineVersion
	tx = tx.Find(&pplVersionList)
	if tx.Error != nil {
		logger.Logger().Errorf("list pipeline version failed. pk:%d, maxKeys:%d, Filters: fs{%v}. error:%s",
			pk, maxKeys, fsFilter, tx.Error.Error())
		return []PipelineVersion{}, tx.Error
	}
	return pplVersionList, nil
}

func IsLastPipelineVersionPk(logEntry *log.Entry, pipelineID string, pk int64, fsFilter []string) (bool, error) {
	logEntry.Debugf("get last ppl version for ppl[%s], Filters: fs{%v}", pipelineID, fsFilter)
	pplVersion := PipelineVersion{}
	tx := storage.DB.Model(&PipelineVersion{}).Where("pipeline_id = ?", pipelineID)
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}

	tx = tx.Last(&pplVersion)
	if tx.Error != nil {
		logEntry.Errorf("get last ppl version failed. error:%s", tx.Error.Error())
		return false, tx.Error
	}

	return pplVersion.Pk == pk, nil
}

func CountPipelineVersion(pipelineID string) (int64, error) {
	logger.Logger().Debugf("begin to count pipeline version for pipeline[%s].", pipelineID)
	tx := storage.DB.Model(&PipelineVersion{}).Where("pipeline_id = ?", pipelineID)

	var count int64
	tx = tx.Count(&count)
	if tx.Error != nil {
		logger.Logger().Errorf("count pipeline version failed. pipelineID[%s]. error:%s",
			pipelineID, tx.Error.Error())
		return count, tx.Error
	}
	return count, nil
}

func GetPipelineVersions(pipelineID string) ([]PipelineVersion, error) {
	pplVersionList := []PipelineVersion{}
	tx := storage.DB.Model(&PipelineVersion{})

	if pipelineID != "" {
		tx.Where("pipeline_id = ?", pipelineID)
	}

	tx = tx.Find(&pplVersionList)
	return pplVersionList, tx.Error
}

func GetPipelineVersion(pipelineID string, pipelineVersionID string) (PipelineVersion, error) {
	pplVersion := PipelineVersion{}
	tx := storage.DB.Model(&PipelineVersion{}).Where("pipeline_id = ?", pipelineID).Where("id = ?", pipelineVersionID).Last(&pplVersion)
	return pplVersion, tx.Error
}

func GetLastPipelineVersion(pipelineID string) (PipelineVersion, error) {
	pplVersion := PipelineVersion{}
	tx := storage.DB.Model(&PipelineVersion{}).Where("pipeline_id = ?", pipelineID).Last(&pplVersion)
	return pplVersion, tx.Error
}

func DeletePipelineVersion(logEntry *log.Entry, pipelineID string, pipelineVersionID string) error {
	logEntry.Debugf("delete pipeline[%s] versionID[%s]", pipelineID, pipelineVersionID)

	tx := storage.DB
	result := tx.Model(&PipelineVersion{}).Where("pipeline_id = ?", pipelineID).Where("id = ?", pipelineVersionID).Delete(&PipelineVersion{})
	return result.Error
}
