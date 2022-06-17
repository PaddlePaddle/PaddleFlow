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

package db_service

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/models"
)

func CreatePipeline(logEntry *log.Entry, ppl *models.Pipeline) (string, error) {
	logEntry.Debugf("begin create pipeline: %+v", ppl)
	err := WithTransaction(database.DB, func(tx *gorm.DB) error {
		result := tx.Model(&models.Pipeline{}).Create(ppl)
		if result.Error != nil {
			logEntry.Errorf("create pipeline failed. pipeline:%+v, error:%v", ppl, result.Error)
			return result.Error
		}
		ppl.ID = schema.PrefixPipeline + fmt.Sprintf("%06d", ppl.Pk)
		logEntry.Debugf("created ppl with pk[%d], pplID[%s]", ppl.Pk, ppl.ID)
		// update ID by pk
		result = tx.Model(&models.Pipeline{}).Where(&models.Pipeline{Pk: ppl.Pk}).Update("id", ppl.ID)
		if result.Error != nil {
			logEntry.Errorf("back filling pplID failed. pk[%d], error:%v", ppl.Pk, result.Error)
			return result.Error
		}
		return nil
	})
	return ppl.ID, err
}

func CountPipelineByNameInFs(name, fsID string) (int64, error) {
	var count int64
	result := database.DB.Model(&models.Pipeline{}).Where(&models.Pipeline{Name: name, FsID: fsID}).Count(&count)
	return count, result.Error
}

func CountPipelineByMd5InFs(md5, fsID string) (int64, error) {
	var count int64
	result := database.DB.Model(&models.Pipeline{}).Where(&models.Pipeline{PipelineMd5: md5, FsID: fsID}).Count(&count)
	return count, result.Error
}

func GetPipelineByMd5AndFs(md5, fsID string) (models.Pipeline, error) {
	var ppl models.Pipeline
	result := database.DB.Model(&models.Pipeline{}).Where(&models.Pipeline{FsID: fsID, PipelineMd5: md5}).Last(&ppl)
	return ppl, result.Error
}

func GetPipelineByID(id string) (models.Pipeline, error) {
	var ppl models.Pipeline
	result := database.DB.Model(&models.Pipeline{}).Where(&models.Pipeline{ID: id}).Last(&ppl)
	return ppl, result.Error
}

func GetPipelineByNameAndFs(fsID, name string) (models.Pipeline, error) {
	var ppl models.Pipeline
	result := database.DB.Model(&models.Pipeline{}).Where(&models.Pipeline{FsID: fsID, Name: name}).Last(&ppl)
	return ppl, result.Error
}

func ListPipeline(pk int64, maxKeys int, userFilter, fsFilter, nameFilter []string) ([]models.Pipeline, error) {
	logger.Logger().Debugf("begin list run. ")
	tx := database.DB.Model(&models.Pipeline{}).Where("pk > ?", pk)
	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
	}
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}
	if len(nameFilter) > 0 {
		tx = tx.Where("name IN (?)", nameFilter)
	}
	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var pplList []models.Pipeline
	tx = tx.Find(&pplList)
	if tx.Error != nil {
		logger.Logger().Errorf("list run failed. Filters: user{%v}, fs{%v}, name{%v}. error:%s",
			userFilter, fsFilter, nameFilter, tx.Error.Error())
		return []models.Pipeline{}, tx.Error
	}
	return pplList, nil
}

func GetLastPipeline(logEntry *log.Entry) (models.Pipeline, error) {
	logEntry.Debugf("get last ppl. ")
	ppl := models.Pipeline{}
	tx := database.DB.Model(&models.Pipeline{}).Last(&ppl)
	if tx.Error != nil {
		logEntry.Errorf("get last ppl failed. error:%s", tx.Error.Error())
		return models.Pipeline{}, tx.Error
	}
	return ppl, nil
}

func HardDeletePipeline(logEntry *log.Entry, id string) error {
	logEntry.Debugf("delete ppl: %s", id)
	return database.DB.Unscoped().Where("id = ?", id).Delete(&models.Pipeline{}).Error
}
