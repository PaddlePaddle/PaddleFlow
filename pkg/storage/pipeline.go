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

package storage

import (
	"fmt"
	"strconv"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type PipelineStore struct {
	db *gorm.DB
}

func newPipelineStore(db *gorm.DB) *PipelineStore {
	return &PipelineStore{db: db}
}

func (ps *PipelineStore) CreatePipeline(logEntry *log.Entry, ppl *model.Pipeline, pplVersion *model.PipelineVersion) (pplID string, pplVersionID string, err error) {
	logEntry.Debugf("begin create pipeline: %+v & pipeline version: %+v", ppl, pplVersion)
	err = ps.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&model.Pipeline{}).Create(ppl)
		if result.Error != nil {
			logEntry.Errorf("create pipeline failed. pipeline:%+v, error:%v", ppl, result.Error)
			return result.Error
		}
		// update ID by pk
		ppl.ID = common.PrefixPipeline + fmt.Sprintf("%06d", ppl.Pk)
		result = tx.Model(&model.Pipeline{}).Where("pk = ?", ppl.Pk).Update("id", ppl.ID)
		if result.Error != nil {
			logEntry.Errorf("backfilling pplID to pipeline[%d] failed. error:%v", ppl.Pk, result.Error)
			return result.Error
		}

		var pplVersionCount int64
		tx = tx.Unscoped().Model(&model.PipelineVersion{}).Where("pipeline_id = ?", ppl.ID).Count(&pplVersionCount)
		if tx.Error != nil {
			logger.Logger().Errorf("count pipeline version failed. pipelineID[%s]. error:%s",
				ppl.ID, tx.Error.Error())
			return tx.Error
		}
		pplVersion.ID = strconv.FormatInt(pplVersionCount+1, 10)
		pplVersion.PipelineID = ppl.ID
		result = tx.Model(&model.PipelineVersion{}).Create(pplVersion)
		if result.Error != nil {
			logEntry.Errorf("create pipeline version failed. pipeline version:%+v, error:%v", pplVersion, result.Error)
			return result.Error
		}

		logEntry.Infof("created ppl with pk[%d], pplID[%s], pplVersionPk[%d], pplVersionID[%s]", ppl.Pk, ppl.ID, pplVersion.Pk, pplVersion.ID)
		return nil
	})
	return ppl.ID, pplVersion.ID, err
}

func (ps *PipelineStore) UpdatePipeline(logEntry *log.Entry, ppl *model.Pipeline, pplVersion *model.PipelineVersion) (pplID string, pplVersionID string, err error) {
	logEntry.Debugf("begin update pipeline: %+v and pipeline version: %+v", ppl, pplVersion)
	err = ps.db.Transaction(func(tx *gorm.DB) error {
		// update desc by pk
		result := tx.Model(&model.Pipeline{}).Where("pk = ?", ppl.Pk).Update("desc", ppl.Desc)
		if result.Error != nil {
			logEntry.Errorf("update desc to pipeline[%d] failed. error:%v", ppl.Pk, result.Error)
			return result.Error
		}

		var pplVersionCount int64
		tx = tx.Unscoped().Model(&model.PipelineVersion{}).Where("pipeline_id = ?", ppl.ID).Count(&pplVersionCount)
		if tx.Error != nil {
			logger.Logger().Errorf("count pipeline version failed. pipelineID[%s]. error:%s",
				ppl.ID, tx.Error.Error())
			return tx.Error
		}

		pplVersion.ID = strconv.FormatInt(pplVersionCount+1, 10)
		pplVersion.PipelineID = ppl.ID
		result = tx.Create(pplVersion)
		if result.Error != nil {
			logEntry.Errorf("update pipeline failed. pipeline version:%+v, error:%v", pplVersion, result.Error)
			return result.Error
		}
		logEntry.Debugf("updated ppl with pplID[%s], new pplVersionPk[%d], pplVersionID[%s]", pplVersion.PipelineID, pplVersion.Pk, pplVersion.ID)
		return nil
	})
	return pplVersion.PipelineID, pplVersion.ID, err
}

func (ps *PipelineStore) GetPipelineByID(id string) (model.Pipeline, error) {
	var ppl model.Pipeline
	tx := ps.db.Model(&model.Pipeline{})

	if id != "" {
		tx = tx.Where("id = ?", id)
	}

	result := tx.Last(&ppl)
	return ppl, result.Error
}

func (ps *PipelineStore) GetPipeline(name, userName string) (model.Pipeline, error) {
	var ppl model.Pipeline
	result := ps.db.Model(&model.Pipeline{}).Where(&model.Pipeline{Name: name, UserName: userName}).Last(&ppl)
	return ppl, result.Error
}

func (ps *PipelineStore) ListPipeline(pk int64, maxKeys int, userFilter, nameFilter []string) ([]model.Pipeline, error) {
	logger.Logger().Debugf("begin list pipeline. ")
	tx := ps.db.Model(&model.Pipeline{}).Where("pk > ?", pk)
	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
	}
	if len(nameFilter) > 0 {
		tx = tx.Where("name IN (?)", nameFilter)
	}
	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var pplList []model.Pipeline
	tx = tx.Find(&pplList)
	if tx.Error != nil {
		logger.Logger().Errorf("list pipeline failed. pk:%d, maxKeys:%d, Filters: user{%v}, name{%v}. error:%s",
			pk, maxKeys, userFilter, nameFilter, tx.Error.Error())
		return []model.Pipeline{}, tx.Error
	}
	return pplList, nil
}

func (ps *PipelineStore) IsLastPipelinePk(logEntry *log.Entry, pk int64, userFilter, nameFilter []string) (bool, error) {
	logger.Logger().Debugf("begin check isLastPipeline.")
	tx := ps.db.Model(&model.Pipeline{})
	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
	}
	if len(nameFilter) > 0 {
		tx = tx.Where("name IN (?)", nameFilter)
	}

	ppl := model.Pipeline{}
	tx = tx.Last(&ppl)
	if tx.Error != nil {
		logEntry.Errorf("get last ppl failed. Filters: user{%v}, name{%v}, error:%s", userFilter, nameFilter, tx.Error.Error())
		return false, tx.Error
	}
	return pk == ppl.Pk, nil
}

func (ps *PipelineStore) DeletePipeline(logEntry *log.Entry, id string) error {
	logEntry.Debugf("delete ppl: %s", id)
	result := ps.db.Where("id = ?", id).Delete(&model.Pipeline{})
	return result.Error
}

// ======================================== pipeline_version =========================================

func (ps *PipelineStore) ListPipelineVersion(pipelineID string, pk int64, maxKeys int, fsFilter []string) ([]model.PipelineVersion, error) {
	logger.Logger().Debugf("begin list pipeline version. ")
	tx := ps.db.Model(&model.PipelineVersion{}).Where("pk > ?", pk).Where("pipeline_id = ?", pipelineID)
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}

	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var pplVersionList []model.PipelineVersion
	tx = tx.Find(&pplVersionList)
	if tx.Error != nil {
		logger.Logger().Errorf("list pipeline version failed. pk:%d, maxKeys:%d, Filters: fs{%v}. error:%s",
			pk, maxKeys, fsFilter, tx.Error.Error())
		return []model.PipelineVersion{}, tx.Error
	}
	return pplVersionList, nil
}

func (ps *PipelineStore) IsLastPipelineVersionPk(logEntry *log.Entry, pipelineID string, pk int64, fsFilter []string) (bool, error) {
	logEntry.Debugf("get last ppl version for ppl[%s], Filters: fs{%v}", pipelineID, fsFilter)
	pplVersion := model.PipelineVersion{}
	tx := ps.db.Model(&model.PipelineVersion{}).Where("pipeline_id = ?", pipelineID)
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

func (ps *PipelineStore) CountPipelineVersion(pipelineID string) (int64, error) {
	logger.Logger().Debugf("begin to count pipeline version for pipeline[%s].", pipelineID)
	tx := ps.db.Model(&model.PipelineVersion{}).Where("pipeline_id = ?", pipelineID)

	var count int64
	tx = tx.Count(&count)
	if tx.Error != nil {
		logger.Logger().Errorf("count pipeline version failed. pipelineID[%s]. error:%s",
			pipelineID, tx.Error.Error())
		return count, tx.Error
	}
	return count, nil
}

func (ps *PipelineStore) GetPipelineVersions(pipelineID string) ([]model.PipelineVersion, error) {
	pplVersionList := []model.PipelineVersion{}
	tx := ps.db.Model(&model.PipelineVersion{})

	if pipelineID != "" {
		tx.Where("pipeline_id = ?", pipelineID)
	}

	tx = tx.Find(&pplVersionList)
	return pplVersionList, tx.Error
}

func (ps *PipelineStore) GetPipelineVersion(pipelineID string, pipelineVersionID string) (model.PipelineVersion, error) {
	pplVersion := model.PipelineVersion{}
	tx := ps.db.Model(&model.PipelineVersion{}).Where("pipeline_id = ?", pipelineID).Where("id = ?", pipelineVersionID).Last(&pplVersion)
	return pplVersion, tx.Error
}

func (ps *PipelineStore) GetLastPipelineVersion(pipelineID string) (model.PipelineVersion, error) {
	pplVersion := model.PipelineVersion{}
	tx := ps.db.Model(&model.PipelineVersion{}).Where("pipeline_id = ?", pipelineID).Last(&pplVersion)
	return pplVersion, tx.Error
}

func (ps *PipelineStore) DeletePipelineVersion(logEntry *log.Entry, pipelineID string, pipelineVersionID string) error {
	logEntry.Debugf("delete pipeline[%s] versionID[%s]", pipelineID, pipelineVersionID)
	result := ps.db.Model(&model.PipelineVersion{}).Where("pipeline_id = ?", pipelineID).Where("id = ?", pipelineVersionID).Delete(&model.PipelineVersion{})
	return result.Error
}
