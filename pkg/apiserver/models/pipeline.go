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
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

type Pipeline struct {
	Pk           int64          `json:"-"                    gorm:"primaryKey;autoIncrement;not null"`
	ID           string         `json:"pipelineID"           gorm:"type:varchar(60);not null;uniqueIndex"`
	Name         string         `json:"name"                 gorm:"type:varchar(60);not null;uniqueIndex:idx_fs_name"`
	FsID         string         `json:"-"                    gorm:"type:varchar(60);not null;uniqueIndex:idx_fs_name;uniqueIndex:idx_fs_md5"`
	FsName       string         `json:"fsname"               gorm:"type:varchar(60);not null"`
	UserName     string         `json:"username"             gorm:"type:varchar(60);not null"`
	PipelineYaml string         `json:"pipelineYaml"         gorm:"type:text;size:65535"`
	PipelineMd5  string         `json:"pipelineMd5"          gorm:"type:varchar(32);not null;uniqueIndex:idx_fs_md5"`
	CreateTime   string         `json:"createTime,omitempty" gorm:"-"`
	UpdateTime   string         `json:"updateTime,omitempty" gorm:"-"`
	CreatedAt    time.Time      `json:"-"`
	UpdatedAt    time.Time      `json:"-"`
	DeletedAt    gorm.DeletedAt `json:"-"                    gorm:"index"`
}

func (Pipeline) TableName() string {
	return "pipeline"
}

func (p *Pipeline) Decode() error {
	// format time
	p.CreateTime = p.CreatedAt.Format("2006-01-02 15:04:05")
	p.UpdateTime = p.UpdatedAt.Format("2006-01-02 15:04:05")
	return nil
}

func CreatePipeline(logEntry *log.Entry, ppl *Pipeline) (string, error) {
	logEntry.Debugf("begin create pipeline: %+v", ppl)
	err := withTransaction(database.DB, func(tx *gorm.DB) error {
		result := tx.Model(&Pipeline{}).Create(ppl)
		if result.Error != nil {
			logEntry.Errorf("create pipeline failed. pipeline:%+v, error:%v", ppl, result.Error)
			return result.Error
		}
		ppl.ID = common.PrefixPipeline + fmt.Sprintf("%06d", ppl.Pk)
		logEntry.Debugf("created ppl with pk[%d], pplID[%s]", ppl.Pk, ppl.ID)
		// update ID by pk
		result = tx.Model(&Pipeline{}).Where(&Pipeline{Pk: ppl.Pk}).Update("id", ppl.ID)
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
	result := database.DB.Model(&Pipeline{}).Where(&Pipeline{Name: name, FsID: fsID}).Count(&count)
	return count, result.Error
}

func CountPipelineByMd5InFs(md5, fsID string) (int64, error) {
	var count int64
	result := database.DB.Model(&Pipeline{}).Where(&Pipeline{PipelineMd5: md5, FsID: fsID}).Count(&count)
	return count, result.Error
}

func GetPipelineByMd5AndFs(md5, fsID string) (Pipeline, error) {
	var ppl Pipeline
	result := database.DB.Model(&Pipeline{}).Where(&Pipeline{FsID: fsID, PipelineMd5: md5}).Last(&ppl)
	return ppl, result.Error
}

func GetPipelineByID(id string) (Pipeline, error) {
	var ppl Pipeline
	result := database.DB.Model(&Pipeline{}).Where(&Pipeline{ID: id}).Last(&ppl)
	return ppl, result.Error
}

func GetPipelineByNameAndFs(fsID, name string) (Pipeline, error) {
	var ppl Pipeline
	result := database.DB.Model(&Pipeline{}).Where(&Pipeline{FsID: fsID, Name: name}).Last(&ppl)
	return ppl, result.Error
}

func ListPipeline(pk int64, maxKeys int, userFilter, fsFilter, nameFilter []string) ([]Pipeline, error) {
	logger.Logger().Debugf("begin list run. ")
	tx := database.DB.Model(&Pipeline{}).Where("pk > ?", pk)
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
	var pplList []Pipeline
	tx = tx.Find(&pplList)
	if tx.Error != nil {
		logger.Logger().Errorf("list run failed. Filters: user{%v}, fs{%v}, name{%v}. error:%s",
			userFilter, fsFilter, nameFilter, tx.Error.Error())
		return []Pipeline{}, tx.Error
	}
	return pplList, nil
}

func GetLastPipeline(logEntry *log.Entry) (Pipeline, error) {
	logEntry.Debugf("get last ppl. ")
	ppl := Pipeline{}
	tx := database.DB.Model(&Pipeline{}).Last(&ppl)
	if tx.Error != nil {
		logEntry.Errorf("get last ppl failed. error:%s", tx.Error.Error())
		return Pipeline{}, tx.Error
	}
	return ppl, nil
}

func HardDeletePipeline(logEntry *log.Entry, id string) error {
	logEntry.Debugf("delete ppl: %s", id)
	return database.DB.Unscoped().Where("id = ?", id).Delete(&Pipeline{}).Error
}
