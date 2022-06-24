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

	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type ArtifactEvent struct {
	Pk           int64          `json:"-"                    gorm:"primaryKey;autoIncrement;not null"`
	Md5          string         `json:"-"                    gorm:"type:varchar(32);not null"`
	RunID        string         `json:"runID"                gorm:"type:varchar(60);not null"`
	FsID         string         `json:"-"                    gorm:"type:varchar(60);not null"`
	FsName       string         `json:"fsname"               gorm:"type:varchar(60);not null"`
	UserName     string         `json:"username"             gorm:"type:varchar(60);not null"`
	ArtifactPath string         `json:"artifactPath"         gorm:"type:varchar(256);not null"`
	Step         string         `json:"step"                 gorm:"type:varchar(256);not null"`
	Type         string         `json:"type"                 gorm:"type:varchar(16);not null"`
	ArtifactName string         `json:"artifactName"         gorm:"type:varchar(32);not null"`
	Meta         string         `json:"meta"                 gorm:"type:text;size:65535"`
	CreateTime   string         `json:"createTime"           gorm:"-"`
	UpdateTime   string         `json:"updateTime,omitempty" gorm:"-"`
	CreatedAt    time.Time      `json:"-"`
	UpdatedAt    time.Time      `json:"-"`
	DeletedAt    gorm.DeletedAt `json:"-"                    gorm:"index"`
}

func (ArtifactEvent) TableName() string {
	return "artifact_event"
}

func (a *ArtifactEvent) decode() error {
	// format time
	a.CreateTime = a.CreatedAt.Format("2006-01-02 15:04:05")
	a.UpdateTime = a.UpdatedAt.Format("2006-01-02 15:04:05")
	return nil
}

func CreateArtifactEvent(logEntry *log.Entry, artifact ArtifactEvent) error {
	logEntry.Debugf("begin create artifact: %+v", artifact)
	tx := storage.DB.Model(&ArtifactEvent{}).Create(&artifact)
	if tx.Error != nil {
		logEntry.Errorf("create artifact: %v failed. error:%v", artifact, tx.Error)
		return tx.Error
	}
	return nil
}

func CountArtifactEvent(logEntry *log.Entry, fsID, artifactPath string) (int64, error) {
	logEntry.Debugf("get artifact count. fsID:%s, artifactPath: %s", fsID, artifactPath)
	var count int64
	tx := storage.DB.Model(&ArtifactEvent{}).Where(&ArtifactEvent{FsID: fsID, ArtifactPath: artifactPath}).Count(&count)
	if tx.Error != nil {
		logEntry.Errorf("get artifact count failed. error:%s", tx.Error.Error())
		return 0, tx.Error
	}
	return count, nil
}

func GetArtifactEvent(logEntry *log.Entry, runID, fsID, artifactPath string) (ArtifactEvent, error) {
	logEntry.Debugf("begin get artifact. runID:%s, fsID:%s, artifactPath:%s", runID, fsID, artifactPath)
	var artifact ArtifactEvent
	tx := storage.DB.Model(&ArtifactEvent{}).Where(&ArtifactEvent{RunID: runID, FsID: fsID, ArtifactPath: artifactPath}).Last(&artifact)
	if tx.Error != nil {
		logEntry.Errorf("get artifact failed. runID:%s, fsID:%s, artifactPath:%s. error:%v", runID, fsID, artifactPath, tx.Error)
		return ArtifactEvent{}, tx.Error
	}
	if err := artifact.decode(); err != nil {
		logEntry.Errorf("decode artifact failed. runID:%s, fsID:%s, artifactPath:%s. error:%v", runID, fsID, artifactPath, err)
		return ArtifactEvent{}, err
	}
	return artifact, nil
}

func UpdateArtifactEvent(logEntry *log.Entry, fsID, artifactPath string, artifact ArtifactEvent) error {
	logEntry.Debugf("begin update artifact. fsID:%s, artifactPath:%s", fsID, artifactPath)
	tx := storage.DB.Model(&ArtifactEvent{}).Where("fs_id = ? AND artifact_path = ?", fsID, artifactPath).Updates(artifact)
	if tx.Error != nil {
		logEntry.Errorf("update artifact failed. fsID:%s, artifactPath:%s, error:%s",
			fsID, artifactPath, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func DeleteArtifactEvent(logEntry *log.Entry, username, fsname, runID, artifactPath string) error {
	logEntry.Debugf("begin delete artifact_event username:%s, fsname:%s, runID:%s, artifactPath:%s", username, fsname, runID, artifactPath)
	tx := storage.DB.Model(&ArtifactEvent{}).Unscoped().Where(
		&ArtifactEvent{UserName: username, FsName: fsname, RunID: runID, ArtifactPath: artifactPath}).Delete(&ArtifactEvent{})
	if tx.Error != nil {
		logEntry.Errorf("delete artifact failed. username:%s, fsname:%s, runID:%s, artifactPath:%s. error:%v",
			username, fsname, runID, artifactPath, tx.Error)
		return tx.Error
	}
	return nil
}

func ListArtifactEvent(logEntry *log.Entry, pk int64, maxKeys int, userFilter, fsFilter, runFilter, typeFilter, pathFilter []string) ([]ArtifactEvent, error) {
	logEntry.Debugf("begin list artifact. Filters: user{%v}, fs{%v}, run{%v}, type{%v}, path{%v}",
		userFilter, fsFilter, runFilter, typeFilter, pathFilter)
	tx := storage.DB.Model(&ArtifactEvent{}).Where("pk > ?", pk)
	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
	}
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}
	if len(runFilter) > 0 {
		tx = tx.Where("run_id IN (?)", runFilter)
	}
	if len(typeFilter) > 0 {
		tx = tx.Where("type IN (?)", typeFilter)
	}
	if len(pathFilter) > 0 {
		tx = tx.Where("artifact_path IN (?)", pathFilter)
	}
	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var artifactList []ArtifactEvent
	tx = tx.Find(&artifactList)
	if tx.Error != nil {
		logEntry.Errorf("list artifact failed. Filters: user{%v}, fs{%v}, run{%v}, type{%v}, path{%v}. error:%v",
			userFilter, fsFilter, runFilter, typeFilter, pathFilter, tx.Error)
		return []ArtifactEvent{}, tx.Error
	}
	for index, _ := range artifactList {
		if err := artifactList[index].decode(); err != nil {
			logEntry.Errorf("decode artifact failed. runID:%s, fsID:%s, artifactPath:%s. error:%v",
				artifactList[index].RunID, artifactList[index].FsID, artifactList[index].ArtifactPath, err)
			return []ArtifactEvent{}, err
		}
	}
	return artifactList, nil
}

func GetLastArtifactEvent(logEntry *log.Entry) (ArtifactEvent, error) {
	logEntry.Debugf("get last ArtifactEvent")
	art := ArtifactEvent{}
	tx := storage.DB.Model(&ArtifactEvent{}).Last(&art)
	if tx.Error != nil {
		logEntry.Errorf("get last ArtifactEvent failed. error:%s", tx.Error.Error())
		return ArtifactEvent{}, tx.Error
	}
	if err := art.decode(); err != nil {
		logEntry.Errorf("decode artifact failed. runID:%s, fsID:%s, artifactPath:%s. error:%v", art.RunID, art.FsID, art.ArtifactPath, err)
		return ArtifactEvent{}, err
	}
	return art, nil
}
