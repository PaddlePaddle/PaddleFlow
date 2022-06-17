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
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/models"
)

func CreateArtifactEvent(logEntry *log.Entry, artifact models.ArtifactEvent) error {
	logEntry.Debugf("begin create artifact: %+v", artifact)
	tx := database.DB.Model(&models.ArtifactEvent{}).Create(&artifact)
	if tx.Error != nil {
		logEntry.Errorf("create artifact: %v failed. error:%v", artifact, tx.Error)
		return tx.Error
	}
	return nil
}

func CountArtifactEvent(logEntry *log.Entry, fsID, artifactPath string) (int64, error) {
	logEntry.Debugf("get artifact count. fsID:%s, artifactPath: %s", fsID, artifactPath)
	var count int64
	tx := database.DB.Model(&models.ArtifactEvent{}).Where(&models.ArtifactEvent{FsID: fsID, ArtifactPath: artifactPath}).Count(&count)
	if tx.Error != nil {
		logEntry.Errorf("get artifact count failed. error:%s", tx.Error.Error())
		return 0, tx.Error
	}
	return count, nil
}

func GetArtifactEvent(logEntry *log.Entry, runID, fsID, artifactPath string) (models.ArtifactEvent, error) {
	logEntry.Debugf("begin get artifact. runID:%s, fsID:%s, artifactPath:%s", runID, fsID, artifactPath)
	var artifact models.ArtifactEvent
	tx := database.DB.Model(&models.ArtifactEvent{}).Where(&models.ArtifactEvent{RunID: runID, FsID: fsID, ArtifactPath: artifactPath}).Last(&artifact)
	if tx.Error != nil {
		logEntry.Errorf("get artifact failed. runID:%s, fsID:%s, artifactPath:%s. error:%v", runID, fsID, artifactPath, tx.Error)
		return models.ArtifactEvent{}, tx.Error
	}
	return artifact, nil
}

func UpdateArtifactEvent(logEntry *log.Entry, fsID, artifactPath string, artifact models.ArtifactEvent) error {
	logEntry.Debugf("begin update artifact. fsID:%s, artifactPath:%s", fsID, artifactPath)
	tx := database.DB.Model(&models.ArtifactEvent{}).Where("fs_id = ? AND artifact_path = ?", fsID, artifactPath).Updates(artifact)
	if tx.Error != nil {
		logEntry.Errorf("update artifact failed. fsID:%s, artifactPath:%s, error:%s",
			fsID, artifactPath, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func DeleteArtifactEvent(logEntry *log.Entry, username, fsname, runID, artifactPath string) error {
	logEntry.Debugf("begin delete artifact_event username:%s, fsname:%s, runID:%s, artifactPath:%s", username, fsname, runID, artifactPath)
	tx := database.DB.Model(&models.ArtifactEvent{}).Unscoped().Where(
		&models.ArtifactEvent{UserName: username, FsName: fsname, RunID: runID, ArtifactPath: artifactPath}).Delete(&models.ArtifactEvent{})
	if tx.Error != nil {
		logEntry.Errorf("delete artifact failed. username:%s, fsname:%s, runID:%s, artifactPath:%s. error:%v",
			username, fsname, runID, artifactPath, tx.Error)
		return tx.Error
	}
	return nil
}

func ListArtifactEvent(logEntry *log.Entry, pk int64, maxKeys int, userFilter, fsFilter, runFilter, typeFilter, pathFilter []string) ([]models.ArtifactEvent, error) {
	logEntry.Debugf("begin list artifact. Filters: user{%v}, fs{%v}, run{%v}, type{%v}, path{%v}",
		userFilter, fsFilter, runFilter, typeFilter, pathFilter)
	tx := database.DB.Model(&models.ArtifactEvent{}).Where("pk > ?", pk)
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
	var artifactList []models.ArtifactEvent
	tx = tx.Find(&artifactList)
	if tx.Error != nil {
		logEntry.Errorf("list artifact failed. Filters: user{%v}, fs{%v}, run{%v}, type{%v}, path{%v}. error:%v",
			userFilter, fsFilter, runFilter, typeFilter, pathFilter, tx.Error)
		return []models.ArtifactEvent{}, tx.Error
	}
	return artifactList, nil
}

func GetLastArtifactEvent(logEntry *log.Entry) (models.ArtifactEvent, error) {
	logEntry.Debugf("get last ArtifactEvent")
	art := models.ArtifactEvent{}
	tx := database.DB.Model(&models.ArtifactEvent{}).Last(&art)
	if tx.Error != nil {
		logEntry.Errorf("get last ArtifactEvent failed. error:%s", tx.Error.Error())
		return models.ArtifactEvent{}, tx.Error
	}
	return art, nil
}
