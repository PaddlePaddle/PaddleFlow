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

package models

import (
	"encoding/json"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
)

type FSCacheConfig struct {
	PK                      int64                  `json:"-"                   gorm:"primaryKey;autoIncrement"`
	FsID                    string                 `json:"fsID"                gorm:"type:varchar(36);unique_index"`
	CacheDir                string                 `json:"cacheDir"`
	Quota                   int                    `json:"quota"`
	MetaDriver              string                 `json:"metaDriver"`
	BlockSize               int                    `json:"blockSize"`
	NodeAffinityJson        string                 `json:"-"                   gorm:"column:node_affinity;type:text;default:'{}'"`
	NodeAffinityMap         map[string]interface{} `json:"nodeAffinity"        gorm:"-"`
	NodeTaintTolerationJson string                 `json:"-"                   gorm:"column:node_tainttoleration;type:text;default:'{}'"`
	NodeTaintTolerationMap  map[string]interface{} `json:"nodeTaintToleration" gorm:"-"`
	ExtraConfigJson         string                 `json:"-"                   gorm:"column:extra_config;type:text;default:'{}'"`
	ExtraConfigMap          map[string]string      `json:"extraConfig"         gorm:"-"`
	CreatedAt               time.Time              `json:"createTime"`
	UpdatedAt               time.Time              `json:"updateTime,omitempty"`
	DeletedAt               gorm.DeletedAt         `json:"deleteTime,omitempty"`
}

func (s *FSCacheConfig) TableName() string {
	return "fs_cache_config"
}

func (s *FSCacheConfig) AfterFind(*gorm.DB) error {
	if s.NodeAffinityJson != "" {
		s.NodeAffinityMap = make(map[string]interface{})
		if err := json.Unmarshal([]byte(s.NodeAffinityJson), &s.NodeAffinityMap); err != nil {
			log.Errorf("json Unmarshal nodeAffinityJson[%s] failed: %v", s.NodeAffinityJson, err)
			return err
		}
	}
	if s.NodeTaintTolerationJson != "" {
		s.NodeTaintTolerationMap = make(map[string]interface{})
		if err := json.Unmarshal([]byte(s.NodeTaintTolerationJson), &s.NodeTaintTolerationMap); err != nil {
			log.Errorf("json Unmarshal nodeTainttolerationJson[%s] failed: %v", s.ExtraConfigJson, err)
			return err
		}
	}
	if s.ExtraConfigJson != "" {
		s.ExtraConfigMap = make(map[string]string)
		if err := json.Unmarshal([]byte(s.ExtraConfigJson), &s.ExtraConfigMap); err != nil {
			log.Errorf("json Unmarshal extraConfigJson[%s] failed: %v", s.ExtraConfigJson, err)
			return err
		}
	}

	return nil
}

func (s *FSCacheConfig) BeforeSave(*gorm.DB) error {
	nodeAffinityMap, err := json.Marshal(&s.NodeAffinityMap)
	if err != nil {
		log.Errorf("json Marshal nodeAffinityMap[%v] failed: %v", s.NodeAffinityMap, err)
		return err
	}
	s.NodeAffinityJson = string(nodeAffinityMap)

	nodeTaintMap, err := json.Marshal(&s.NodeTaintTolerationMap)
	if err != nil {
		log.Errorf("json Marshal nodeTaintMap[%v] failed: %v", s.NodeTaintTolerationMap, err)
		return err
	}
	s.NodeTaintTolerationJson = string(nodeTaintMap)

	extraConfigMap, err := json.Marshal(&s.ExtraConfigMap)
	if err != nil {
		log.Errorf("json Marshal extraConfigMap[%v] failed: %v", s.ExtraConfigMap, err)
		return err
	}
	s.ExtraConfigJson = string(extraConfigMap)
	return nil
}

func CreateFSCacheConfig(logEntry *log.Entry, fsCacheConfig *FSCacheConfig) error {
	logEntry.Debugf("begin create fsCacheConfig:%+v", fsCacheConfig)
	err := database.DB.Model(&FSCacheConfig{}).Create(fsCacheConfig).Error
	if err != nil {
		logEntry.Errorf("create fsCacheConfig failed. fsCacheConfig:%v, error:%s",
			fsCacheConfig, err.Error())
		return err
	}
	return nil
}

func UpdateFSCacheConfig(logEntry *log.Entry, fsCacheConfig FSCacheConfig) error {
	logEntry.Debugf("begin update fsCacheConfig fsCacheConfig. fsID:%s", fsCacheConfig.FsID)
	tx := database.DB.Model(&FSCacheConfig{}).Where(&FSCacheConfig{FsID: fsCacheConfig.FsID}).Updates(fsCacheConfig)
	if tx.Error != nil {
		logEntry.Errorf("update fsCacheConfig failed. fsCacheConfig.ID:%s, error:%s",
			fsCacheConfig.FsID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func DeleteFSCacheConfig(logEntry *log.Entry, fsID string) error {
	logEntry.Debugf("begin delete fsCacheConfig. fsID:%s", fsID)
	tx := database.DB.Model(&FSCacheConfig{}).Unscoped().Where(&FSCacheConfig{FsID: fsID}).Delete(&FSCacheConfig{})
	if tx.Error != nil {
		logEntry.Errorf("delete fsCacheConfig failed. fsID:%s, error:%s",
			fsID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func GetFSCacheConfig(logEntry *log.Entry, fsID string) (FSCacheConfig, error) {
	logEntry.Debugf("begin get fsCacheConfig. fsID:%s", fsID)
	var fsCacheConfig FSCacheConfig
	tx := database.DB.Model(&FSCacheConfig{}).Where(&FSCacheConfig{FsID: fsID}).First(&fsCacheConfig)
	if tx.Error != nil {
		logEntry.Errorf("get fsCacheConfig failed. fsID:%s, error:%s",
			fsID, tx.Error.Error())
		return FSCacheConfig{}, tx.Error
	}
	return fsCacheConfig, nil
}
