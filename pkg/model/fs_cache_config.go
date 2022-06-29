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

package model

import (
	"encoding/json"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type FSCacheConfig struct {
	PK                      int64                  `json:"-"                   gorm:"primaryKey;autoIncrement"`
	FsID                    string                 `json:"fsID"                gorm:"type:varchar(36);unique_index"`
	CacheDir                string                 `json:"cacheDir"`
	Quota                   int                    `json:"quota"`
	MetaDriver              string                 `json:"metaDriver"`
	BlockSize               int                    `json:"blockSize"`
	Debug                   bool                   `json:"debug"`
	NodeAffinityJson        string                 `json:"-"                   gorm:"column:node_affinity;type:text;default:'{}'"`
	NodeAffinityMap         map[string]interface{} `json:"nodeAffinity"        gorm:"-"`
	NodeTaintTolerationJson string                 `json:"-"                   gorm:"column:node_tainttoleration;type:text;default:'{}'"`
	NodeTaintTolerationMap  map[string]interface{} `json:"nodeTaintToleration" gorm:"-"`
	ExtraConfigJson         string                 `json:"-"                   gorm:"column:extra_config;type:text;default:'{}'"`
	ExtraConfigMap          map[string]string      `json:"extraConfig"         gorm:"-"`
	CreateTime              string                 `json:"createTime"           gorm:"-"`
	UpdateTime              string                 `json:"updateTime,omitempty" gorm:"-"`
	CreatedAt               time.Time              `json:"-"`
	UpdatedAt               time.Time              `json:"-"`
	DeletedAt               gorm.DeletedAt         `json:"-"`
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
	s.CreateTime = s.CreatedAt.Format(TimeFormat)
	s.UpdateTime = s.UpdatedAt.Format(TimeFormat)
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
