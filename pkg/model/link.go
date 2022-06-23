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

package model

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const (
	LinkTableName = "link"
)

// Link defined file system model, which can be used to create link
type Link struct {
	Model
	FsID           string            `json:"fsID"`
	FsPath         string            `json:"FsPath"`
	ServerAddress  string            `json:"serverAddress"`
	SubPath        string            `json:"subPath" gorm:"column:subpath"`
	Type           string            `json:"type"`
	PropertiesJson string            `json:"-" gorm:"column:properties;type:text;default:'{}'"`
	PropertiesMap  map[string]string `json:"properties" gorm:"-"`
	UserName       string            `json:"userName"`
}

func (Link) TableName() string {
	return LinkTableName
}

// AfterFind is the callback methods doing after the find link
func (s *Link) AfterFind(*gorm.DB) error {
	if s.PropertiesJson != "" {
		s.PropertiesMap = make(map[string]string)
		if err := json.Unmarshal([]byte(s.PropertiesJson), &s.PropertiesMap); err != nil {
			log.Errorf("json Unmarshal propertiesJson[%s] failed: %v", s.PropertiesJson, err)
			return err
		}
	}
	return nil
}

// BeforeSave is the callback methods for saving file system
func (s *Link) BeforeSave(*gorm.DB) error {
	propertiesJson, err := json.Marshal(&s.PropertiesMap)
	if err != nil {
		log.Errorf("json Marshal propertiesMap[%v] failed: %v", s.PropertiesMap, err)
		return err
	}
	s.PropertiesJson = string(propertiesJson)
	return nil
}
