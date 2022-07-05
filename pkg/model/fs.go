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

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const (
	FileSystemTableName = "filesystem"
)

// FileSystem defined file system model, which can be used to create file system
type FileSystem struct {
	Model
	Name                    string            `json:"name"`
	Type                    string            `json:"type"`
	ServerAddress           string            `json:"serverAddress"`
	SubPath                 string            `json:"subPath" gorm:"column:subpath"`
	PropertiesJson          string            `json:"-" gorm:"column:properties;type:text;default:'{}'"`
	PropertiesMap           map[string]string `json:"properties" gorm:"-"`
	UserName                string            `json:"userName"`
	IndependentMountProcess bool              `json:"independentMountProcess"`
}

func (FileSystem) TableName() string {
	return FileSystemTableName
}

// AfterFind is the callback methods doing after the find file system
func (s *FileSystem) AfterFind(*gorm.DB) error {
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
func (s *FileSystem) BeforeSave(*gorm.DB) error {
	propertiesJson, err := json.Marshal(&s.PropertiesMap)
	if err != nil {
		log.Errorf("json Marshal propertiesMap[%v] failed: %v", s.PropertiesMap, err)
		return err
	}
	s.PropertiesJson = string(propertiesJson)
	return nil
}
