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
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
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

// CreateLink creates a new link
func CreateLink(link *Link) error {
	db := database.DB
	return db.Create(link).Error
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

func FsNameLinks(fsID string) ([]Link, error) {
	var links []Link
	db := database.DB
	result := &gorm.DB{}
	result = db.Where(&Link{FsID: fsID}).Find(&links)
	return links, result.Error
}

func LinkWithFsIDAndFsPath(fsID, fsPath string) (Link, error) {
	var link Link
	db := database.DB
	result := &gorm.DB{}
	result = db.Where(&Link{FsID: fsID, FsPath: fsPath}).Find(&link)
	return link, result.Error
}

// DeleteLinkWithFsIDAndFsPath delete a file system link
func DeleteLinkWithFsIDAndFsPath(fsID, fsPath string) error {
	db := database.DB
	result := db.Where(fmt.Sprintf(QueryEqualWithParam, FsID), fsID).Where(fmt.Sprintf(QueryEqualWithParam, FsPath), fsPath).Delete(&Link{})
	return result.Error
}

// ListLink get links with marker and limit sort by create_at desc
func ListLink(limit int, marker, fsID string) ([]Link, error) {
	var links []Link
	db := database.DB
	result := &gorm.DB{}
	result = db.Where(&Link{FsID: fsID}).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
		Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&links)
	return links, result.Error
}

func GetLinkWithFsIDAndPath(fsID, fsPath string) ([]Link, error) {
	var links []Link
	db := database.DB
	result := db.Where(&Link{FsID: fsID, FsPath: fsPath}).First(&links)
	return links, result.Error
}
