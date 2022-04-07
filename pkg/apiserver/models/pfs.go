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
	"fmt"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const (
	FileSystemTableName = "filesystem"
)

// FileSystem defined file system model, which can be used to create file system
type FileSystem struct {
	Model
	Name           string            `json:"name"`
	Type           string            `json:"type"`
	ServerAddress  string            `json:"serverAddress"`
	SubPath        string            `json:"subPath" gorm:"column:subpath"`
	PropertiesJson string            `json:"-" gorm:"column:properties;type:text;default:'{}'"`
	PropertiesMap  map[string]string `json:"properties" gorm:"-"`
	UserName       string            `json:"userName"`
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

func CreatFileSystem(db *gorm.DB, fs *FileSystem) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(fs).Error; err != nil {
			return err
		}

		grantID := uuid.NewString()
		grantModel := &Grant{ID: grantID, UserName: fs.UserName, ResourceID: fs.ID, ResourceType: GrantFsType}
		if err := tx.Create(grantModel).Error; err != nil {
			return err
		}
		return nil
	})
}
func GetFileSystemWithFsID(db *gorm.DB, fsID string) (FileSystem, error) {
	var fileSystem FileSystem
	result := db.Where(&FileSystem{Model: Model{ID: fsID}}).Find(&fileSystem)
	return fileSystem, result.Error
}

func GetFileSystemWithFsIDAndUserName(db *gorm.DB, fsID, userName string) (FileSystem, error) {
	var fileSystem FileSystem
	result := db.Where(&FileSystem{Model: Model{ID: fsID}, UserName: userName}).Find(&fileSystem)
	return fileSystem, result.Error
}

func DeleteFileSystem(db *gorm.DB, id string) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(&FileSystem{Model: Model{ID: id}}).Error; err != nil {
			return err
		}

		if err := tx.Where(fmt.Sprintf(QueryEqualWithParam, FsID), id).Delete(&Link{}).Error; err != nil {
			return err
		}
		return nil
	})
}

// ListFileSystem get file systems with marker and limit sort by create_at desc
func ListFileSystem(db *gorm.DB, limit int, userName, marker, fsName string) ([]FileSystem, error) {
	var fileSystems []FileSystem
	result := &gorm.DB{}
	if fsName == "" {
		result = db.Where(&FileSystem{UserName: userName}).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
			Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&fileSystems)
	} else {
		result = db.Where(&FileSystem{UserName: userName, Name: fsName}).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
			Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&fileSystems)
	}
	return fileSystems, result.Error
}

// GetFsWithID get file system detail from id
func GetFsWithID(db *gorm.DB, fsID string) (FileSystem, error) {
	var fileSystem FileSystem
	result := db.Where(&FileSystem{Model: Model{ID: fsID}}).Find(&fileSystem)
	return fileSystem, result.Error
}

// GetFsWithIDs get file system detail from ids
func GetFsWithIDs(db *gorm.DB, fsID []string) ([]FileSystem, error) {
	var fileSystems []FileSystem
	result := db.Where(fmt.Sprintf(QueryInWithParam, ID), fsID).Find(&fileSystems)
	return fileSystems, result.Error
}

// GetFsWithNameAndUserName get file system detail from name and userID
func GetFsWithNameAndUserName(db *gorm.DB, fsName, userName string) (FileSystem, error) {
	var fileSystem FileSystem
	result := db.Where(&FileSystem{UserName: userName, Name: fsName}).Find(&fileSystem)
	return fileSystem, result.Error
}

// GetSimilarityAddressList find fs where have same type and serverAddress
func GetSimilarityAddressList(db *gorm.DB, fsType string, ips []string) ([]FileSystem, error) {
	var fileSystems []FileSystem
	result := &gorm.DB{}

	// local has no ip
	if len(ips) == 0 {
		result = db.Where(fmt.Sprintf("%s = ?", Type), fsType).Find(&fileSystems)
	} else {
		for k, ip := range ips {
			if k == 0 {
				db = db.Where(fmt.Sprintf(QueryLikeWithParam, ServerAddress), fmt.Sprintf("%%%s%%", ip))
			} else {
				db = db.Or(fmt.Sprintf(QueryLikeWithParam, ServerAddress), fmt.Sprintf("%%%s%%", ip))
			}
		}
		db = db.Where(fmt.Sprintf("%s = ?", Type), fsType)
		result = db.Find(&fileSystems)
	}

	return fileSystems, result.Error
}
