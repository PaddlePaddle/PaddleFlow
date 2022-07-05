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

type Image struct {
	Pk        int64          `json:"-"         gorm:"primaryKey;autoIncrement;not null"`
	ID        string         `json:"PFImageID" gorm:"type:varchar(128);not null;uniqueIndex"`
	ImageID   string         `json:"imageID"   gorm:"type:varchar(64)"`
	FsID      string         `json:"fsID"      gorm:"type:varchar(60);not null"`
	Source    string         `json:"source"    gorm:"type:varchar(256);not null"`
	Url       string         `json:"url"       gorm:"type:varchar(256)"`
	CreatedAt time.Time      `json:"-"`
	UpdatedAt time.Time      `json:"-"`
	DeletedAt gorm.DeletedAt `json:"-"         gorm:"index"`
}

func (Image) TableName() string {
	return "image"
}

func CreateImage(logEntry *log.Entry, image *Image) error {
	logEntry.Debugf("begin create image.")
	tx := storage.DB.Model(&Image{}).Create(image)
	if tx.Error != nil {
		logEntry.Errorf("create image failed. ID:%s, error:%s",
			image.ID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func ListImageIDsByFsID(logEntry *log.Entry, fsID string) ([]string, error) {
	logEntry.Debugf("begin list image by fs[%s].", fsID)
	var imageIDs []string
	tx := storage.DB.Model(&Image{}).Select("image_id").Where("fs_id = ?", fsID).Find(&imageIDs)
	if tx.Error != nil {
		logEntry.Errorf("list imageIDs by fs[%s] failed. error:%s",
			fsID, tx.Error.Error())
		return nil, tx.Error
	}
	return imageIDs, nil
}

func GetImage(logEntry *log.Entry, PFImageID string) (Image, error) {
	logEntry.Debugf("begin GetImage")
	var image Image
	tx := storage.DB.Model(&Image{}).Where("id = ?", PFImageID).Find(&image)
	if tx.Error != nil {
		logEntry.Errorf("GetImage[%s] failed. error:%s",
			PFImageID, tx.Error.Error())
		return Image{}, tx.Error
	}
	return image, nil
}

func GetUrlByPFImageID(logEntry *log.Entry, PFImageID string) (string, error) {
	logEntry.Debugf("begin GetUrlByPFImageID[%s].", PFImageID)
	var url string
	tx := storage.DB.Model(&Image{}).Select("url").Where("id =", PFImageID).Find(&url)
	if tx.Error != nil {
		logEntry.Errorf("GetUrlByPFImageID[%s] failed. error:%s",
			PFImageID, tx.Error.Error())
		return "", tx.Error
	}
	return url, nil
}

func UpdateImage(logEntry *log.Entry, PFImageID string, image Image) error {
	logEntry.Debugf("begin UpdateImage[%s]", PFImageID)
	tx := storage.DB.Model(&Image{}).Where("id = ?", PFImageID).Updates(image)
	if tx.Error != nil {
		logEntry.Errorf("UpdateImage[%s] failed. error:%s",
			PFImageID, tx.Error.Error())
		return tx.Error
	}
	return nil
}
