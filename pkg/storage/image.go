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

package storage

import (
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type ImageStore struct {
	db *gorm.DB
}

func newImageStore(db *gorm.DB) *ImageStore {
	return &ImageStore{db: db}
}

func (is *ImageStore) CreateImage(logEntry *log.Entry, image *model.Image) error {
	logEntry.Debugf("begin create image.")
	tx := is.db.Model(&model.Image{}).Create(image)
	if tx.Error != nil {
		logEntry.Errorf("create image failed. ID:%s, error:%s",
			image.ID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func (is *ImageStore) ListImageIDsByFsID(logEntry *log.Entry, fsID string) ([]string, error) {
	logEntry.Debugf("begin list image by fs[%s].", fsID)
	var imageIDs []string
	tx := is.db.Model(&model.Image{}).Select("image_id").Where("fs_id = ?", fsID).Find(&imageIDs)
	if tx.Error != nil {
		logEntry.Errorf("list imageIDs by fs[%s] failed. error:%s",
			fsID, tx.Error.Error())
		return nil, tx.Error
	}
	return imageIDs, nil
}

func (is *ImageStore) GetImage(logEntry *log.Entry, PFImageID string) (model.Image, error) {
	logEntry.Debugf("begin GetImage")
	var image model.Image
	tx := is.db.Model(&model.Image{}).Where("id = ?", PFImageID).Find(&image)
	if tx.Error != nil {
		logEntry.Errorf("GetImage[%s] failed. error:%s",
			PFImageID, tx.Error.Error())
		return model.Image{}, tx.Error
	}
	return image, nil
}

func (is *ImageStore) GetUrlByPFImageID(logEntry *log.Entry, PFImageID string) (string, error) {
	logEntry.Debugf("begin GetUrlByPFImageID[%s].", PFImageID)
	var url string
	tx := is.db.Model(&model.Image{}).Select("url").Where("id =", PFImageID).Find(&url)
	if tx.Error != nil {
		logEntry.Errorf("GetUrlByPFImageID[%s] failed. error:%s",
			PFImageID, tx.Error.Error())
		return "", tx.Error
	}
	return url, nil
}

func (is *ImageStore) UpdateImage(logEntry *log.Entry, PFImageID string, image model.Image) error {
	logEntry.Debugf("begin UpdateImage[%s]", PFImageID)
	tx := is.db.Model(&model.Image{}).Where("id = ?", PFImageID).Updates(image)
	if tx.Error != nil {
		logEntry.Errorf("UpdateImage[%s] failed. error:%s",
			PFImageID, tx.Error.Error())
		return tx.Error
	}
	return nil
}
