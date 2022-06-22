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

package storage

import (
	"fmt"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

// CreateLink creates a new link
func (fss *FilesystemStore) CreateLink(link *model.Link) error {
	return fss.db.Create(link).Error
}

func (fss *FilesystemStore) FsNameLinks(fsID string) ([]model.Link, error) {
	var links []model.Link
	result := fss.db.Where(&model.Link{FsID: fsID}).Find(&links)
	return links, result.Error
}

func (fss *FilesystemStore) LinkWithFsIDAndFsPath(fsID, fsPath string) (model.Link, error) {
	var link model.Link
	result := fss.db.Where(&model.Link{FsID: fsID, FsPath: fsPath}).Find(&link)
	return link, result.Error
}

// DeleteLinkWithFsIDAndFsPath delete a file system link
func (fss *FilesystemStore) DeleteLinkWithFsIDAndFsPath(fsID, fsPath string) error {
	result := fss.db.Where(fmt.Sprintf(QueryEqualWithParam, FsID), fsID).Where(fmt.Sprintf(QueryEqualWithParam, FsPath), fsPath).Delete(&model.Link{})
	return result.Error
}

// ListLink get links with marker and limit sort by create_at desc
func (fss *FilesystemStore) ListLink(limit int, marker, fsID string) ([]model.Link, error) {
	var links []model.Link
	result := fss.db.Where(&model.Link{FsID: fsID}).Where(fmt.Sprintf(QueryLess, CreatedAt, "'"+marker+"'")).
		Order(fmt.Sprintf(" %s %s ", CreatedAt, DESC)).Limit(limit).Find(&links)
	return links, result.Error
}

func (fss *FilesystemStore) GetLinkWithFsIDAndPath(fsID, fsPath string) ([]model.Link, error) {
	var links []model.Link
	result := fss.db.Where(&model.Link{FsID: fsID, FsPath: fsPath}).First(&links)
	return links, result.Error
}
