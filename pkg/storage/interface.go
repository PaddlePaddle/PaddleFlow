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
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

var (
	Filesystem   FileSystemStoreInterface
	FsMountStore FsMountStoreInterface
	FsCache      FsCacheStoreInterface
)

func InitStores(db *gorm.DB) {
	// do not use once.Do() because unit test need to init db twice
	Filesystem = newFilesystemStore(db)
	FsMountStore = NewFsMountStore(db)
	FsCache = newDBFSCache(db)
}

type FileSystemStoreInterface interface {
	// filesystem
	CreatFileSystem(fs *model.FileSystem) error
	GetFileSystemWithFsID(fsID string) (model.FileSystem, error)
	DeleteFileSystem(tx *gorm.DB, id string) error
	ListFileSystem(limit int, userName, marker, fsName string) ([]model.FileSystem, error)
	GetSimilarityAddressList(fsType string, ips []string) ([]model.FileSystem, error)
	// link
	CreateLink(link *model.Link) error
	FsNameLinks(fsID string) ([]model.Link, error)
	LinkWithFsIDAndFsPath(fsID, fsPath string) (model.Link, error)
	DeleteLinkWithFsIDAndFsPath(fsID, fsPath string) error
	ListLink(limit int, marker, fsID string) ([]model.Link, error)
	GetLinkWithFsIDAndPath(fsID, fsPath string) ([]model.Link, error)
	// fs_cache_config
	CreateFSCacheConfig(logEntry *log.Entry, fsCacheConfig *model.FSCacheConfig) error
	UpdateFSCacheConfig(logEntry *log.Entry, fsCacheConfig model.FSCacheConfig) error
	DeleteFSCacheConfig(tx *gorm.DB, fsID string) error
	GetFSCacheConfig(logEntry *log.Entry, fsID string) (model.FSCacheConfig, error)
}

// FsCacheStoreInterface currently has two implementations: DB and memory
// use newMemFSCache() or newDBFSCache(db *gorm.DB) to initiate
type FsCacheStoreInterface interface {
	Add(value *model.FSCache) error
	Get(fsID string, cacheID string) (*model.FSCache, error)
	Delete(fsID, cacheID string) error
	List(fsID, cacheID string) ([]model.FSCache, error)
	Update(value *model.FSCache) (int64, error)
}
