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
	log "github.com/sirupsen/logrus"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

var (
	Filesystem   FileSystemStoreInterface
	FsMountStore FsMountStoreInterface
	FsCacheStore FsCacheStoreInterface
)

func InitStores(db *gorm.DB) {
	// do not use once.Do() because unit test need to init db twice
	Filesystem = NewFilesystemStore(db)
	FsMountStore = NewFsMountStore(db)
	FsCacheStore = NewFsCacheStore(db)
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
	// cache config
	CreateFSCacheConfig(logEntry *log.Entry, fsCacheConfig *model.FSCacheConfig) error
	UpdateFSCacheConfig(logEntry *log.Entry, fsCacheConfig model.FSCacheConfig) error
	DeleteFSCacheConfig(tx *gorm.DB, fsID string) error
	GetFSCacheConfig(logEntry *log.Entry, fsID string) (model.FSCacheConfig, error)
}

type FilesystemStore struct {
	db *gorm.DB
}

func NewFilesystemStore(db *gorm.DB) *FilesystemStore {
	return &FilesystemStore{db: db}
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

func NewFsCacheStore(db *gorm.DB) FsCacheStoreInterface {
	// default use db storage, mem used in the future maybe as the cache for db
	return newDBFSCache(db)
}

func initMockDB() {
	// github.com/mattn/go-sqlite3
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
		// print sql
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		log.Fatalf("InitMockDB open db error: %v", err)
	}

	if err := db.AutoMigrate(
		&model.FileSystem{},
		&model.Link{},
		&model.FSCacheConfig{},
		&model.FSCache{},
		&model.FsMount{},
	); err != nil {
		log.Fatalf("InitMockDB createDatabaseTables error[%s]", err.Error())
	}
	database.DB = db
	InitStores(db)
}
