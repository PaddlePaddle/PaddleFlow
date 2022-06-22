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
	FsStore          FileSystemStoreInterface
	CacheConfigStore FsCacheConfigStoreInterface
	LinkStore        LinkStoreInterface
	FsMountStore     FsMountStoreInterface
	FsCacheStore     FsCacheStoreInterface
)

func InitStores(db *gorm.DB) {
	// do not use once.Do() because unit test need to init db twice
	FsStore = NewFileSystemStore(db)
	CacheConfigStore = NewFsCacheConfigStore(db)
	LinkStore = NewLinkStore(db)
	FsMountStore = NewFsMountStore(db)
	FsCacheStore = NewFsCacheStore(db)
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
