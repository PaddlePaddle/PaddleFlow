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
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func TestGetFSCacheStore(t *testing.T) {
	initMockDB()
	fsCache, err := FsCache.Get("", "")
	assert.NotNil(t, err)
	assert.Nil(t, fsCache)
}

func TestDBFSCache(t *testing.T) {
	initMockDB()
	dbfs := newDBFSCache(DB)
	fsCache1 := new(model.FSCache)
	fsCache1.CacheDir = "cachedir"
	fsCache1.CacheID = "cacheID1"
	fsCache1.FsID = "fsid"
	fsCache1.NodeName = "nodename"
	fsCache1.UsedSize = 111
	// test add
	err := dbfs.Add(fsCache1)
	assert.Nil(t, err)
	fsc, err := dbfs.Get("fsid", "cacheID1")
	assert.Nil(t, err)
	assert.Equal(t, fsc.FsID, "fsid")
	fscacheList, err := dbfs.List("", "")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(fscacheList))
}

func TestMemFSCache(t *testing.T) {
	mm := newMemFSCache()
	fsCache1 := new(model.FSCache)
	fsCache1.CacheDir = "cachedir"
	fsCache1.CacheID = "cacheID1"
	fsCache1.FsID = "fsid"
	fsCache1.NodeName = "nodename"
	fsCache1.UsedSize = 111
	_ = mm.Add(fsCache1)
	retV, _ := mm.Get("fsid", "cacheID1")
	assert.Equal(t, fsCache1.CacheDir, retV.CacheDir)
	assert.Equal(t, fsCache1.FsID, retV.FsID)
	retValues, _ := mm.List("fsid", "")
	assert.Equal(t, len(retValues), 1)
	_ = mm.Delete("fsid", "")
	retValues, _ = mm.List("fsid", "")
	assert.Equal(t, len(retValues), 0)
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
	); err != nil {
		log.Fatalf("InitMockDB createDatabaseTables error[%s]", err.Error())
	}
	DB = db
	InitStores(db)
}
