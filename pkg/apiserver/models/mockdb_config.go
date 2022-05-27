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
	log "github.com/sirupsen/logrus"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
)

func InitMockDB() {
	// github.com/mattn/go-sqlite3
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
		// print sql
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		log.Fatalf("The fake DB doesn't create successfully. Fail fast. error: %v", err)
	}
	// Create tables
	_ = db.AutoMigrate(
		&Pipeline{},
		&RunCache{},
		&ArtifactEvent{},
		&User{},
		&Run{},
		&RunJob{},
		&Queue{},
		&Flavour{},
		&Grant{},
		&Job{},
		&ClusterInfo{},
		&Image{},
		&FileSystem{},
		&Link{},
		&FSCacheConfig{},
		&FSCache{},
		&FsMount{},
	)
	database.DB = db
}
