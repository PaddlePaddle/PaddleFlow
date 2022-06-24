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

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	CreatedAt   = "created_at"
	UpdatedAt   = "updated_at"
	Type        = "type"
	ID          = "id"
	FsID        = "fs_id"
	UserName    = "user_name"
	FsName      = "name"
	GrantFsType = "fs"

	TimeFormat = "2006-01-02 15:04:05"
)

type Model struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"-"`
	UpdatedAt time.Time `json:"-"`
}

// BeforeCreate the function do the operation before creating file system or link
func (m *Model) BeforeCreate(tx *gorm.DB) error {
	if m.ID != "" {
		return nil
	}

	m.ID = uuid.NewString()
	return nil
}

func initMockDB() {
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
		&PipelineDetail{},
		&Schedule{},
		&RunCache{},
		&ArtifactEvent{},
		&model.User{},
		&Run{},
		&RunJob{},
		&Queue{},
		&Flavour{},
		&model.Grant{},
		&Job{},
		&JobTask{},
		&JobLabel{},
		&ClusterInfo{},
		&Image{},
	)
	storage.DB = db
	storage.InitStores(db)
}
