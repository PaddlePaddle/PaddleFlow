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

package model

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	CreatedAt  = "created_at"
	UpdatedAt  = "updated_at"
	Type       = "type"
	ID         = "id"
	FsID       = "fs_id"
	UserName   = "user_name"
	FsName     = "name"
	TimeFormat = "2006-01-02 15:04:05"
)

type Model struct {
	ID         string    `json:"id"`
	CreatedAt  time.Time `json:"-"`
	UpdatedAt  time.Time `json:"-"`
	CreateTime string    `json:"createTime"           gorm:"-"`
	UpdateTime string    `json:"updateTime,omitempty" gorm:"-"`
}

// BeforeCreate the function do the operation before creating file system or link
func (m *Model) BeforeCreate(tx *gorm.DB) error {
	if m.ID != "" {
		return nil
	}

	m.ID = uuid.NewString()
	return nil
}

func (m *Model) AfterFind(tx *gorm.DB) error {
	m.CreateTime = m.CreatedAt.Format(TimeFormat)
	m.UpdateTime = m.UpdatedAt.Format(TimeFormat)
	return nil
}
