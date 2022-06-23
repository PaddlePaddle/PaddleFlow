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

	"gorm.io/gorm"
)

type Grant struct {
	Pk           int64          `json:"-" gorm:"primaryKey;autoIncrement"`
	ID           string         `json:"grantID" gorm:"uniqueIndex"`
	UserName     string         `json:"userName"`
	ResourceType string         `json:"resourceType"`
	ResourceID   string         `json:"resourceID"`
	CreatedAt    time.Time      `json:"createTime"`
	UpdatedAt    time.Time      `json:"updateTime,omitempty"`
	DeletedAt    gorm.DeletedAt `json:"-" gorm:"index"`
}

func (Grant) TableName() string {
	return "grant"
}
