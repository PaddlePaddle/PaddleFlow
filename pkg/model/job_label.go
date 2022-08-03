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

type JobLabel struct {
	Pk        int64  `gorm:"primaryKey;autoIncrement"`
	ID        string `gorm:"type:varchar(36);uniqueIndex"`
	Label     string `gorm:"type:varchar(255);NOT NULL"`
	JobID     string `gorm:"type:varchar(60);NOT NULL"`
	CreatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

func (JobLabel) TableName() string {
	return "job_label"
}
