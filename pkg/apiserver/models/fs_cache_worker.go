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

type FSCacheWorker struct {
	Model
	Dir        string `json:"dir"`
	NodeName   string `json:"nodename"`
	MountPoint string `json:"mountPoint" gorm:"column:mount_point"`
	UsedSize   int    `json:"usedSize" gorm:"column:used_size"`
}

func (s *FSCacheWorker) TableName() string {
	return "fs_cache_worker"
}
