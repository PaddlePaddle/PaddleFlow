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
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

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
