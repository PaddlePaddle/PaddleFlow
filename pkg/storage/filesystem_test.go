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

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/stretchr/testify/assert"
)

const (
	mockFsID = "fs-root-mock"
)

func TestGetFsInfo(t *testing.T) {
	initMockDB()
	fs := model.FileSystem{
		Model: model.Model{
			ID: mockFsID,
		},
		Type:    "s3",
		SubPath: "elsie",
	}
	err := Filesystem.CreatFileSystem(&fs)
	assert.Nil(t, err)
	fsCache := model.FSCacheConfig{
		FsID:       mockFsID,
		CacheDir:   "/data/paddleflow-fs/mnt",
		MetaDriver: "nutsdb",
	}
	err = Filesystem.CreateFSCacheConfig(&fsCache)
	assert.Nil(t, err)

	fsInfo, err := Filesystem.GetFsInfo(mockFsID)
	assert.Nil(t, err)
	assert.Equal(t, fs.ID, fsInfo.ID)
	assert.Equal(t, fs.Type, fsInfo.Type)
	assert.Equal(t, fs.SubPath, fsInfo.SubPath)
	assert.Equal(t, fsCache.CacheDir, fsInfo.CacheDir)
	assert.Equal(t, fsCache.MetaDriver, fsInfo.MetaDriver)
}
