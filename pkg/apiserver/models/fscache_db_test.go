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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBFSCache(t *testing.T) {
	InitMockDB()
	dbfs := newDBFSCache()
	fsCache1 := new(FSCache)
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
