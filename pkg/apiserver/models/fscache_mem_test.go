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

func TestMemFSCache(t *testing.T) {
	mm := newMemFSCache()
	fsCache1 := new(FSCache)
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
