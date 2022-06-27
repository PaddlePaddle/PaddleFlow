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

package location_awareness

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestListMountNodesByFsID(t *testing.T) {
	driver.InitMockDB()

	fsID1, fsID2, cacheDir1, cacheDir2, nodeName1, nodeName2, clusterID :=
		"fs-root-1", "fs-root-2", "/mnt/fs-root-1/storage", "/mnt/fs-root-2/storage", "node1", "node2", ""
	cache := &model.FSCache{
		FsID:      fsID1,
		CacheDir:  cacheDir1,
		NodeName:  nodeName1,
		ClusterID: clusterID,
	}
	err := storage.FsCache.Add(cache)
	assert.Nil(t, err)

	cache = &model.FSCache{
		FsID:      fsID1,
		CacheDir:  cacheDir1,
		NodeName:  nodeName2,
		ClusterID: clusterID,
	}
	err = storage.FsCache.Add(cache)
	assert.Nil(t, err)

	cache = &model.FSCache{
		FsID:      fsID2,
		CacheDir:  cacheDir2,
		NodeName:  nodeName1,
		ClusterID: clusterID,
	}
	err = storage.FsCache.Add(cache)
	assert.Nil(t, err)

	fsIDs := []string{fsID1, fsID2, "fs-non-exist"}
	nodeList, err := ListFsCacheLocation(fsIDs)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(nodeList))
}
