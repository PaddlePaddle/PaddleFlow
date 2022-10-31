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

package fs

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	k8sCore "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func mockFSCache() model.FSCacheConfig {
	return model.FSCacheConfig{
		FsID:           mockFSID,
		CacheDir:       "/abs/path",
		MetaDriver:     "disk",
		BlockSize:      666,
		ExtraConfigMap: map[string]string{"abc": "def"},
	}
}

func buildCreateReq(model model.FSCacheConfig) CreateFileSystemCacheRequest {
	req := CreateFileSystemCacheRequest{
		Username:    mockRootName,
		FsName:      mockFSName,
		FsID:        model.FsID,
		CacheDir:    model.CacheDir,
		MetaDriver:  "disk",
		BlockSize:   model.BlockSize,
		ExtraConfig: map[string]string{"aa": "bb"},
	}
	return req
}

func Test_FSCacheConfig(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: mockRootName}
	cacheConf := mockFSCache()
	createRep := buildCreateReq(cacheConf)

	err := CreateFileSystemCacheConfig(ctx, createRep)
	assert.Nil(t, err)

	// test get success
	cache, err := GetFileSystemCacheConfig(ctx, mockFSID)
	assert.Nil(t, err)
	assert.Equal(t, cacheConf.MetaDriver, cache.MetaDriver)
	assert.Equal(t, cacheConf.CacheDir, cache.CacheDir)
	assert.Equal(t, cacheConf.BlockSize, cache.BlockSize)
	assert.Equal(t, cacheConf.BlockSize, cache.BlockSize)
	// test fsToName()
	assert.Equal(t, createRep.Username, cache.Username)

	// test get failure
	_, err = GetFileSystemCacheConfig(ctx, "notExist")
	assert.NotNil(t, err)

	// delete
	svc := GetFileSystemService()
	p := gomonkey.ApplyPrivateMethod(reflect.TypeOf(svc), "checkFsMountedAllClustersAndScheduledJobs",
		func(fsID string) (bool, map[*runtime.KubeRuntime][]k8sCore.Pod, error) {
			return true, nil, nil
		})

	p2 := gomonkey.ApplyPrivateMethod(reflect.TypeOf(svc), "cleanFsResources",
		func(runtimePodsMap map[*runtime.KubeRuntime][]k8sCore.Pod, fsID string) (err error) {
			return nil
		})
	defer p2.Reset()

	// delete failed - mounted
	err = DeleteFileSystemCacheConfig(ctx, mockFSID)
	assert.NotNil(t, err)
	_, err = GetFileSystemCacheConfig(ctx, mockFSID)
	assert.Nil(t, err)

	// delete successful
	defer p.Reset()
	p = gomonkey.ApplyPrivateMethod(reflect.TypeOf(svc), "checkFsMountedAllClustersAndScheduledJobs",
		func(fsID string) (bool, map[*runtime.KubeRuntime][]k8sCore.Pod, error) {
			return false, nil, nil
		})
	err = DeleteFileSystemCacheConfig(ctx, mockFSID)
	assert.Nil(t, err)
	_, err = GetFileSystemCacheConfig(ctx, mockFSID)
	assert.NotNil(t, err)

	// delete failed - not exist
	err = DeleteFileSystemCacheConfig(ctx, mockFSID)
	assert.NotNil(t, err)
}
