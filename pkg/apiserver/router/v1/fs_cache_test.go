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

package v1

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/apiserver/controller/fs"
	"paddleflow/pkg/apiserver/models"
)

func buildMockFS() models.FileSystem {
	return models.FileSystem{
		Model:    models.Model{ID: mockFsID},
		UserName: MockRootUser,
		Name:     mockFsName,
	}
}

func buildMockFSCacheConfig() models.FSCacheConfig {
	return models.FSCacheConfig{
		FsID:      mockFsID,
		CacheDir:  "path",
		Quota:     444,
		CacheType: "disk",
		BlockSize: 666,
	}
}

func buildRequest(model models.FSCacheConfig) fs.CreateOrUpdateFSCacheRequest {
	return fs.CreateOrUpdateFSCacheRequest{
		FSCacheConfig: model,
		Username:      MockRootUser,
		FsName:        mockFsName,
	}
}

func TestFSCacheConfigRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	mockFs := buildMockFS()
	cacheConf := buildMockFSCacheConfig()
	req := buildRequest(cacheConf)

	// test create failure - no fs
	url := baseUrl + "/fs/cache"
	result, err := PerformPostRequest(router, url, req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusForbidden, result.Code)

	// test create success
	err = models.CreatFileSystem(&mockFs)
	assert.Nil(t, err)

	result, err = PerformPostRequest(router, url, req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, result.Code)

	// test get success
	urlWithFsID := url + "/" + mockFsName
	result, err = PerformGetRequest(router, urlWithFsID)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)
	cacheRsp := models.FSCacheConfig{}
	err = ParseBody(result.Body, &cacheRsp)
	assert.Nil(t, err)
	assert.Equal(t, cacheConf.CacheType, cacheRsp.CacheType)
	assert.Equal(t, cacheConf.CacheDir, cacheRsp.CacheDir)
	assert.Equal(t, cacheConf.BlockSize, cacheRsp.BlockSize)
	assert.Equal(t, cacheConf.Quota, cacheRsp.Quota)

	// test get failure
	urlWrong := url + "/666"
	result, err = PerformGetRequest(router, urlWrong)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusNotFound, result.Code)

	// test update success
	req.Quota = 333
	req.CacheDir = "newPath"
	result, err = PerformPutRequest(router, urlWithFsID, req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)

	result, err = PerformGetRequest(router, urlWithFsID)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)
	err = ParseBody(result.Body, &cacheRsp)
	assert.Nil(t, err)
	assert.Equal(t, req.Quota, cacheRsp.Quota)
	assert.Equal(t, req.CacheDir, cacheRsp.CacheDir)

	// test update failure
	result, err = PerformPutRequest(router, urlWrong, req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusNotFound, result.Code)
}
