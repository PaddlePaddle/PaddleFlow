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
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

func mockFS() model.FileSystem {
	return model.FileSystem{
		Model:    model.Model{ID: mockFsID},
		UserName: MockRootUser,
		Name:     mockFsName,
	}
}

func mockFSCache() model.FSCacheConfig {
	return model.FSCacheConfig{
		FsID:       mockFsID,
		CacheDir:   "/abs/path",
		MetaDriver: "nutsdb",
		BlockSize:  666,
	}
}

func buildUpdateReq(model model.FSCacheConfig) fs.UpdateFileSystemCacheRequest {
	return fs.UpdateFileSystemCacheRequest{
		FsID:       model.FsID,
		CacheDir:   model.CacheDir,
		MetaDriver: "nutsdb",
		BlockSize:  model.BlockSize,
	}
}

func buildCreateReq(model model.FSCacheConfig) fs.CreateFileSystemCacheRequest {
	req := fs.CreateFileSystemCacheRequest{
		Username:                     MockRootUser,
		FsName:                       mockFsName,
		UpdateFileSystemCacheRequest: buildUpdateReq(model),
	}
	return req
}

func TestFSCacheConfigRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	mockFs := mockFS()
	cacheConf := mockFSCache()
	updateReq := buildUpdateReq(cacheConf)
	createRep := buildCreateReq(cacheConf)

	// test create failure - no fs
	url := baseUrl + "/fsCache"
	result, err := PerformPostRequest(router, url, createRep)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusNotFound, result.Code)

	// test create success
	err = storage.Filesystem.CreatFileSystem(&mockFs)
	assert.Nil(t, err)

	result, err = PerformPostRequest(router, url, createRep)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, result.Code)

	// test get success
	urlWithFsID := url + "/" + mockFsName
	result, err = PerformGetRequest(router, urlWithFsID)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)
	cacheRsp := fs.FileSystemCacheResponse{}
	err = ParseBody(result.Body, &cacheRsp)
	assert.Nil(t, err)
	assert.Equal(t, cacheConf.MetaDriver, cacheRsp.MetaDriver)
	assert.Equal(t, cacheConf.CacheDir, cacheRsp.CacheDir)
	assert.Equal(t, cacheConf.BlockSize, cacheRsp.BlockSize)
	assert.Equal(t, cacheConf.BlockSize, cacheRsp.BlockSize)
	// test fsToName()
	assert.Equal(t, createRep.Username, cacheRsp.Username)

	// test get failure
	urlWrong := url + "/666"
	result, err = PerformGetRequest(router, urlWrong)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusNotFound, result.Code)

	// test update success
	updateReq.BlockSize = 333
	updateReq.CacheDir = "/newPath"
	result, err = PerformPutRequest(router, urlWithFsID, updateReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)

	result, err = PerformGetRequest(router, urlWithFsID)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)
	err = ParseBody(result.Body, &cacheRsp)
	assert.Nil(t, err)
	assert.Equal(t, updateReq.BlockSize, cacheRsp.BlockSize)
	assert.Equal(t, updateReq.CacheDir, cacheRsp.CacheDir)

	// test update failure
	result, err = PerformPutRequest(router, urlWrong, updateReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusNotFound, result.Code)

	updateReq.CacheDir = "newPath" // not abs path
	result, err = PerformPutRequest(router, urlWithFsID, updateReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, result.Code)

	// delete
	result, err = PerformDeleteRequest(router, urlWithFsID)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)

	time.Sleep(3 * time.Second)
	result, err = PerformDeleteRequest(router, urlWithFsID)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusNotFound, result.Code)
}

func buildReportRequest() fs.CacheReportRequest {
	req := fs.CacheReportRequest{
		Username:  MockRootUser,
		FsName:    mockFsName,
		ClusterID: "testcluster",
		CacheDir:  "/var/cache",
		NodeName:  "abc.com",
		UsedSize:  100,
	}
	return req
}

func TestFSCacheReportRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	// mockFs := buildMockFS()
	// cacheConf := buildMockFSCacheConfig()
	req := buildReportRequest()
	badReq := fs.CacheReportRequest{
		Username:  MockRootUser,
		FsName:    mockFsName,
		ClusterID: "testcluster",
		CacheDir:  "/var/cache",
		NodeName:  "abc.com",
	}
	// test create failure - no fs
	url := baseUrl + "/fsCache/report"
	result, err := PerformPostRequest(router, url, badReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, result.Code)

	result, err = PerformPostRequest(router, url, req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)

	cacheList, err := storage.FsCache.List(common.ID(MockRootUser, mockFsName), "")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(cacheList))

	req.UsedSize = 200
	result, err = PerformPostRequest(router, url, req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)

	cacheList, err = storage.FsCache.List(common.ID(MockRootUser, mockFsName), "")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(cacheList))
	assert.Equal(t, 200, cacheList[0].UsedSize)
}
