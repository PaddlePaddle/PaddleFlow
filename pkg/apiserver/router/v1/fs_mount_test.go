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

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
)

func TestFSMount(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	// mockFs := buildMockFS()
	// cacheConf := buildMockFSCacheConfig()
	req1 := fs.CreateMountRequest{
		Username:   MockRootUser,
		FsName:     mockFsName,
		ClusterID:  "testcluster",
		NodeName:   "abc",
		MountPoint: "/var/1",
	}

	badReq := fs.CreateMountRequest{
		Username:  MockRootUser,
		FsName:    mockFsName,
		ClusterID: "testcluster",
		NodeName:  "abc",
	}
	// test create failure - no fs
	url := baseUrl + "/fsMount"
	result, err := PerformPostRequest(router, url, badReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, result.Code)

	result, err = PerformPostRequest(router, url, req1)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, result.Code)

	req2 := fs.CreateMountRequest{
		Username:   MockRootUser,
		FsName:     mockFsName,
		ClusterID:  "testcluster",
		NodeName:   "abc",
		MountPoint: "/var/2",
	}

	result, err = PerformPostRequest(router, url, req2)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, result.Code)

	time.Sleep(1 * time.Second)
	// with filters
	filters := "?" + util.QueryNodeName + "=abc"
	result, err = PerformGetRequest(router, url+filters)
	assert.Nil(t, err)
	listRsp := fs.ListMountResponse{}
	err = ParseBody(result.Body, &listRsp)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(listRsp.MountList))

	filters2 := "?" + util.QueryMountPoint + "=/var/2&" + util.QueryNodeName + "=abc&" + util.QueryClusterID + "=testcluster"
	_, err = PerformDeleteRequest(router, url+"/"+mockFsName+filters2)
	assert.Nil(t, err)

	result, err = PerformGetRequest(router, url+filters)
	assert.Nil(t, err)
	listRsp = fs.ListMountResponse{}
	err = ParseBody(result.Body, &listRsp)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(listRsp.MountList))
}
