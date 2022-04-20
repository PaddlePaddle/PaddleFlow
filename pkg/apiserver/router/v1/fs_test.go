/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/controller/fs"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/apiserver/router/util"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
	fsCommon "paddleflow/pkg/fs/common"
)

func Test_validateCreateFileSystem(t *testing.T) {
	type args struct {
		ctx *logger.RequestContext
		req *fs.CreateFileSystemRequest
	}

	var p1 = gomonkey.ApplyFunc(models.GetSimilarityAddressList, func(fsType string, ips []string) ([]models.FileSystem, error) {
		return []models.FileSystem{}, nil
	})
	defer p1.Reset()
	var p2 = gomonkey.ApplyFunc(checkStorageConnectivity, func(fsMeta fsCommon.FSMeta) error {
		return nil
	})
	defer p2.Reset()
	var p3 = gomonkey.ApplyFunc(checkPVCExist, func(pvc, namespace string) bool {
		return true
	})
	defer p3.Reset()

	ctx := &logger.RequestContext{UserName: "testusername"}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "cfs://12 ok",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "cfs://12:/"},
			},
			wantErr: false,
		},
		{
			name: "cfs://home wrong",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "cfs://12"},
			},
			wantErr: true,
		},
		{
			name: "url error",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Url: "httpxxxx", Username: "Test"},
			},
			wantErr: true,
		},
		{
			name: "hdfs url miss path",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Url: "hdfs://192.168.1.2:9000,192.168.1.3:9000", Username: "Test"},
			},
			wantErr: true,
		},
		{
			name: "hdfs /",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "hdfs://127.0.0.1:9000/", Properties: map[string]string{"user": "test", "group": "test"}},
			},
			wantErr: false,
		},
		{
			name: "hdfs /myfs/data",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "hdfs://127.0.0.1:9000/myfs/data", Properties: map[string]string{"user": "test", "group": "test"}},
			},
			wantErr: false,
		},
		{
			name: "hdfs url miss address",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "hdfs://path"},
			},
			wantErr: true,
		},
		{
			name: "s3 url no path wrong",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "s3://127.0.0.1"},
			},
			wantErr: true,
		},
		{
			name: "s3 url no bucket wrong",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "s3://127.0.0.1/", Properties: map[string]string{}},
			},
			wantErr: true,
		},
		{
			name: "s3 url has only bucket",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "s3://bucket/", Properties: map[string]string{fsCommon.Endpoint: "bj.bos.com", fsCommon.Region: "bj", fsCommon.AccessKey: "testak", fsCommon.SecretKey: "testsk"}},
			},
			wantErr: false,
		},
		{
			name: "s3 url bucket data",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "s3://bucket/subpath", Properties: map[string]string{fsCommon.Endpoint: "bj.bos.com", fsCommon.Region: "bj", fsCommon.AccessKey: "testak", fsCommon.SecretKey: "testsk"}},
			},
			wantErr: false,
		},
		{
			name: "local url wrong",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "local:"},
			},
			wantErr: true,
		},
		{
			name: "local:://home ok",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "local:://home", Properties: map[string]string{"debug": "true"}},
			},
			wantErr: false,
		},
		{
			name: "local::// wrong",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "local:://"},
			},
			wantErr: true,
		},
		{
			name: "local:://root",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "local:://root"},
			},
			wantErr: true,
		},
		{
			name: "local:://root/abc",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "local:://root/abc"},
			},
			wantErr: true,
		},
		{
			name: "mock url wrong",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "mock:"},
			},
			wantErr: true,
		},
		{
			name: "mock://home ok",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "mock://home", Properties: map[string]string{"pvc": "paddleflow1", "namespace": "default"}},
			},
			wantErr: false,
		},
		{
			name: "mock://home wrong",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "mock://home", Properties: map[string]string{"pvc": "paddleflow1"}},
			},
			wantErr: true,
		},
		{
			name: "mock:// wrong",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "mock://"},
			},
			wantErr: true,
		},
		{
			name: "mock://root",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "mock://root"},
			},
			wantErr: true,
		},
		{
			name: "mock://root/abc",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "mock://root/abc"},
			},
			wantErr: true,
		},
		{
			name: "wrong file system",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "test://1123"},
			},
			wantErr: true,
		},
		{
			name: "no user id",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "testname", Url: "test://1123"},
			},
			wantErr: true,
		},
		{
			name: "username is wrong 1",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "test@@@@", Url: "test://1123"},
			},
			wantErr: true,
		},
		{
			name: "username is wrong 2",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "123456789", Url: "test://1123"},
			},
			wantErr: true,
		},
		{
			name: "username is wrong 2",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{Name: "1234444-", Url: "test://1123"},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateCreateFileSystem(tt.args.ctx, tt.args.req); (err != nil) != tt.wantErr {
				t.Errorf("validateCreateFileSystem() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_checkFsDir(t *testing.T) {
	type args struct {
		fsType     string
		url        string
		properties map[string]string
	}
	var p1 = gomonkey.ApplyFunc(models.GetSimilarityAddressList, func(fsType string, ips []string) ([]models.FileSystem, error) {
		return []models.FileSystem{
			{SubPath: "/data"},
			{SubPath: "/data/mypath"},
		}, nil
	})
	defer p1.Reset()
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "dir ok",
			args: args{
				fsType: common.HDFS,
				url:    "hdfs://192.168.1.1:9000,192.168.1.2:9000/myfs",
			},
			wantErr: false,
		},
		{
			name: "dir nested up",
			args: args{
				fsType: common.HDFS,
				url:    "hdfs://192.168.1.3:9000/data/mypath/path",
			},
			wantErr: true,
		},
		{
			name: "dir nested down",
			args: args{
				fsType: common.HDFS,
				url:    "hdfs://192.168.1.3:9000/data",
			},
			wantErr: true,
		},
		{
			name: "local",
			args: args{
				fsType: common.Local,
				url:    "local://mypath/data",
			},
			wantErr: false,
		},
		{
			name: "s3",
			args: args{
				fsType: common.S3,
				url:    "s3://bucket/datatest",
				properties: map[string]string{
					fsCommon.Endpoint: "s3.xxx.com",
				},
			},
			wantErr: false,
		},
		{
			name: "mock",
			args: args{
				fsType: common.Mock,
				url:    "mock://mypath/data",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkFsDir(tt.args.fsType, tt.args.url, tt.args.properties); (err != nil) != tt.wantErr {
				t.Errorf("checkFsDir() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_getListResult(t *testing.T) {
	type args struct {
		fsModel    []models.FileSystem
		marker     string
		nextMarker string
	}
	tests := []struct {
		name string
		args args
		want *fs.ListFileSystemResponse
	}{
		{
			name: "Truncated true",
			args: args{
				fsModel:    []models.FileSystem{{Name: "fsName"}},
				nextMarker: "2019-5-19 00:00:00",
				marker:     "2016-5-19 00:00:00",
			},
			want: &fs.ListFileSystemResponse{
				FsList: []*fs.FileSystemResponse{
					{
						Name: "fsName",
					},
				},
				NextMarker: "2019-5-19 00:00:00",
				Marker:     "2016-5-19 00:00:00",
				Truncated:  true,
			},
		},
		{
			name: "Truncated false",
			args: args{
				fsModel: []models.FileSystem{{Name: "fsName"}},
				marker:  "",
			},
			want: &fs.ListFileSystemResponse{
				FsList: []*fs.FileSystemResponse{
					{
						Name: "fsName",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getListResult(tt.args.fsModel, tt.args.nextMarker, tt.args.marker); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getListResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

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

func buildUpdateRequest(model models.FSCacheConfig) fs.UpdateFileSystemCacheRequest {
	return fs.UpdateFileSystemCacheRequest{
		FsID:      model.FsID,
		CacheDir:  model.CacheDir,
		Quota:     model.Quota,
		CacheType: model.CacheType,
		BlockSize: model.BlockSize,
	}
}

func buildCreateRequest(model models.FSCacheConfig) fs.CreateFileSystemCacheRequest {
	req := fs.CreateFileSystemCacheRequest{
		Username:                     MockRootUser,
		FsName:                       mockFsName,
		UpdateFileSystemCacheRequest: buildUpdateRequest(model),
	}
	return req
}

func TestFSCacheConfigRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	mockFs := buildMockFS()
	cacheConf := buildMockFSCacheConfig()
	updateReq := buildUpdateRequest(cacheConf)
	createRep := buildCreateRequest(cacheConf)

	// test create failure - no fs
	url := baseUrl + "/fsCache"
	result, err := PerformPostRequest(router, url, createRep)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusForbidden, result.Code)

	// test create success
	err = models.CreatFileSystem(&mockFs)
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
	assert.Equal(t, cacheConf.CacheType, cacheRsp.CacheType)
	assert.Equal(t, cacheConf.CacheDir, cacheRsp.CacheDir)
	assert.Equal(t, cacheConf.BlockSize, cacheRsp.BlockSize)
	assert.Equal(t, cacheConf.Quota, cacheRsp.Quota)
	// test fsToName()
	assert.Equal(t, createRep.Username, cacheRsp.Username)

	// test get failure
	urlWrong := url + "/666"
	result, err = PerformGetRequest(router, urlWrong)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusNotFound, result.Code)

	// test update success
	updateReq.Quota = 333
	updateReq.CacheDir = "newPath"
	result, err = PerformPutRequest(router, urlWithFsID, updateReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)

	result, err = PerformGetRequest(router, urlWithFsID)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)
	err = ParseBody(result.Body, &cacheRsp)
	assert.Nil(t, err)
	assert.Equal(t, updateReq.Quota, cacheRsp.Quota)
	assert.Equal(t, updateReq.CacheDir, cacheRsp.CacheDir)

	// test update failure
	result, err = PerformPutRequest(router, urlWrong, updateReq)
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

	var cache []models.FSCache
	tx := database.DB.Where(&models.FSCache{FsID: common.ID(MockRootUser, mockFsName)}).Find(&cache)
	assert.Equal(t, int64(1), tx.RowsAffected)
	assert.Equal(t, 100, cache[0].UsedSize)

	req.UsedSize = 200
	result, err = PerformPostRequest(router, url, req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)

	tx = database.DB.Where(&models.FSCache{FsID: common.ID(MockRootUser, mockFsName)}).Find(&cache)
	assert.Equal(t, int64(1), tx.RowsAffected)
	assert.Equal(t, 200, cache[0].UsedSize)
}

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
	assert.Equal(t, 2, len(listRsp.FsList))

	filters2 := "?" + util.QueryMountPoint + "=/var/2&" + util.QueryNodeName + "=abc&" + util.QueryClusterID + "=testcluster"
	result, err = PerformDeleteRequest(router, url+"/"+mockFsName+filters2)
	assert.Nil(t, err)

	result, err = PerformGetRequest(router, url+filters)
	assert.Nil(t, err)
	listRsp = fs.ListMountResponse{}
	err = ParseBody(result.Body, &listRsp)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(listRsp.FsList))
}
