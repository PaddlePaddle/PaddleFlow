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
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	k8sCore "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	fsCommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	Ori_ak       = "ori_ak"
	Ori_sk       = "ori_sk"
	Ori_Bucket   = "ori_bucket"
	Test_SubPath = "testpath"
)

func Test_validateCreateFileSystem(t *testing.T) {
	type args struct {
		ctx *logger.RequestContext
		req *fs.CreateFileSystemRequest
	}

	var p1 = gomonkey.ApplyFunc(storage.Filesystem.GetSimilarityAddressList, func(fsType string, ips []string) ([]model.FileSystem, error) {
		return []model.FileSystem{}, nil
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
		{
			name: "username and filename is wrong",
			args: args{
				ctx: ctx,
				req: &fs.CreateFileSystemRequest{
					Name:     RandomString(9),
					Username: RandomString(51),
					Url:      "test://1123",
				},
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
	var p1 = gomonkey.ApplyMethod(reflect.TypeOf(storage.Filesystem), "GetSimilarityAddressList",
		func(_ *storage.FilesystemStore, fsType string, ips []string) ([]model.FileSystem, error) {
			return []model.FileSystem{
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
				fsType: fsCommon.HDFSType,
				url:    "hdfs://192.168.1.1:9000,192.168.1.2:9000/myfs",
			},
			wantErr: false,
		},
		{
			name: "dir nested up",
			args: args{
				fsType: fsCommon.HDFSType,
				url:    "hdfs://192.168.1.3:9000/data/mypath/path",
			},
			wantErr: true,
		},
		{
			name: "dir nested down",
			args: args{
				fsType: fsCommon.HDFSType,
				url:    "hdfs://192.168.1.3:9000/data",
			},
			wantErr: true,
		},
		{
			name: "local",
			args: args{
				fsType: fsCommon.LocalType,
				url:    "local://mypath/data",
			},
			wantErr: false,
		},
		{
			name: "s3",
			args: args{
				fsType: fsCommon.S3Type,
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
				fsType: fsCommon.MockType,
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
		fsModel    []model.FileSystem
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
				fsModel:    []model.FileSystem{{Name: "fsName"}},
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
				fsModel: []model.FileSystem{{Name: "fsName"}},
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

func TestCreateFSDuplicateName(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	str, err := os.Getwd()
	defer func() {
		os.RemoveAll(str + "/fs")
	}()
	assert.Nil(t, err)
	createFsReq := fs.CreateFileSystemRequest{
		Name: mockFsName,
		Url:  "local:/" + str + "/fs",
		Properties: map[string]string{
			"debug": "true",
		},
	}

	fsUrl := baseUrl + "/fs"
	result, err := PerformPostRequest(router, fsUrl, createFsReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, result.Code)

	result, err = PerformPostRequest(router, fsUrl, createFsReq)
	assert.Equal(t, http.StatusBadRequest, result.Code)
}

func TestCreateFSAndDeleteFs(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	// mockFs := buildMockFS()
	// cacheConf := buildMockFSCacheConfig()
	str, err := os.Getwd()
	defer func() {
		os.RemoveAll(str + "/fs")
	}()
	assert.Nil(t, err)
	createFsReq := fs.CreateFileSystemRequest{
		Name: mockFsName,
		Url:  "local:/" + str + "/fs",
		Properties: map[string]string{
			"debug": "true",
		},
	}

	fsUrl := baseUrl + "/fs"
	result, err := PerformPostRequest(router, fsUrl, createFsReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, result.Code)
	// test delete fs successful
	var p1 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(fs.GetFileSystemService()), "checkFsMountedAllClustersAndScheduledJobs",
		func(_ *fs.FileSystemService, fsID string) (bool, error) {
			return false, nil
		})
	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(fs.GetFileSystemService()), "cleanFsResources",
		func(_ *fs.FileSystemService, runtimePodsMap map[*runtime.KubeRuntime][]k8sCore.Pod, fsID string) (err error) {
			return nil
		})
	defer p2.Reset()
	deleteUrl := fsUrl + "/" + mockFsName
	result, err = PerformDeleteRequest(router, deleteUrl)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)

	p1.Reset()

	// test fs mounted
	p1 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(fs.GetFileSystemService()), "checkFsMountedAllClustersAndScheduledJobs",
		func(_ *fs.FileSystemService, fsID string) (bool, error) {
			return true, nil
		})
	defer p1.Reset()
	result, err = PerformPostRequest(router, fsUrl, createFsReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, result.Code)

	deleteUrl = fsUrl + "/" + mockFsName
	result, err = PerformDeleteRequest(router, deleteUrl)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusForbidden, result.Code)
}

func RandomString(n int) string {
	var letterRunes = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestCreateStsError(t *testing.T) {
	ak := "abc"
	sk := "xxx"
	router, baseUrl := prepareDBAndAPI(t)

	createFsReq := fs.CreateFileSystemRequest{
		Name: mockFsName,
		Url:  "bos://" + Test_SubPath,
		Properties: map[string]string{
			"accessKey": ak,
			"endpoint":  "bj.bcebos.com",
			"region":    "bj",
			"secretKey": sk,
			"sts":       "true",
			"duration":  "10",
		},
	}

	fsUrl := baseUrl + "/fs"
	result, err := PerformPostRequest(router, fsUrl, createFsReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, result.Code)

	createFsReq = fs.CreateFileSystemRequest{
		Name: mockFsName,
		Url:  "bos://" + Test_SubPath,
		Properties: map[string]string{
			"accessKey": ak,
			"endpoint":  "bj.bcebos.com",
			"region":    "bj",
			"secretKey": sk,
			"sts":       "true",
			"duration":  "abc",
		},
	}

	fsUrl = baseUrl + "/fs"
	result, err = PerformPostRequest(router, fsUrl, createFsReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, result.Code)

}

func TestCreateBos(t *testing.T) {
	ak := os.Getenv(Ori_ak)
	sk := os.Getenv(Ori_sk)
	bucket := os.Getenv(Ori_Bucket)
	if bucket == "" || sk == "" || ak == "" {
		log.Info("no ak or sk")
		return
	}
	router, baseUrl := prepareDBAndAPI(t)
	createFsReq := fs.CreateFileSystemRequest{
		Name: mockFsName,
		Url:  "bos://" + bucket + "/" + Test_SubPath,
		Properties: map[string]string{
			"accessKey": ak,
			"endpoint":  "bj.bcebos.com",
			"region":    "bj",
			"sts":       "true",
			"duration":  "70",
		},
	}

	fsUrl := baseUrl + "/fs"
	result, err := PerformPostRequest(router, fsUrl, createFsReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, result.Code)

	createFsReq = fs.CreateFileSystemRequest{
		Name: mockFsName,
		Url:  "bos://" + bucket + "/" + Test_SubPath,
		Properties: map[string]string{
			"accessKey": ak,
			"region":    "bj",
			"secretKey": sk,
			"sts":       "true",
			"duration":  "70",
		},
	}

	fsUrl = baseUrl + "/fs"
	result, err = PerformPostRequest(router, fsUrl, createFsReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, result.Code)

	createFsReq = fs.CreateFileSystemRequest{
		Name: mockFsName,
		Url:  "bos://" + Test_SubPath,
		Properties: map[string]string{
			"accessKey": ak,
			"endpoint":  "bj.bcebos.com",
			"region":    "bj",
			"secretKey": sk,
			"sts":       "true",
			"duration":  "70",
		},
	}

	fsUrl = baseUrl + "/fs"
	result, err = PerformPostRequest(router, fsUrl, createFsReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, result.Code)
}

func TestStsAPI(t *testing.T) {
	ak := os.Getenv(Ori_ak)
	sk := os.Getenv(Ori_sk)
	bucket := os.Getenv(Ori_Bucket)
	if bucket == "" || sk == "" || ak == "" {
		log.Info("no ak or sk")
		return
	}
	router, baseUrl := prepareDBAndAPI(t)
	createFsReq := fs.CreateFileSystemRequest{
		Name: mockFsName,
		Url:  "bos://" + bucket + "/" + Test_SubPath,
		Properties: map[string]string{
			"accessKey": ak,
			"endpoint":  "bj.bcebos.com",
			"region":    "bj",
			"secretKey": sk,
			"sts":       "true",
			"duration":  "70",
		},
	}

	fsUrl := baseUrl + "/fs"
	result, err := PerformPostRequest(router, fsUrl, createFsReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, result.Code)

	fsUrlSts := baseUrl + "/fsSts/" + mockFsName
	result, err = PerformGetRequest(router, fsUrlSts)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)

	ak_ := "bad"
	sk_ := "bad"

	fsUrl = baseUrl + "/fs"

	createFsReq = fs.CreateFileSystemRequest{
		Name: mockFsName,
		Url:  "bos://" + bucket + "/" + Test_SubPath,
		Properties: map[string]string{
			"accessKey": ak_,
			"endpoint":  "bj.bcebos.com",
			"region":    "bj",
			"secretKey": sk_,
			"sts":       "true",
			"duration":  "70",
		},
	}
	result, err = PerformPostRequest(router, fsUrl, createFsReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, result.Code)

	fsUrlSts = baseUrl + "/fsSts/no"
	result, err = PerformGetRequest(router, fsUrlSts)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusNotFound, result.Code)

	creates3Req := fs.CreateFileSystemRequest{
		Name: mockFsName + "1",
		Url:  "s3://" + bucket + "/" + Test_SubPath,
		Properties: map[string]string{
			"accessKey": ak,
			"endpoint":  "s3.bj.bcebos.com",
			"region":    "bj",
			"secretKey": sk,
		},
	}

	fsUrl = baseUrl + "/fs"
	result, err = PerformPostRequest(router, fsUrl, creates3Req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, result.Code)

	fsUrlSts = baseUrl + "/fsSts/" + mockFsName + "1"
	result, err = PerformGetRequest(router, fsUrlSts)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusInternalServerError, result.Code)

	createFsReq = fs.CreateFileSystemRequest{
		Name: mockFsName + "falseSts",
		Url:  "bos://" + bucket + "/" + Test_SubPath,
		Properties: map[string]string{
			"accessKey": ak,
			"endpoint":  "bj.bcebos.com",
			"region":    "bj",
			"secretKey": sk,
			"duration":  "70",
		},
	}
	fsUrl = baseUrl + "/fs"
	result, err = PerformPostRequest(router, fsUrl, createFsReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, result.Code)

	fsUrlSts = baseUrl + "/fsSts/" + mockFsName + "falseSts"
	result, err = PerformGetRequest(router, fsUrlSts)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusInternalServerError, result.Code)

}
