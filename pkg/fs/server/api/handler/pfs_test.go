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

package handler

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/server/api/request"
	"paddleflow/pkg/fs/server/api/response"
	"paddleflow/pkg/fs/server/model"
	"paddleflow/pkg/fs/server/utils/fs"
)

func Test_validateCreateFileSystem(t *testing.T) {
	type args struct {
		ctx *logger.RequestContext
		req *request.CreateFileSystemRequest
	}

	var p1 = gomonkey.ApplyFunc(model.GetSimilarityAddressList, func(fsType string, ips []string) ([]model.FileSystem, error) {
		return []model.FileSystem{}, nil
	})
	defer p1.Reset()
	var p2 = gomonkey.ApplyFunc(checkStorageConnectivity, func(fsMeta base.FSMeta) error {
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
			name: "url error",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Url: "httpxxxx", Username: "Test"},
			},
			wantErr: true,
		},
		{
			name: "hdfs url miss path",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Url: "hdfs://192.168.1.2:9000,192.168.1.3:9000", Username: "Test"},
			},
			wantErr: true,
		},
		{
			name: "hdfs /",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "hdfs://127.0.0.1:9000/", Properties: map[string]string{"user": "test", "group": "test"}},
			},
			wantErr: false,
		},
		{
			name: "hdfs /myfs/data",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "hdfs://127.0.0.1:9000/myfs/data", Properties: map[string]string{"user": "test", "group": "test"}},
			},
			wantErr: false,
		},
		{
			name: "hdfs url miss address",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "hdfs://path"},
			},
			wantErr: true,
		},
		{
			name: "s3 url no path wrong",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "s3://127.0.0.1"},
			},
			wantErr: true,
		},
		{
			name: "s3 url no bucket wrong",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "s3://127.0.0.1/", Properties: map[string]string{}},
			},
			wantErr: true,
		},
		{
			name: "s3 url has only bucket",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "s3://bucket/", Properties: map[string]string{base.Endpoint: "bj.bos.com", base.Region: "bj", base.AccessKey: "testak", base.SecretKey: "testsk"}},
			},
			wantErr: false,
		},
		{
			name: "s3 url bucket data",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "s3://bucket/subpath", Properties: map[string]string{base.Endpoint: "bj.bos.com", base.Region: "bj", base.AccessKey: "testak", base.SecretKey: "testsk"}},
			},
			wantErr: false,
		},
		{
			name: "local url wrong",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "local:"},
			},
			wantErr: true,
		},
		{
			name: "local:://home ok",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "local:://home", Properties: map[string]string{"debug": "true"}},
			},
			wantErr: false,
		},
		{
			name: "local::// wrong",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "local:://"},
			},
			wantErr: true,
		},
		{
			name: "local:://root",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "local:://root"},
			},
			wantErr: true,
		},
		{
			name: "local:://root/abc",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "local:://root/abc"},
			},
			wantErr: true,
		},
		{
			name: "mock url wrong",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "mock:"},
			},
			wantErr: true,
		},
		{
			name: "mock://home ok",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "mock://home", Properties: map[string]string{"pvc": "paddleflow1", "namespace": "default"}},
			},
			wantErr: false,
		},
		{
			name: "mock://home wrong",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "mock://home", Properties: map[string]string{"pvc": "paddleflow1"}},
			},
			wantErr: true,
		},
		{
			name: "mock:// wrong",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "mock://"},
			},
			wantErr: true,
		},
		{
			name: "mock://root",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "mock://root"},
			},
			wantErr: true,
		},
		{
			name: "mock://root/abc",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "mock://root/abc"},
			},
			wantErr: true,
		},
		{
			name: "wrong file system",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Username: "testUsername", Url: "test://1123"},
			},
			wantErr: true,
		},
		{
			name: "no user id",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "testname", Url: "test://1123"},
			},
			wantErr: true,
		},
		{
			name: "username is wrong 1",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "test@@@@", Url: "test://1123"},
			},
			wantErr: true,
		},
		{
			name: "username is wrong 2",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "123456789", Url: "test://1123"},
			},
			wantErr: true,
		},
		{
			name: "username is wrong 2",
			args: args{
				ctx: ctx,
				req: &request.CreateFileSystemRequest{Name: "1234444-", Url: "test://1123"},
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
	var p1 = gomonkey.ApplyFunc(model.GetSimilarityAddressList, func(fsType string, ips []string) ([]model.FileSystem, error) {
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
				fsType: fs.HDFS,
				url:    "hdfs://192.168.1.1:9000,192.168.1.2:9000/myfs",
			},
			wantErr: false,
		},
		{
			name: "dir nested up",
			args: args{
				fsType: fs.HDFS,
				url:    "hdfs://192.168.1.3:9000/data/mypath/path",
			},
			wantErr: true,
		},
		{
			name: "dir nested down",
			args: args{
				fsType: fs.HDFS,
				url:    "hdfs://192.168.1.3:9000/data",
			},
			wantErr: true,
		},
		{
			name: "local",
			args: args{
				fsType: fs.Local,
				url:    "local://mypath/data",
			},
			wantErr: false,
		},
		{
			name: "s3",
			args: args{
				fsType: fs.S3,
				url:    "s3://bucket/datatest",
				properties: map[string]string{
					base.Endpoint: "s3.baidu.com",
				},
			},
			wantErr: false,
		},
		{
			name: "mock",
			args: args{
				fsType: fs.Mock,
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
		want *response.ListFileSystemResponse
	}{
		{
			name: "Truncated true",
			args: args{
				fsModel:    []model.FileSystem{{Name: "fsName"}},
				nextMarker: "2019-5-19 00:00:00",
				marker:     "2016-5-19 00:00:00",
			},
			want: &response.ListFileSystemResponse{
				FsList: []*response.FileSystemResponse{
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
			want: &response.ListFileSystemResponse{
				FsList: []*response.FileSystemResponse{
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
