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

package common

import (
	"testing"

	"paddleflow/pkg/fs/common"
)

func TestInformationFromUrl(t *testing.T) {
	type args struct {
		url        string
		properties map[string]string
	}
	tests := []struct {
		name               string
		args               args
		wantFileSystemType string
		wantServerAddress  string
		wantSubPath        string
	}{
		{
			name:               "one hdfs",
			args:               args{url: "hdfs://192.168.1.2:9000/myfs"},
			wantFileSystemType: "hdfs",
			wantServerAddress:  "192.168.1.2:9000",
			wantSubPath:        "/myfs",
		},
		{
			name:               "more hdfs",
			args:               args{url: "hdfs://192.168.1.2:9000,192.168.1.3:9000/myfs"},
			wantFileSystemType: "hdfs",
			wantServerAddress:  "192.168.1.2:9000,192.168.1.3:9000",
			wantSubPath:        "/myfs",
		},
		{
			name:               "hdfs path",
			args:               args{url: "hdfs://192.168.1.2:9000,192.168.1.3:9000/myfs/path"},
			wantFileSystemType: "hdfs",
			wantServerAddress:  "192.168.1.2:9000,192.168.1.3:9000",
			wantSubPath:        "/myfs/path",
		},
		{
			name:               "hdfs kerberos path",
			args:               args{url: "hdfs://192.168.1.2:9000,192.168.1.3:9000/myfs/path", properties: map[string]string{common.KeyTabData: "xxx"}},
			wantFileSystemType: HDFSWithKerberos,
			wantServerAddress:  "192.168.1.2:9000,192.168.1.3:9000",
			wantSubPath:        "/myfs/path",
		},
		{
			name:               "hdfs /",
			args:               args{url: "hdfs://127.0.0.1:9000/"},
			wantFileSystemType: "hdfs",
			wantServerAddress:  "127.0.0.1:9000",
			wantSubPath:        "/",
		},
		{
			name:               "local",
			args:               args{url: "local://data/myfs"},
			wantFileSystemType: "local",
			wantServerAddress:  "",
			wantSubPath:        "/data/myfs",
		},
		{
			name:               "s3",
			args:               args{url: "s3://bucket/path", properties: map[string]string{common.Endpoint: "192.168.1.4"}},
			wantFileSystemType: "s3",
			wantServerAddress:  "192.168.1.4",
			wantSubPath:        "/path",
		},
		{
			name:               "s3",
			args:               args{url: "s3://bucket", properties: map[string]string{common.Endpoint: "192.168.1.4"}},
			wantFileSystemType: "s3",
			wantServerAddress:  "192.168.1.4",
			wantSubPath:        "/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFileSystemType, gotServerAddress, gotSubPath := InformationFromURL(tt.args.url, tt.args.properties)
			if gotFileSystemType != tt.wantFileSystemType {
				t.Errorf("InformationFromURL() gotFileSystemType = %v, want %v", gotFileSystemType, tt.wantFileSystemType)
			}
			if gotServerAddress != tt.wantServerAddress {
				t.Errorf("InformationFromURL() gotServerAddress = %v, want %v", gotServerAddress, tt.wantServerAddress)
			}
			if gotSubPath != tt.wantSubPath {
				t.Errorf("InformationFromURL() gotSubPath = %v, want %v", gotSubPath, tt.wantSubPath)
			}
		})
	}
}

func TestCheckFsNested(t *testing.T) {
	type args struct {
		path1 string
		path2 string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "no confict",
			args: args{
				path1: "/root/path",
				path2: "/home",
			},
			want: false,
		},
		{
			name: "no confict 2",
			args: args{
				path1: "/root/path",
				path2: "/root_test",
			},
			want: false,
		},
		{
			name: "no confict 3",
			args: args{
				path1: "/root",
				path2: "/root_test",
			},
			want: false,
		},
		{
			name: "no confict 5",
			args: args{
				path1: "/long-xxxxxxxxxxxxxxxxxxxxxxx",
				path2: "/root_test/abc/test",
			},
			want: false,
		},
		{
			name: "confict 1",
			args: args{
				path1: "/root",
				path2: "/root/path",
			},
			want: true,
		},
		{
			name: "confict 2",
			args: args{
				path1: "/root/path",
				path2: "/root",
			},
			want: true,
		},
		{
			name: "confict 3",
			args: args{
				path1: "/root/path/",
				path2: "/root",
			},
			want: true,
		},
		{
			name: "confict 4",
			args: args{
				path1: "/root/path/",
				path2: "/root/path/abc",
			},
			want: true,
		},
		{
			name: "confict 5",
			args: args{
				path1: "/root",
				path2: "/root/",
			},
			want: true,
		},
		{
			name: "confict 6",
			args: args{
				path1: "/data/mypath/path",
				path2: "/data/mypath",
			},
			want: true,
		},
		{
			name: "confict 7",
			args: args{
				path1: "/",
				path2: "/",
			},
			want: true,
		},
		{
			name: "confict 8",
			args: args{
				path1: "/",
				path2: "/home",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckFsNested(tt.args.path1, tt.args.path2); got != tt.want {
				t.Errorf("CheckFsNested() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFSIDToName(t *testing.T) {
	type args struct {
		fsID string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "use fsname",
			args: args{
				fsID: "myfs",
			},
			want: "myfs",
		},
		{
			name: "use fsID",
			args: args{
				fsID: "fs-root-myfs",
			},
			want: "myfs",
		},
		{
			name: "use fsID with user has '-' ",
			args: args{
				fsID: "fs-user-test-myfs",
			},
			want: "myfs",
		},
		{
			name: "empty fsID",
			args: args{
				fsID: "",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FSIDToName(tt.args.fsID); got != tt.want {
				t.Errorf("FSIDToName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNameToFsID(t *testing.T) {
	type args struct {
		fsName   string
		userName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "fsname",
			args: args{
				fsName:   "fs-root-sftplll",
				userName: "root",
			},
			want: "fs-root-sftplll",
		},
		{
			name: "fsid",
			args: args{
				fsName:   "sftplll",
				userName: "root",
			},
			want: "fs-root-sftplll",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NameToFsID(tt.args.fsName, tt.args.userName); got != tt.want {
				t.Errorf("NameToFsID() = %v, want %v", got, tt.want)
			}
		})
	}
}
