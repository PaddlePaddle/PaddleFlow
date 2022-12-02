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

package utils

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func TestGetVolumeMountPath(t *testing.T) {
	value := "/var/lib/kubelet/pods/uid/volumes/kubernetes.io~csi/pv/mount"
	path := GetVolumeMountPath("/var/lib/kubelet/pods/uid/volumes/kubernetes.io~csi/pv")
	assert.Equal(t, value, path)
}

func TestGetKubeletDataPath(t *testing.T) {
	// case1
	value := "/var/lib/kubelet"
	path := GetKubeletDataPath()
	assert.Equal(t, path, value)

	// case2
	newValue := "/var/lib/test"
	os.Setenv(KubeletDataPathEnv, newValue)
	path = GetKubeletDataPath()
	assert.Equal(t, path, newValue)
}

func TestGetDefaultUID(t *testing.T) {
	// case1
	value := 601
	uid := GetDefaultUID()
	assert.Equal(t, uid, value)
}

func TestGetDefaultGID(t *testing.T) {
	// case1
	value := 601
	uid := GetDefaultGID()
	assert.Equal(t, uid, value)
}

func TestGetFsNameAndUserNameByFsID(t *testing.T) {
	type args struct {
		fsID string
	}
	tests := []struct {
		name         string
		args         args
		wantUserName string
		wantFsName   string
	}{
		{
			name: "fs-root-abc",
			args: args{
				fsID: "fs-root-abc",
			},
			wantFsName:   "abc",
			wantUserName: "root",
		},
		{
			name: "fs-root-v-xxx",
			args: args{
				fsID: "fs-root-v-xxx",
			},
			wantFsName:   "v-xxx",
			wantUserName: "root",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFsName, gotUserName, err := GetFsNameAndUserNameByFsID(tt.args.fsID)
			assert.Nil(t, err)
			if gotUserName != tt.wantUserName {
				t.Errorf("GetFsNameAndUserNameByFsID() gotUserName = %v, want %v", gotUserName, tt.wantUserName)
			}
			if gotFsName != tt.wantFsName {
				t.Errorf("GetFsNameAndUserNameByFsID() gotFsName = %v, want %v", gotFsName, tt.wantFsName)
			}
		})
	}
}

func TestProcessFSInfo(t *testing.T) {
	fs := model.FileSystem{
		Model: model.Model{
			ID:        "fs-root-testfs",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		UserName:      "root",
		Name:          "testfs",
		Type:          "s3",
		SubPath:       "/supath",
		ServerAddress: "server_address",

		PropertiesMap: map[string]string{
			"accessKey": "accessKey",
			"bucket":    "bucket",
			"endpoint":  "server_address",
			"region":    "bj",
			"secretKey": "secretKey"},
	}
	fsStr, err := json.Marshal(fs)
	assert.Nil(t, err)
	fsBase64 := base64.StdEncoding.EncodeToString(fsStr)

	fsInfo, err := ProcessFSInfo(fsBase64)
	assert.Nil(t, err)
	assert.Equal(t, fs.IndependentMountProcess, fsInfo.IndependentMountProcess)
	assert.Equal(t, fs.SubPath, fsInfo.SubPath)
	assert.Equal(t, fs.ServerAddress, fsInfo.ServerAddress)

	errBase64 := "testxxxx"
	_, err = ProcessFSInfo(errBase64)
	assert.NotNil(t, err)
}

func TestProcessCacheConfig1(t *testing.T) {
	cacheDir := "/data/paddleflow-FS/mnt"
	blockSize := 4096
	fsCache := model.FSCacheConfig{
		FsID:       "fs-root-testfs",
		CacheDir:   cacheDir,
		MetaDriver: "nutsdb",
		BlockSize:  blockSize,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	fsCacheStr, err := json.Marshal(fsCache)
	assert.Nil(t, err)
	fsCacheBase64 := base64.StdEncoding.EncodeToString(fsCacheStr)

	cacheInfo, err := ProcessCacheConfig(fsCacheBase64)

	assert.Nil(t, err)
	assert.Equal(t, cacheDir, cacheInfo.CacheDir)
	assert.Equal(t, blockSize, cacheInfo.BlockSize)
	assert.Equal(t, false, cacheInfo.Debug)
}

func TestGetPodUIDFromTargetPath(t *testing.T) {
	type args struct {
		targetPath string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "get pod uid",
			args: args{
				targetPath: "/var/lib/kubelet/pods/cb0b4bb0-98de-4cd5-9d73-146a226dcf93/volumes/kubernetes.io~csi/pfs-fs-root-mxy-default-pv/mount",
			},
			want: "cb0b4bb0-98de-4cd5-9d73-146a226dcf93",
		},
	}
	os.Setenv(KubeletDataPathEnv, "")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetPodUIDFromTargetPath(tt.args.targetPath); got != tt.want {
				t.Errorf("GetPodUIDFromTargetPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetCpuPercent(t *testing.T) {
	tests := []struct {
		name string
		want float64
	}{
		{
			name: "ok",
			want: 0,
		},
	}
	for _, tt := range tests {
		cpuP := GetSysCpuPercent()
		if tt.want > cpuP {
			t.Errorf("GetSysCpuPercent() = %v and want %v", cpuP, tt.want)
		}
	}
}

func TestGetMemPercent(t *testing.T) {
	tests := []struct {
		name string
		want float64
	}{
		{
			name: "ok",
			want: 0,
		},
	}
	for _, tt := range tests {
		cpuP := GetSysCpuPercent()
		if tt.want > cpuP {
			t.Errorf("GetSysMemPercent() = %v and want %v", cpuP, tt.want)
		}
	}
}

func TestGetDiskPercent(t *testing.T) {
	tests := []struct {
		name string
		want float64
	}{
		{
			name: "ok",
			want: 0,
		},
	}
	for _, tt := range tests {
		cpuP := GetDiskPercent()
		if tt.want > cpuP {
			t.Errorf("GetSysMemPercent() = %v and want %v", cpuP, tt.want)
		}
	}
}

func TestGetProcessCPUPercent(t *testing.T) {
	tests := []struct {
		name string
		want float64
	}{
		{
			name: "ok",
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			GetProcessCPUPercent()
		})
	}
}

func TestGetProcessMemPercent(t *testing.T) {
	tests := []struct {
		name string
		want float32
	}{
		{
			name: "ok",
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			GetProcessMemPercent()
		})
	}
}
