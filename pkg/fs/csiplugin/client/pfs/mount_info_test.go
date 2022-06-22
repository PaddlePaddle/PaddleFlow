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

package pfs

import (
	"reflect"
	"testing"

	"paddleflow/pkg/fs/csiplugin/csiconfig"
)

func TestMountInfo_GetMountCmd(t *testing.T) {
	type fields struct {
		Server        string
		FSID          string
		TargetPath    string
		LocalPath     string
		UsernameRoot  string
		PasswordRoot  string
		ClusterID     string
		Type          string
		ServerAddress string
		SubPath       string
		UID           int
		GID           int
		Options       []string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		want1  []string
	}{
		{
			name: "glusterfs",
			fields: fields{
				FSID:          "fs-root-1",
				Type:          "glusterfs",
				ServerAddress: "127.0.0.1",
				SubPath:       "default-volume",
				LocalPath:     "/mnt/test",
			},
			want:  "mount",
			want1: []string{"-t", "glusterfs", "127.0.0.1:default-volume", "/mnt/test"},
		},
		{
			name: "glusterfs-with-log",
			fields: fields{
				FSID:          "fs-root-1",
				Type:          "glusterfs",
				ServerAddress: "127.0.0.1",
				SubPath:       "default-volume",
				LocalPath:     "/mnt/test",
			},
			want:  "mount",
			want1: []string{"-t", "glusterfs", "-o", "log-level=TRACE", "-o", "log-file=/home/paddleflow/log/fs-root-1.log", "127.0.0.1:default-volume", "/mnt/test"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MountInfo{
				Server:        tt.fields.Server,
				FSID:          tt.fields.FSID,
				TargetPath:    tt.fields.TargetPath,
				LocalPath:     tt.fields.LocalPath,
				UsernameRoot:  tt.fields.UsernameRoot,
				PasswordRoot:  tt.fields.PasswordRoot,
				ClusterID:     tt.fields.ClusterID,
				Type:          tt.fields.Type,
				ServerAddress: tt.fields.ServerAddress,
				SubPath:       tt.fields.SubPath,
				UID:           tt.fields.UID,
				GID:           tt.fields.GID,
				Options:       tt.fields.Options,
			}
			if tt.name == "glusterfs-with-log" {
				csiconfig.GlusterFsLogLevel = "TRACE"
			}
			got, got1 := m.GetMountCmd()
			if got != tt.want {
				t.Errorf("GetMountCmd() got = %v, want %v", got, tt.want)
			}

			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("GetMountCmd() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
