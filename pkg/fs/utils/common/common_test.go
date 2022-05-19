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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
			if fsName, _ := FsIDToFsNameUsername(tt.args.fsID); fsName != tt.want {
				t.Errorf("FSIDToName() = %v, want %v", fsName, tt.want)
			}
		})
	}
}
