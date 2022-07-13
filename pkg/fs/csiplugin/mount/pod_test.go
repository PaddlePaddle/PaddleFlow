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

package mount

import (
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/client/pfs"
)

func Test_getMountCmd(t *testing.T) {
	type args struct {
		mountInfo pfs.MountInfo
		cacheConf common.FsCacheConfig
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "glusterfs",
			args: args{
				mountInfo: pfs.MountInfo{
					Type:          common.GlusterfsType,
					ServerAddress: "127.0.0.1",
					SubPath:       "default-volume",
				},
			},
			want: "mkdir -p /home/paddleflow/mnt/storage;mount -t glusterfs 127.0.0.1:default-volume/home/paddleflow/mnt/storage;while true; sleep 1; done;",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getMountCmd(tt.args.mountInfo, tt.args.cacheConf); got != tt.want {
				t.Errorf("getMountCmd() = %v, want %v", got, tt.want)
			}
		})
	}
}
