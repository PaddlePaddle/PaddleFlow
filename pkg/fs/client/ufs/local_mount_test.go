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

package ufs

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

func TestLocalMount(t *testing.T) {
	os.MkdirAll("./mock", 0755)
	fs := &localMount{
		localPath: "./mock",
	}
	fs.Mkdir("data1", 0755)
	fs.Mkdir("data1/data2", 0755)

	finfo, err := fs.GetAttr("data1/data2")
	assert.NoError(t, err)
	assert.Equal(t, true, finfo.IsDir)

	fh, err := fs.Create("data1/hello", uint32(os.O_WRONLY|os.O_CREATE), 0755)
	assert.NoError(t, err)
	content := []byte("hello world")
	fh.Write(content, 0)
	fh.Flush()
	fh.Release()

	fh, err = fs.Open("data1/hello", uint32(os.O_RDONLY), 11)
	assert.NoError(t, err)
	buf := make([]byte, 20)
	n, e := fh.Read(buf, 0)
	assert.Nil(t, e)
	assert.Equal(t, len(content), n)
	fh.Release()

	entries, err := fs.ReadDir("data1")
	assert.NoError(t, err)
	assert.LessOrEqual(t, 1, len(entries))

	out := fs.StatFs("data1/hello")
	assert.NotNil(t, out)

	err = fs.Rmdir("data1/data2")
	assert.NoError(t, err)

	os.RemoveAll("./mock")
}

func TestNewLocalMountFileSystem(t *testing.T) {
	type args struct {
		properties map[string]interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    UnderFileStorage
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "mount ok",
			args: args{
				map[string]interface{}{
					common.Type:    common.CFSType,
					common.Address: "xxxx.cfs.bj.baidubce.com",
					common.SubPath: "/abc",
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return false
			},
		},
		{
			name: "mount fail",
			args: args{
				map[string]interface{}{
					common.Type:    common.CFSType,
					common.Address: "xxxx.cfs.bj.baidubce.com",
					common.SubPath: "/abc",
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
		{
			name: "unmount fail",
			args: args{
				map[string]interface{}{
					common.Type:    common.CFSType,
					common.Address: "xxxx.cfs.bj.baidubce.com",
					common.SubPath: "/abc",
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
	}
	var p1 = gomonkey.ApplyFunc(utils.ExecMount, func(sourcePath, targetPath string, args []string) ([]byte, error) {
		return []byte("ok"), nil
	})
	defer p1.Reset()

	var p2 = gomonkey.ApplyFunc(utils.ManualUnmount, func(path string) error {
		return nil
	})
	defer p2.Reset()
	for _, tt := range tests {
		if tt.name == "mount fail" {
			var p3 = gomonkey.ApplyFunc(utils.ExecMount, func(sourcePath, targetPath string, args []string) ([]byte, error) {
				return []byte("mount fail"), errors.New("mount fail")
			})
			defer p3.Reset()
		}
		if tt.name == "unmount fail" {
			var p4 = gomonkey.ApplyFunc(utils.ExecMount, func(sourcePath, targetPath string, args []string) ([]byte, error) {
				return []byte("ok"), nil
			})
			defer p4.Reset()

			var p5 = gomonkey.ApplyFunc(utils.ManualUnmount, func(path string) error {
				return errors.New("umount fail")
			})
			defer p5.Reset()
		}
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLocalMountFileSystem(tt.args.properties)
			if !tt.wantErr(t, err, fmt.Sprintf("NewLocalMountFileSystem(%v)", tt.args.properties)) {
				return
			}
			assert.Equalf(t, tt.want, got, "NewLocalMountFileSystem(%v)", tt.args.properties)
		})
	}
}
