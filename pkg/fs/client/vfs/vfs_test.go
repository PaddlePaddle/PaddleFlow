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
package vfs

import (
	"reflect"
	"syscall"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
)

func TestWrite(t *testing.T) {
	type args struct {
		ino Ino
		off uint64
		fh  uint64
	}
	tests := []struct {
		name    string
		args    args
		vfs     VFS
		want    []string
		wantErr bool
	}{
		{
			name:    "filehandler nil err",
			vfs:     VFS{},
			want:    []string{},
			wantErr: true,
		},
		{
			name: "writer_nil_err",
			args: args{ino: 1},
			vfs: VFS{
				handleMap: map[Ino][]*handle{Ino(1): []*handle{&handle{}}},
			},
			want:    []string{},
			wantErr: true,
		},
		{
			name: "writer write err",
			args: args{ino: 1},
			vfs: VFS{
				handleMap: map[Ino][]*handle{Ino(1): []*handle{&handle{
					writer: &fileWriter{},
				}}},
			},
			want:    []string{},
			wantErr: true,
		},
	}
	gomonkey.ApplyMethod(reflect.TypeOf(&fileWriter{}), "Write", func(_ *fileWriter, data []byte, offset uint64) syscall.Errno {
		return syscall.EBADF
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.vfs.Write(nil, tt.args.ino, []byte("test"), tt.args.fh, tt.args.off); (err != syscall.F_OK) != tt.wantErr {
				t.Errorf("write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

}
