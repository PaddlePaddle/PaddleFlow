/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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

package fuse

import (
	"reflect"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta"
)

func TestPFS_Write(t *testing.T) {
	type fields struct {
		debug         bool
		RawFileSystem fuse.RawFileSystem
	}
	type args struct {
		cancel <-chan struct{}
		input  *fuse.WriteIn
		data   []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   uint32
		want1  fuse.Status
	}{
		{
			name: "panic case",
			fields: fields{
				debug:         false,
				RawFileSystem: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &PFS{
				debug:         tt.fields.debug,
				RawFileSystem: tt.fields.RawFileSystem,
			}
			got, got1 := fs.Write(tt.args.cancel, tt.args.input, tt.args.data)
			if got != tt.want {
				t.Errorf("Write() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Write() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestPFS_Read(t *testing.T) {
	type fields struct {
		debug         bool
		RawFileSystem fuse.RawFileSystem
	}
	type args struct {
		cancel <-chan struct{}
		input  *fuse.ReadIn
		buf    []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   fuse.ReadResult
		want1  fuse.Status
	}{
		{
			name: "panic case",
			fields: fields{
				debug:         false,
				RawFileSystem: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &PFS{
				debug:         tt.fields.debug,
				RawFileSystem: tt.fields.RawFileSystem,
			}
			got, got1 := fs.Read(tt.args.cancel, tt.args.input, tt.args.buf)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Read() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Read() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestPFS_replyEntry(t *testing.T) {
	type fields struct {
		debug         bool
		RawFileSystem fuse.RawFileSystem
	}
	type args struct {
		entry *meta.Entry
		out   *fuse.EntryOut
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "test entry Attr mode == 0",
			fields: fields{
				debug:         false,
				RawFileSystem: nil,
			},
			args: args{
				entry: &meta.Entry{
					Attr: &meta.Attr{
						Mode: 0,
					},
				},
				out: &fuse.EntryOut{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &PFS{
				debug:         tt.fields.debug,
				RawFileSystem: tt.fields.RawFileSystem,
			}
			fs.replyEntry(tt.args.entry, tt.args.out)
		})
	}
}
