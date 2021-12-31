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

package meta

import (
	"reflect"
	"sync"
	"testing"
)

var inode1 = &Inode{inode: 1, name: "", child: map[string]Ino{"a": 2}, parent: nil}
var inode2 = &Inode{inode: 2, name: "a", child: map[string]Ino{"b": 3}, parent: inode1}
var inode3 = &Inode{inode: 3, name: "b", child: map[string]Ino{"c": 4}, parent: inode2}
var inode4 = &Inode{inode: 4, name: "c", child: nil, parent: inode3}

var handles = map[Ino]*Inode{
	4: inode4,
	3: inode3,
	2: inode2,
	1: inode1,
}

func TestInodeHandle_PathToInode(t *testing.T) {
	type fields struct {
		RWMutex    sync.RWMutex
		handles    map[Ino]*Inode
		nextNodeID uint64
	}
	type args struct {
		path string
	}
	tests := []struct {
		name      string
		args      args
		fields    fields
		wantInode *Inode
	}{
		{
			name: "path abc",
			args: args{
				"/a/b/c",
			},
			fields: fields{
				handles: handles,
			},
			wantInode: inode4,
		},
		{
			name: "path ab",
			args: args{
				"/a/b",
			},
			fields: fields{
				handles: handles,
			},
			wantInode: inode3,
		},
	}
	for _, tt := range tests {
		m := &InodeHandle{
			RWMutex:    tt.fields.RWMutex,
			handles:    tt.fields.handles,
			nextNodeID: tt.fields.nextNodeID,
		}
		t.Run(tt.name, func(t *testing.T) {
			if gotInode := m.PathToInode(tt.args.path); !reflect.DeepEqual(gotInode, tt.wantInode) {
				t.Errorf("PathToInode() = %v, want %v", gotInode, tt.wantInode)
			}
		})
	}
}

func TestInodeHandle_InodeToPath(t *testing.T) {
	type fields struct {
		RWMutex    sync.RWMutex
		handles    map[Ino]*Inode
		nextNodeID uint64
	}
	type args struct {
		i *Inode
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "path abc",
			fields: fields{
				handles: handles,
			},
			args: args{
				i: inode4,
			},
			want: "/a/b/c",
		},
		{
			name: "path root",
			fields: fields{
				handles: handles,
			},
			args: args{
				i: inode1,
			},
			want: "/",
		},
	}
	for _, tt := range tests {
		m := &InodeHandle{
			RWMutex:    tt.fields.RWMutex,
			handles:    tt.fields.handles,
			nextNodeID: tt.fields.nextNodeID,
		}
		t.Run(tt.name, func(t *testing.T) {
			if got := m.InodeToPath(tt.args.i); got != tt.want {
				t.Errorf("InodeToPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInodeHandle_ParentInodeToPath(t *testing.T) {
	type fields struct {
		RWMutex    sync.RWMutex
		handles    map[Ino]*Inode
		nextNodeID uint64
	}
	type args struct {
		parent *Inode
		name   string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "path /test1 with parent is root",
			args: args{
				parent: inode1,
				name:   "test1",
			},
			fields: fields{
				handles: handles,
			},
			want: "/test1",
		},
		{
			name: "path /a/b/test1 with parent is inode3",
			args: args{
				parent: inode3,
				name:   "test1",
			},
			fields: fields{
				handles: handles,
			},
			want: "/a/b/test1",
		},
		{
			name: "path /a/b/test1 with parent is inode3 and name with prefix['/']",
			args: args{
				parent: inode3,
				name:   "/test1",
			},
			fields: fields{
				handles: handles,
			},
			want: "/a/b/test1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &InodeHandle{
				RWMutex:    tt.fields.RWMutex,
				handles:    tt.fields.handles,
				nextNodeID: tt.fields.nextNodeID,
			}
			if got := m.ParentInodeToPath(tt.args.parent, tt.args.name); got != tt.want {
				t.Errorf("ParentInodeToPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
