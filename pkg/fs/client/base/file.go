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

package base

import (
	"os"
	"path"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type DirEntry fuse.DirEntry

type FileInfo struct {
	Name  string // relative path of file
	Path  string // full path of file
	Size  int64
	Mtime uint64
	IsDir bool
	Owner string
	Group string
	Mode  os.FileMode // file mode, not stat mode
	Sys   interface{} // underlying data source (can return nil)
}

type StatfsOut fuse.StatfsOut

func (f *FileInfo) FixLinkPrefix(prefix string) {
	// Link的根目录，即为Link文件
	if f.Name == "" || f.Name == "/" {
		f.Mode = f.Mode | os.ModeSymlink
	}
	f.Name = path.Join(prefix, f.Name)
}

func ToDirEntryList(dirs []DirEntry) []fuse.DirEntry {
	fuseDirs := make([]fuse.DirEntry, len(dirs))
	for i := range dirs {
		fuseDirs[i] = fuse.DirEntry(dirs[i])
	}
	return fuseDirs
}
