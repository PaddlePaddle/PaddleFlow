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
	"syscall"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
)

func (a *Attr) FromFileInfo(info *base.FileInfo) {
	st := info.Sys.(syscall.Stat_t)
	if info.IsDir {
		a.Type = TypeDirectory
	} else {
		a.Type = TypeFile
	}
	a.Mode = uint32(st.Mode)
	a.Uid = st.Uid
	a.Gid = st.Gid
	a.Rdev = uint64(st.Rdev)
	a.Atime = st.Atimespec.Sec
	a.Mtime = st.Mtimespec.Sec
	a.Ctime = st.Ctimespec.Sec
	a.Atimensec = uint32(st.Atimespec.Nsec)
	a.Mtimensec = uint32(st.Mtimespec.Nsec)
	a.Ctimensec = uint32(st.Ctimespec.Nsec)
	a.Nlink = uint64(st.Nlink)
	a.Size = uint64(st.Size)
	a.Blksize = int64(st.Blksize)
	a.Block = st.Blocks
}
