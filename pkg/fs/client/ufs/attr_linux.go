// +build linux
// +build amd64 ppc64

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

import "syscall"

func fillStat(nlink uint64, mode uint32, uid uint32, gid uint32, size int64, blksize int64, blocks int64, atime, mtime, ctime syscall.Timespec) syscall.Stat_t {
	return syscall.Stat_t{
		Nlink:   nlink,
		Mode:    mode,
		Uid:     uid,
		Gid:     gid,
		Size:    size,
		Blksize: blksize,
		Blocks:  blocks,
		Atim:    atime,
		Mtim:    mtime,
		Ctim:    ctime,
	}
}
