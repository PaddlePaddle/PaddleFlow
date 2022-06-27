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
	"os"
	"syscall"
)

func fillStat(nlink uint64, mode uint32, uid uint32, gid uint32, size int64, blksize int64, blocks int64, atime, mtime, ctime syscall.Timespec) syscall.Stat_t {
	return syscall.Stat_t{
		Nlink:     uint16(nlink),
		Mode:      uint16(mode),
		Uid:       uid,
		Gid:       gid,
		Size:      size,
		Blksize:   int32(blksize),
		Blocks:    blocks,
		Atimespec: atime,
		Mtimespec: mtime,
		Ctimespec: ctime,
	}
}

func (fs *hdfsFileSystem) sysHdfsToAttr(info os.FileInfo) (a Attr) {
	st := fs.statFromFileInfo(info)
	if info.IsDir() {
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
	return a
}

func (fs *sftpFileSystem) sysSFTPToAttr(info os.FileInfo) (a Attr) {
	st := fs.statFromFileInfo(info)
	if info.IsDir() {
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
	return a
}

func sysToAttr(info os.FileInfo) (a Attr) {
	st := info.Sys().(*syscall.Stat_t)
	if info.IsDir() {
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
	return a
}
