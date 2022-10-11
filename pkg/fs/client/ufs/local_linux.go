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
	"fmt"
	"syscall"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
)

// Attributes.  This function is the main entry point, through
// which FUSE discovers which files and directories exist.
//
// If the filesystem wants to implement hard-links, it should
// return consistent non-zero FileInfo.Ino data.  Using
// hardlinks incurs a performance hit.
func (fs *localFileSystem) GetAttr(name string) (*base.FileInfo, error) {
	fullPath := fs.GetPath(name)
	var err error = nil
	st := syscall.Stat_t{}
	if name == "" {
		// When GetAttr is called for the toplevel directory, we always want
		// to look through symlinks.
		err = syscall.Stat(fullPath, &st)
	} else {
		err = syscall.Lstat(fullPath, &st)
	}
	if err != nil {
		return nil, err
	}

	fmode := utils.StatModeToFileMode(int(st.Mode))

	return &base.FileInfo{
		Name:  name,
		Path:  fullPath,
		Size:  st.Size,
		Mtime: uint64(st.Mtim.Sec),
		IsDir: fmode.IsDir(),
		Owner: utils.UserName(int(st.Uid)),
		Group: utils.GroupName(int(st.Gid)),
		Mode:  fmode,
		Sys:   st,
	}, nil
}

func (fs *localFileSystem) Access(name string, mode, callerUid, callerGid uint32) error {
	finfo, err := fs.GetAttr(name)
	if err != nil {
		return err

	}
	if attr, ok := finfo.Sys.(syscall.Stat_t); ok {
		if !utils.HasAccess(callerUid, callerGid, attr.Uid, attr.Gid, attr.Mode, mode) {
			return syscall.EACCES
		}
	}
	return nil
}

// Extended attributes.
func (fs *localFileSystem) GetXAttr(name string, attribute string) (data []byte, err error) {
	var dest []byte
	if _, err := syscall.Getxattr(fs.GetPath(name), attribute, dest); err != nil {
		return nil, err
	}
	return dest, nil
}

func (fs *localFileSystem) ListXAttr(name string) (attributes []string, err error) {
	var dest []byte
	if _, err := syscall.Listxattr(fs.GetPath(name), dest); err != nil {
		return nil, err
	}

	for d := range dest {
		attributes = append(attributes, fmt.Sprint(d))
	}

	err = nil
	return
}

func (fs *localFileSystem) RemoveXAttr(name string, attr string) error {
	return syscall.Removexattr(fs.GetPath(name), attr)
}

func (fs *localFileSystem) SetXAttr(name string, attr string, data []byte, flags int) error {
	return syscall.Setxattr(fs.GetPath(name), attr, data, flags)
}

func (fs *localFileSystem) Utimens(name string, Atime *time.Time, Mtime *time.Time) error {
	var tv []syscall.Timeval

	tv = append(tv, syscall.Timeval{
		Sec:  int64(Atime.Second()),
		Usec: Atime.Unix(),
	})

	tv = append(tv, syscall.Timeval{
		Sec:  int64(Mtime.Second()),
		Usec: Mtime.Unix(),
	})
	return syscall.Utimes(fs.GetPath(name), tv)
}

func (f *localFileHandle) Allocate(off uint64, sz uint64, mode uint32) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	return syscall.Fallocate(int(f.File.Fd()), mode, int64(off), int64(sz))
}
