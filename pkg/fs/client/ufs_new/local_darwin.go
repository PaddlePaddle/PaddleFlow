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

package ufs_new

import (
	"syscall"
	"time"
	"unsafe"

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
		Mtime: uint64(st.Mtimespec.Sec),
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
		if !utils.HasAccess(callerUid, callerGid, attr.Uid, attr.Gid, uint32(attr.Mode), mode) {
			return syscall.EACCES
		}
	}
	return nil
}

// Extended attributes.
func (fs *localFileSystem) GetXAttr(name string, attribute string) (data []byte, err error) {
	var dest []byte
	if _, err := GetXAttr(fs.GetPath(name), attribute, dest); err != syscall.F_OK {
		return nil, syscall.Errno(err)
	}
	return dest, nil
}

func (fs *localFileSystem) ListXAttr(name string) (attributes []string, err error) {
	if attrs, err := ListXAttr(fs.GetPath(name)); err != syscall.F_OK {
		return nil, syscall.Errno(err)
	} else {
		return attrs, nil
	}
}

func (fs *localFileSystem) RemoveXAttr(name string, attr string) error {
	return syscall.Errno(Removexattr(fs.GetPath(name), attr))
}

func (fs *localFileSystem) SetXAttr(name string, attr string, data []byte, flags int) error {
	return syscall.Errno(Setxattr(fs.GetPath(name), attr, data, flags))
}

func (fs *localFileSystem) Utimens(name string, Atime *time.Time, Mtime *time.Time) error {
	var tv []syscall.Timeval

	tv = append(tv, syscall.Timeval{
		Sec:  int64(Atime.Second()),
		Usec: int32(Atime.Unix()),
	})

	tv = append(tv, syscall.Timeval{
		Sec:  int64(Mtime.Second()),
		Usec: int32(Atime.Unix()),
	})
	return syscall.Utimes(fs.GetPath(name), tv)
}

func (f *localFileHandle) Allocate(off uint64, sz uint64, mode uint32) error {
	// TODO: Handle `mode` parameter.

	// From `man fcntl` on OSX:
	//     The F_PREALLOCATE command operates on the following structure:
	//
	//             typedef struct fstore {
	//                 u_int32_t fst_flags;      /* IN: flags word */
	//                 int       fst_posmode;    /* IN: indicates offset field */
	//                 off_t     fst_offset;     /* IN: start of the region */
	//                 off_t     fst_length;     /* IN: size of the region */
	//                 off_t     fst_bytesalloc; /* OUT: number of bytes allocated */
	//             } fstore_t;
	//
	//     The flags (fst_flags) for the F_PREALLOCATE command are as follows:
	//
	//           F_ALLOCATECONTIG   Allocate contiguous space.
	//
	//           F_ALLOCATEALL      Allocate all requested space or no space at all.
	//
	//     The position modes (fst_posmode) for the F_PREALLOCATE command indicate how to use the offset field.  The modes are as fol-
	//     lows:
	//
	//           F_PEOFPOSMODE   Allocate from the physical end of file.
	//
	//           F_VOLPOSMODE    Allocate from the volume offset.

	k := struct {
		Flags      uint32 // u_int32_t
		Posmode    int64  // int
		Offset     int64  // off_t
		Length     int64  // off_t
		Bytesalloc int64  // off_t
	}{
		0,
		0,
		int64(off),
		int64(sz),
		0,
	}

	// Linux version for reference:
	// err := syscall.Fallocate(int(f.File.Fd()), mode, int64(off), int64(sz))

	f.lock.Lock()
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.File.Fd(), uintptr(syscall.F_PREALLOCATE), uintptr(unsafe.Pointer(&k)))
	f.lock.Unlock()
	if errno != 0 {
		return errno
	}
	return nil
}
