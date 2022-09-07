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
	"io"
	"log"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse/nodefs"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

type localFileSystem struct {
	subpath string
}

var _ UnderFileStorage = &localFileSystem{}

// Used for pretty printing.
func (fs *localFileSystem) String() string {
	return common.LocalType
}

func (fs *localFileSystem) GetPath(relPath string) string {
	return filepath.Join(fs.subpath, relPath)
}

// These should update the file's ctime too.
func (fs *localFileSystem) Chmod(name string, mode uint32) error {
	return os.Chmod(fs.GetPath(name), os.FileMode(mode))
}

func (fs *localFileSystem) Chown(name string, uid uint32, gid uint32) error {
	return os.Chown(fs.GetPath(name), int(uid), int(gid))
}

func (fs *localFileSystem) Truncate(name string, size uint64) error {
	return os.Truncate(fs.GetPath(name), int64(size))
}

// Tree structure
func (fs *localFileSystem) Link(oldName string, newName string) error {
	return os.Link(fs.GetPath(oldName), fs.GetPath(newName))
}

func (fs *localFileSystem) Mkdir(name string, mode uint32) error {
	return os.Mkdir(fs.GetPath(name), os.FileMode(mode))
}

func (fs *localFileSystem) Mknod(name string, mode uint32, dev uint32) error {
	return syscall.Mknod(fs.GetPath(name), mode, int(dev))
}

func (fs *localFileSystem) Rename(oldName string, newName string) error {
	oldPath := fs.GetPath(oldName)
	newPath := fs.GetPath(newName)
	return os.Rename(oldPath, newPath)
}

func (fs *localFileSystem) Rmdir(name string) error {
	return syscall.Rmdir(fs.GetPath(name))
}

func (fs *localFileSystem) Unlink(name string) error {
	return syscall.Unlink(fs.GetPath(name))
}

func (fs *localFileSystem) Get(name string, flags uint32, off, limit int64) (io.ReadCloser, error) {
	// filter out append. The kernel layer will translate the
	// offsets for us appropriately.
	flags = flags &^ syscall.O_APPEND
	reader, err := os.OpenFile(fs.GetPath(name), int(flags), 0)
	if err != nil {
		return nil, err
	}
	if off > 0 {
		if _, err := reader.Seek(off, io.SeekStart); err != nil {
			reader.Close()
			return nil, err
		}
	}
	if limit > 0 {
		return withCloser{io.LimitReader(reader, limit), reader}, nil
	}
	return reader, nil
}

func (fs *localFileSystem) Put(name string, reader io.Reader) error {
	return nil
}

// File handling.  If opening for writing, the file's mtime
// should be updated too.
func (fs *localFileSystem) Open(name string, flags uint32) (fd FileHandle, err error) {
	// filter out append. The kernel layer will translate the
	// offsets for us appropriately.
	flags = flags &^ syscall.O_APPEND
	f, err := os.OpenFile(fs.GetPath(name), int(flags), 0)
	if err != nil {
		return nil, err
	}
	return nodefs.NewLoopbackFile(f), nil
}

func (fs *localFileSystem) Create(name string, flags uint32, mode uint32) (fd FileHandle, err error) {
	flags = flags &^ syscall.O_APPEND
	f, err := os.OpenFile(fs.GetPath(name), int(flags)|os.O_CREATE, os.FileMode(mode))
	return nodefs.NewLoopbackFile(f), err
}

// Directory handling
func (fs *localFileSystem) ReadDir(name string) (stream []DirEntry, err error) {
	// What other ways beyond O_RDONLY are there to open
	// directories?
	ofile, err := os.Open(fs.GetPath(name))
	defer ofile.Close()
	if err != nil {
		return nil, err
	}
	want := 500
	output := make([]DirEntry, 0, want)
	for {
		infos, err := ofile.Readdir(want)
		for i := range infos {
			// workaround for https://err.google.com/p/go/issues/detail?id=5960
			if infos[i] == nil {
				continue
			}
			n := infos[i].Name()
			d := DirEntry{
				Name: n,
			}
			attr := sysToAttr(infos[i])
			d.Attr = &attr
			output = append(output, d)
		}
		if len(infos) < want || err == io.EOF {
			break
		}
		if err != nil {
			log.Println("Readdir() returned err:", err)
			break
		}
	}
	return output, nil
}

// Symlinks.
func (fs *localFileSystem) Symlink(value string, linkName string) error {
	return os.Symlink(value, fs.GetPath(linkName))
}

func (fs *localFileSystem) Readlink(name string) (string, error) {
	f, err := os.Readlink(fs.GetPath(name))
	return f, err
}

func (fs *localFileSystem) StatFs(name string) *base.StatfsOut {
	s := syscall.Statfs_t{}
	err := syscall.Statfs(fs.GetPath(name), &s)
	if err == nil {
		out := &base.StatfsOut{}
		out.FromStatfsT(&s)
		return out
	}
	return nil
}

// A FUSE filesystem that shunts all request to an underlying file
// system.  Its main purpose is to provide test coverage without
// having to build a synthetic filesystem.
func NewLocalFileSystem(properties map[string]interface{}) (UnderFileStorage, error) {
	// Make sure the Root path is absolute to avoid problems when the
	// application changes working directory.
	subpath := properties[common.SubPath].(string)
	subpath, err := filepath.Abs(subpath)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(subpath, 0644); err != nil {
		return nil, err
	}

	return &localFileSystem{
		subpath: subpath,
	}, nil
}

func init() {
	RegisterUFS(common.LocalType, NewLocalFileSystem)
}
