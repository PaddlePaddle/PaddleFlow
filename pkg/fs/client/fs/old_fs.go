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

package fs

import (
	"os"
	"runtime"
	"syscall"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/meta"
	"paddleflow/pkg/fs/client/utils"
	"paddleflow/pkg/fs/client/vfs"
)

type OldFileSystem struct {
	fsMeta base.FSMeta
	vfs    *vfs.OldVFS
	stop   chan struct{}
}

func NewOldFileSystem(fsMeta base.FSMeta, links map[string]base.FSMeta, skipSub bool,
	linkMetaDirPrefix string) (*OldFileSystem, error) {
	fs := OldFileSystem{fsMeta: fsMeta, stop: make(chan struct{})}
	vfs, err := vfs.InitOldVFS(fsMeta, links, false)
	if err != nil {
		log.Errorf("init vfs failed: %v", err)
		return nil, err
	}
	fs.vfs = vfs
	// 释放fs的时候会结束协程
	runtime.SetFinalizer(&fs, func(fs *OldFileSystem) {
		close(fs.stop)
	})
	if !skipSub {
		go vfs.LinksMetaUpdateHandler(fs.stop, meta.DefaultLinkUpdateInterval, linkMetaDirPrefix)
	}
	return &fs, nil
}

func (fs *OldFileSystem) Open(path string) (*OldFile, error) {
	ctx := base.NewDefaultContext()
	// 参考的os.path， 默认flag=O_RDONLY
	flags := uint32(syscall.O_RDONLY)
	fileHandle, err := fs.vfs.Open(path, flags, ctx)
	if utils.IsError(err) {
		log.Errorf("open file[%s] failed: %v", path, err)
		return nil, err
	}

	attr, err := fs.vfs.GetAttr(path, ctx)
	if utils.IsError(err) {
		log.Errorf("open file[%s] failed: %v", path, err)
		return nil, err
	}
	file := NewOldPFILE(fileHandle, NewFileInfo(attr), fs.vfs)
	return &file, nil
}

func (fs *OldFileSystem) Chmod(name string, mode os.FileMode) error {
	ctx := base.NewDefaultContext()
	err := fs.vfs.Chmod(name, uint32(mode), ctx)
	if utils.IsError(err) {
		log.Errorf("chmod file[%s] with mode [%d] failed: %v", name, mode, err)
		return err
	}
	return nil
}

func (fs *OldFileSystem) Chown(name string, uid, gid int) error {
	ctx := base.NewDefaultContext()
	err := fs.vfs.Chown(name, uint32(uid), uint32(gid), ctx)
	if utils.IsError(err) {
		log.Errorf("chown file[%s] with uid[%d] and gid[%d] failed: %v",
			name, uid, gid, err)
		return err
	}
	return nil
}

func (fs *OldFileSystem) Stat(path string) (os.FileInfo, error) {
	ctx := base.NewDefaultContext()
	attr, err := fs.vfs.GetAttr(path, ctx)
	if utils.IsError(err) {
		log.Errorf("[fs] get attr for path[%s] failed: [%s]", path, err)
		return nil, err
	}
	fileInfo := NewFileInfo(attr)
	return &fileInfo, nil
}

func (fs *OldFileSystem) Mkdir(name string, mode os.FileMode) error {
	ctx := base.NewDefaultContext()
	err := fs.vfs.Mkdir(name, uint32(mode), ctx)
	if utils.IsError(err) {
		log.Errorf("mkdir path[%s] with mode[%d] failed: %v", name, mode, err)
		return err
	}
	return nil
}

func (fs *OldFileSystem) Rmdir(path string) error {
	ctx := base.NewDefaultContext()
	err := fs.vfs.Rmdir(path, ctx)
	if utils.IsError(err) {
		log.Errorf("rmdir path[%s] failed: %v", path, err)
		return err
	}
	return nil
}

func (fs *OldFileSystem) Rename(oldPath string, newPath string) error {
	ctx := base.NewDefaultContext()
	err := fs.vfs.Rename(oldPath, newPath, ctx)
	if utils.IsError(err) {
		log.Errorf("Rename: oldName[%s], newName[%s], failed: [%v]",
			oldPath, newPath, err)
		return err
	}
	return nil
}

func (fs *OldFileSystem) Unlink(path string) error {
	ctx := base.NewDefaultContext()
	err := fs.vfs.Unlink(path, ctx)
	if utils.IsError(err) {
		log.Errorf("delete path[%s] failed: %v", path, err)
		return err
	}
	return nil
}

func (fs *OldFileSystem) Create(path string, flags uint32, mode uint32) (*OldFile, error) {
	ctx := base.NewDefaultContext()
	fileHandle, err := fs.vfs.Create(path, flags, mode, ctx)
	if utils.IsError(err) {
		log.Errorf("create path[%s] failed: %v", path, err)
		return nil, err
	}

	attr := NewFileInfoForCreate(path, os.FileMode(mode))
	file := NewOldPFILE(fileHandle, attr, fs.vfs)
	return &file, nil
}
