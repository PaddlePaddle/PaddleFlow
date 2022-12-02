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

package vfs

import (
	"sync"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/cache"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta"
	ufslib "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
)

type FileWriter interface {
	Write(data []byte, offset uint64) syscall.Errno
	Flush() syscall.Errno
	Fsync(fd int) syscall.Errno
	Close()
	Truncate(size uint64) syscall.Errno
	Fallocate(size int64, off int64, mode uint32) syscall.Errno
	// GetSize() uint64
}

type DataWriter interface {
	Open(inode Ino, length uint64, ufs ufslib.UnderFileStorage, path string) (FileWriter, error)
	// Flush(path string) syscall.Errno
	// GetLength(path string) uint64
	// Truncate(path string, length uint64)
}

func NewDataWriter(m meta.Meta, blockSize int, store cache.Store) DataWriter {
	w := &dataWriter{
		m:         m,
		files:     make(map[Ino]*fileWriter),
		store:     store,
		blockSize: blockSize,
	}
	return w
}

type fileWriter struct {
	sync.Mutex
	writer *dataWriter
	inode  Ino
	path   string
	length uint64
	ufs    ufslib.UnderFileStorage

	// TODO: 先用base.FileHandle跑通流程，后续修改ufs接口
	fd ufslib.FileHandle
}

func (f *fileWriter) Fallocate(size int64, off int64, mode uint32) syscall.Errno {
	f.Lock()
	defer f.Unlock()
	return utils.ToSyscallErrno(f.fd.Allocate(uint64(off), uint64(size), mode))
}

func (f *fileWriter) Write(data []byte, offset uint64) syscall.Errno {
	f.Lock()
	defer f.Unlock()
	var err error
	if f.writer.store != nil {
		// todo:: length可能会有遗漏
		log.Debugf("fileWriter write: InvalidateCache path[%s] cache length[%d]", f.path, f.length)
		err = f.writer.store.InvalidateCache(f.path, int(f.length))
		if err != nil {
			log.Errorf("ufs delete cache err: %v", err)
			return syscall.EBADF
		}
	}
	_, err = f.fd.Write(data, offset)
	if err != nil {
		log.Errorf("ufs write err: %v", err)
		return syscall.EBADF
	}
	return syscall.F_OK
}

func (f *fileWriter) Flush() syscall.Errno {
	f.Lock()
	defer f.Unlock()
	if f.writer.store != nil {
		log.Debugf("flush: delete cache is %s", f.path)
		delErr := f.writer.store.InvalidateCache(f.path, int(f.length))
		if delErr != nil {
			log.Errorf("del cache error: %v", delErr)
			return syscall.EBADF
		}
	}
	// todo:: 需要加一个超时和重试
	return utils.ToSyscallErrno(f.fd.Flush())
}

func (f *fileWriter) Fsync(fd int) syscall.Errno {
	f.Lock()
	defer f.Unlock()
	// todo:: 需要加一个超时和重试
	return utils.ToSyscallErrno(f.fd.Fsync(fd))
}

func (f *fileWriter) Close() {
	f.release()
}

func (f *fileWriter) release() {
	delete(f.writer.files, f.inode)
	f.fd.Release()
}

func (f *fileWriter) Truncate(size uint64) syscall.Errno {
	return utils.ToSyscallErrno(f.fd.Truncate(size))
}

type dataWriter struct {
	sync.Mutex
	m         meta.Meta
	ufsMap    *ufsMap
	files     map[Ino]*fileWriter
	store     cache.Store
	blockSize int
}

func (w *dataWriter) Open(inode Ino, length uint64, ufs ufslib.UnderFileStorage, path string) (FileWriter, error) {
	fd, err := ufs.Open(path, syscall.O_WRONLY, length)
	if err != nil {
		return nil, err
	}
	f := &fileWriter{
		writer: w,
		inode:  inode,
		path:   path,
		length: length,
		ufs:    ufs,
		fd:     fd,
	}
	w.Lock()
	w.files[inode] = f
	w.Unlock()
	return f, nil
}
