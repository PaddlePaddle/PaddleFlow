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

	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/cache"
	"paddleflow/pkg/fs/client/meta"
	ufslib "paddleflow/pkg/fs/client/ufs"
	"paddleflow/pkg/fs/client/utils"
)

type FileWriter interface {
	Write(data []byte, offset uint64) syscall.Errno
	Flush() syscall.Errno
	Fsync(fd int) syscall.Errno
	Close() syscall.Errno
	Truncate(size uint64) syscall.Errno
	// GetSize() uint64
}

type DataWriter interface {
	Open(inode Ino, length uint64, ufs ufslib.UnderFileStorage, path string) (FileWriter, error)
	// Flush(name string) syscall.Errno
	// GetLength(name string) uint64
	// Truncate(name string, length uint64)
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
	name   string
	path   string
	length uint64
	ufs    ufslib.UnderFileStorage

	// TODO: 先用base.FileHandle跑通流程，后续修改ufs接口
	fd base.FileHandle
}

func (f *fileWriter) Write(data []byte, offset uint64) syscall.Errno {
	ufsHandle := ufslib.NewFileHandle(f.fd)
	var err error
	if f.writer.store != nil {
		// todo:: length可能会有遗漏
		log.Debugf("write is here name[%s] lenght[%d]", f.name, f.length)
		err = f.writer.store.InvalidateCache(f.name, int(f.length))
		if err != nil {
			log.Errorf("ufs delete cache err: %v", err)
			return syscall.EBADF
		}
	}
	_, err = ufsHandle.WriteAt(data, int64(offset))
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
		log.Debugf("flush: delete cache is %s", f.name)
		delErr := f.writer.store.InvalidateCache(f.name, int(f.length))
		if delErr != nil {
			log.Errorf("del cache error: %v", delErr)
			return syscall.EBADF
		}
	}
	// todo:: 需要加一个超时和重试
	err := f.fd.Flush()
	return syscall.Errno(err)
}

func (f *fileWriter) Fsync(fd int) syscall.Errno {
	f.Lock()
	defer f.Unlock()
	// todo:: 需要加一个超时和重试
	err := f.fd.Fsync(fd)
	return syscall.Errno(err)
}

func (f *fileWriter) Close() syscall.Errno {
	err := f.Flush()
	if utils.IsError(err) {
		return err
	}
	f.release()
	return 0
}

func (f *fileWriter) release() {
	delete(f.writer.files, f.inode)
	f.fd.Release()
}

func (f *fileWriter) Truncate(size uint64) syscall.Errno {
	return syscall.Errno(f.fd.Truncate(size))
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
	name := w.m.InoToPath(inode)
	fd, err := ufs.Open(path, syscall.O_WRONLY)
	if err != nil {
		return nil, err
	}
	f := &fileWriter{
		writer: w,
		inode:  inode,
		name:   name,
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
