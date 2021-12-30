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
)

type FileReader interface {
	Read(buf []byte, off uint64) (int, syscall.Errno)
	Close()
}

type DataReader interface {
	Open(inode Ino, length uint64, ufs ufslib.UnderFileStorage, path string) (FileReader, error)
}

func NewDataReader(m meta.Meta, blockSize int, store cache.Store) DataReader {
	r := &dataReader{
		m:         m,
		files:     make(map[Ino]*fileReader),
		store:     store,
		blockSize: blockSize,
	}
	return r
}

type fileReader struct {
	inode  Ino
	size   int64
	flags  uint32
	name   string
	path   string
	length uint64
	ufs    ufslib.UnderFileStorage
	reader *dataReader
	sync.Mutex

	// TODO: 先用base.FileHandle跑通流程，后续修改ufs接口
	fd base.FileHandle
}

type dataReader struct {
	sync.Mutex
	m         meta.Meta
	files     map[Ino]*fileReader
	ufsMap    *ufsMap
	store     cache.Store
	blockSize int
}

func (f *fileReader) Read(buf []byte, off uint64) (int, syscall.Errno) {
	ufsHandle := ufslib.NewFileHandle(f.fd)
	var n int
	var err error
	if f.reader.store != nil {
		log.Debugf("len[%d] off[%d] blockName[%s]", len(buf), off, f.name)
		n, err = f.reader.Read(f.name, buf, int(off), ufsHandle)
		if err != nil {
			log.Errorf("fileReader read err: %v", err)
			return 0, syscall.EBADF
		}
	} else {
		n, err = ufsHandle.ReadAt(buf, int64(off))
		if err != nil {
			log.Errorf("ufs read err: %v", err)
			return 0, syscall.EBADF
		}
	}

	return n, syscall.F_OK
}

func (f *fileReader) Close() {
	f.Lock()
	f.release()
	f.Unlock()
}

func (f *fileReader) release() {
	// todo:: 硬链接的情况下，需要增加refer判断，不能直接删除
	delete(f.reader.files, f.inode)
	f.fd.Release()
}

func (d *dataReader) Open(inode Ino, length uint64, ufs ufslib.UnderFileStorage, path string) (FileReader, error) {
	name := d.m.InoToPath(inode)
	fd, err := ufs.Open(path, syscall.O_RDONLY)
	if err != nil {
		return nil, err
	}
	f := &fileReader{
		reader: d,
		inode:  inode,
		name:   name,
		path:   path,
		length: length,
		ufs:    ufs,
		fd:     fd,
	}
	d.Lock()
	d.files[inode] = f
	d.Unlock()
	return f, nil
}

func (d *dataReader) Read(name string, buf []byte, off int, ufs ufslib.FileHandle) (int, error) {
	read := 0
	bufSize := len(buf)
	reader := d.store.NewReader(name, ufs)
	var err error
	var n int
	for read < len(buf) {
		// 保证从cache读只需要读其中一块cache
		cacheSize := d.blockSize - off%d.blockSize
		end := read + cacheSize
		if end > bufSize {
			end = bufSize
		}
		n, err = reader.ReadAt(buf[read:end], int64(off))
		if err != nil {
			log.Errorf("reader readat failed: %v", err)
			return 0, err
		}
		if n == 0 {
			break
		}
		read += n
		off += n
	}
	return read, nil
}
