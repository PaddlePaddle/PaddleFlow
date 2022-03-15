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
	f.Lock()
	defer f.Unlock()
	log.Debugf("len[%d] off[%d] blockName[%s] length[%d]", len(buf), off, f.name, f.length)
	if off >= f.length || len(buf) == 0 {
		return 0, syscall.F_OK
	}
	var n int
	var err error
	if f.reader.store != nil {
		n, err = f.reader.Read(f.path, buf, int(off), f.length, f.flags, f.ufs)
		if err != nil {
			log.Errorf("fileReader read err: %v", err)
			return 0, syscall.EBADF
		}
	} else {
		if f.fd == nil {
			log.Debug("fd is empty")
			return 0, syscall.EBADF
		}
		// todo:: 不走缓存部分需要保持原来open-read模式，保证这部分性能
		ufsHandle := ufslib.NewFileHandle(f.fd)
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
	f.reader.Lock()
	delete(f.reader.files, f.inode)
	f.reader.Unlock()
	if f.fd != nil {
		f.fd.Release()
	}
}

func (d *dataReader) Open(inode Ino, length uint64, ufs ufslib.UnderFileStorage, path string) (FileReader, error) {
	name := d.m.InoToPath(inode)
	f := &fileReader{
		reader: d,
		inode:  inode,
		name:   name,
		path:   path,
		length: length,
		ufs:    ufs,
	}
	if d.store == nil {
		fd, err := ufs.Open(path, syscall.O_RDONLY)
		if err != nil {
			return nil, err
		}
		f.fd = fd
	}
	d.Lock()
	d.files[inode] = f
	d.Unlock()
	return f, nil
}

func (d *dataReader) Read(name string, buf []byte, off int, length uint64, flags uint32, ufs ufslib.UnderFileStorage) (int, error) {
	read := 0
	bufSize := len(buf)
	reader := d.store.NewReader(name, int(length), flags, ufs)
	var err error
	var n int
	for read < bufSize {
		/*
			n的值会有三种情况
			没有命中缓存的情况下，n的值会返回多个预读区合并在一起的值
			命中缓存的情况下，n会返回blockSize或者length-off大小
			越界的情况，返回0，如off>=length||len(buf)==0
		*/
		// todo:: readerAt返回的都是从off开始，读取bufSize或者到文件结束的内容，返回的n为读取的大小，目前底层cache只返回了部分内容
		n, err = reader.ReadAt(buf[read:], int64(off))
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
