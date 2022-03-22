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
	fd            base.FileHandle
	buffersCache  cache.ReadBufferMap
	seqReadAmount uint64
}

type dataReader struct {
	sync.Mutex
	m         meta.Meta
	files     map[Ino]*fileReader
	ufsMap    *ufsMap
	store     cache.Store
	blockSize int
}

func (fh *fileReader) read(buf []byte, off uint64) (int, syscall.Errno) {
	var bytesRead int
	bufSize := len(buf)
	var nread int
	var err error
	reader := fh.reader.store.NewReader(fh.path, int(fh.length), fh.flags, fh.ufs, fh.buffersCache, fh.seqReadAmount)
	for bytesRead < bufSize {
		/*
			n的值会有三种情况
			没有命中缓存的情况下，n的值会返回多个预读区合并在一起的值
			命中缓存的情况下，n会返回blockSize或者length-off大小
			越界的情况，返回0，如off>=length||len(buf)==0
		*/
		nread, err = reader.ReadAt(buf[bytesRead:], int64(off))
		if err != nil {
			log.Errorf("reader readat failed: %v", err)
			return 0, syscall.EBADF
		}
		if nread == 0 {
			break
		}
		bytesRead += nread
		fh.seqReadAmount += uint64(nread)
		off += uint64(nread)
	}
	return bytesRead, syscall.F_OK
}

func (fh *fileReader) Read(buf []byte, off uint64) (int, syscall.Errno) {
	fh.Lock()
	defer fh.Unlock()
	log.Debugf("len[%d] off[%d] blockName[%s] length[%d]", len(buf), off, fh.name, fh.length)
	if off >= fh.length || len(buf) == 0 {
		return 0, syscall.F_OK
	}
	var bytesRead int
	var err error
	if fh.reader.store != nil {
		bytesRead, err = fh.read(buf, off)
	} else {
		if fh.fd == nil {
			log.Debug("fd is empty")
			return 0, syscall.EBADF
		}
		// todo:: 不走缓存部分需要保持原来open-read模式，保证这部分性能
		ufsHandle := ufslib.NewFileHandle(fh.fd)
		bytesRead, err = ufsHandle.ReadAt(buf, int64(off))
		if err != nil {
			log.Errorf("ufs read err: %v", err)
			return 0, syscall.EBADF
		}
	}
	return bytesRead, syscall.F_OK
}

func (fh *fileReader) Close() {
	fh.Lock()
	fh.release()
	fh.Unlock()
}

func (fh *fileReader) release() {
	// todo:: 硬链接的情况下，需要增加refer判断，不能直接删除
	fh.reader.Lock()
	delete(fh.reader.files, fh.inode)
	for _, buffer := range fh.buffersCache {
		buffer.Buffer.Close()
	}
	fh.reader.Unlock()
	if fh.fd != nil {
		fh.fd.Release()
	}
}

func (d *dataReader) Open(inode Ino, length uint64, ufs ufslib.UnderFileStorage, path string) (FileReader, error) {
	name := d.m.InoToPath(inode)
	f := &fileReader{
		reader:       d,
		inode:        inode,
		name:         name,
		path:         path,
		length:       length,
		ufs:          ufs,
		buffersCache: make(cache.ReadBufferMap),
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
