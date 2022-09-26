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
	"io"
	"sync"
	"syscall"

	log "github.com/sirupsen/logrus"

	cache "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/cache_new"
	meta "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta_new"
	ufslib "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs_new"
)

const READAHEAD_CHUNK = uint32(20 * 1024 * 1024)

type FileReader interface {
	Read(buf []byte, off uint64) (int, syscall.Errno)
	Close()
}

type DataReader interface {
	Open(inode Ino, length uint64, ufs ufslib.UnderFileStorage, path string) (FileReader, error)
}

func NewDataReader(m meta.Meta, blockSize int, store cache.Store) DataReader {
	bufferPool := cache.BufferPool{}
	r := &dataReader{
		m:          m,
		files:      make(map[Ino]*fileReader),
		store:      store,
		blockSize:  blockSize,
		bufferPool: bufferPool.Init(blockSize),
	}
	return r
}

type fileReader struct {
	inode  Ino
	size   int64
	flags  uint32
	path   string
	length uint64
	ufs    ufslib.UnderFileStorage
	reader *dataReader
	sync.Mutex
	// TODO: 先用base.FileHandle跑通流程，后续修改ufs接口
	fd            ufslib.FileHandle
	buffersCache  cache.ReadBufferMap
	streamReader  io.ReadCloser
	seqReadAmount uint64
	readBufOffset uint64
}

type dataReader struct {
	sync.Mutex
	m          meta.Meta
	files      map[Ino]*fileReader
	store      cache.Store
	bufferPool *cache.BufferPool
	blockSize  int
}

func (fh *fileReader) Read(buf []byte, off uint64) (int, syscall.Errno) {
	fh.Lock()
	defer fh.Unlock()
	log.Debugf("fileReader len[%d] off[%d] path[%s] length[%d]", len(buf), off, fh.path, fh.length)
	if off >= fh.length || len(buf) == 0 {
		return 0, syscall.F_OK
	}
	var bytesRead int
	var err error
	var nread int
	bufSize := len(buf)
	if fh.reader.store != nil {
		reader := fh.reader.store.NewReader(fh.path, int(fh.length),
			fh.flags, fh.ufs, fh.buffersCache, fh.reader.bufferPool, fh.seqReadAmount)
		for bytesRead < bufSize {
			/*
				n的值会有三种情况
				没有命中缓存的情况下，n的值会返回多个预读区合并在一起的值
				命中缓存的情况下，n会返回blockSize或者length-off大小
				越界的情况，返回0，如off>=length||len(buf)==0
			*/
			nread, err = reader.ReadAt(buf[bytesRead:], int64(off))
			if err != nil && err != syscall.ENOMEM {
				log.Errorf("reader readat failed: %v", err)
				return 0, syscall.EBADF
			}
			if err == syscall.ENOMEM {
				nread, err = fh.readFromStream(int64(off), buf[bytesRead:])
			}
			if err != nil {
				log.Errorf("read from stream failed: %v", err)
				return 0, syscall.EBADF
			}
			bytesRead += nread
			fh.seqReadAmount += uint64(nread)
			if off+uint64(nread) >= fh.length {
				break
			}
			off += uint64(nread)
		}
	} else {
		if fh.fd == nil {
			log.Debug("fd is empty")
			return 0, syscall.EBADF
		}
		// todo:: 不走缓存部分需要保持原来open-read模式，保证这部分性能
		bytesRead, err = fh.fd.Read(buf, off)
		if err != nil {
			log.Errorf("ufs read err: %v", err)
			return 0, syscall.EBADF
		}
	}
	return bytesRead, syscall.F_OK
}

func (fh *fileReader) readFromStream(off int64, buf []byte) (bytesRead int, err error) {
	log.Debugf("read from stream %v readBufOffset[%d], len[%d]", off, fh.readBufOffset, len(buf))
	if fh.readBufOffset != uint64(off) {
		fh.readBufOffset = uint64(off)
		if fh.streamReader != nil {
			_ = fh.streamReader.Close()
			fh.streamReader = nil
		}
	}
	if fh.streamReader == nil {
		log.Debugf("init reader %s flags[%d] off[%d]", fh.path, fh.flags, off)
		resp, err := fh.ufs.Get(fh.path, fh.flags, off, 0)
		if err != nil {
			return 0, err
		}
		fh.streamReader = resp
	}

	bytesRead, err = fh.streamReader.Read(buf)
	fh.readBufOffset += uint64(bytesRead)
	if err != nil {
		log.Debugf("stream reader err %v", err)
		if err != io.EOF {
			log.Errorf("readFromStream err %v", err)
		}
		// always retry
		_ = fh.streamReader.Close()
		fh.streamReader = nil
		err = nil
	}
	log.Debugf("stream result nread[%d] and buf[%s]", bytesRead, string(buf[:bytesRead]))
	return
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
	if fh.streamReader != nil {
		_ = fh.streamReader.Close()
		fh.streamReader = nil
	}
}

func (d *dataReader) Open(inode Ino, length uint64, ufs ufslib.UnderFileStorage, path string) (FileReader, error) {
	f := &fileReader{
		reader:       d,
		inode:        inode,
		path:         path,
		length:       length,
		ufs:          ufs,
		buffersCache: make(cache.ReadBufferMap),
	}
	if d.store == nil {
		fd, err := ufs.Open(path, syscall.O_RDONLY, length)
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
