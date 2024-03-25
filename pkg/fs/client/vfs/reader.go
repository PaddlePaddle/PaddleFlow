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
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/cache"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta"
	ufslib "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

const READAHEAD_CHUNK = uint32(20 * 1024 * 1024)
const DeleteBufferLimit = uint64(100 * 1024 * 1024)

const thresholdDuration = 1 * time.Minute

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
	bufferMapLock *sync.RWMutex
	stop          chan struct{}
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
	exceedFirstTime := time.Now()
	if fh.reader.store != nil {
		reader := fh.reader.store.NewReader(fh.path, int(fh.length),
			fh.flags, fh.ufs, fh.buffersCache, fh.reader.bufferPool, fh.seqReadAmount, fh.bufferMapLock)
		for bytesRead < bufSize {
			/*
				n的值会有三种情况
				没有命中缓存的情况下，n的值会返回多个预读区合并在一起的值
				命中缓存的情况下，n会返回blockSize或者length-off大小
				越界的情况，返回0，如off>=length||len(buf)==0
			*/
			nread, err = reader.ReadAt(buf[bytesRead:], int64(off))
			if nread == 0 && err == nil {
				if time.Since(exceedFirstTime) > 5*time.Minute {
					log.Errorf("nread time out is zero len buf %v off %v bytesRead %v size %v path[%s]", len(buf), off, bytesRead, fh.size, fh.path)
					return 0, syscall.EBADF
				}
			} else {
				exceedFirstTime = time.Now()
			}

			if err != nil && err != syscall.ENOMEM && err != io.EOF && err != io.ErrUnexpectedEOF {
				log.Errorf("reader failed: %v", err)
				// 重试的时候稍微等待一下
				time.Sleep(1 * time.Second)
				nread, err = fh.readFromStream(int64(off), buf[bytesRead:])
				if err != nil {
					log.Errorf("read from stream with unexpected error: %v", err)
					return 0, syscall.EBADF
				}
				if nread == 0 {
					log.Infof("readFromStream nread 0 bytesRead %v off %v", bytesRead, off)
					break
				}
			}
			if err == syscall.ENOMEM {
				nread, err = fh.readFromStream(int64(off), buf[bytesRead:])
				if err != nil {
					log.Errorf("read from stream with not ENOMEM failed: %v", err)
					return 0, syscall.EBADF
				}
				if nread == 0 {
					log.Infof("ENOMEM readFromStream nread 0 bytesRead %v off %v", bytesRead, off)
					break
				}
			}
			bytesRead += nread
			fh.seqReadAmount += uint64(nread)
			fh.readBufOffset += uint64(nread)
			if off+uint64(nread) >= fh.length {
				break
			}
			off += uint64(nread)
			if nread == 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
				log.Errorf("read EOF and nread == 0: %v len[%d] off[%d] path[%s] length[%d] ", err, len(buf), off, fh.path, fh.length)
				break
			}
		}
	} else {
		if fh.fd == nil {
			fh.fd, err = fh.ufs.Open(fh.path, syscall.O_RDONLY, fh.length)
			if err != nil {
				log.Errorf("fh ufs open err %v", err)
				return 0, syscall.EBADF
			}
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
		resp, err := fh.ufs.Get(fh.path, fh.flags, off, fh.size-off)
		if err != nil {
			log.Errorf("ufs get err %v", err)
			return 0, err
		}
		fh.streamReader = resp
	}

	bytesRead, err = fh.streamReader.Read(buf)
	if err != nil {
		log.Debugf("stream reader err %v", err)
		if err != io.EOF {
			log.Errorf("readFromStream err %v", err)
			return 0, err
		}
		// always retry
		_ = fh.streamReader.Close()
		fh.streamReader = nil
		err = nil
	}
	log.Debugf("stream result nread[%d]", bytesRead)
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
	close(fh.stop)
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
		reader:        d,
		inode:         inode,
		path:          path,
		length:        length,
		ufs:           ufs,
		buffersCache:  make(cache.ReadBufferMap),
		bufferMapLock: &sync.RWMutex{},
		stop:          make(chan struct{}),
	}
	if d.store == nil {
		fd, err := ufs.Open(path, syscall.O_RDONLY, length)
		if err != nil {
			log.Errorf("data reader path[%s] err[%v]", path, err)
			return nil, err
		}
		f.fd = fd
	}
	d.Lock()
	d.files[inode] = f
	if length > DeleteBufferLimit {
		go func() {
			f.cleanBufferCache(f.stop)
		}()
	}
	d.Unlock()
	return f, nil
}

func (fh *fileReader) cleanBufferCache(stopChan chan struct{}) {
	for {
		select {
		case <-stopChan:
			return
		default:
			processMemPercent := utils.GetProcessMemPercent()
			// 程序超过20%清理buffer，程序内存超过10%的占用触发自动清理且buffer1分钟未用则清理
			if processMemPercent/100 >= 0.2 {
				fh.bufferMapLock.Lock()
				for index, _ := range fh.buffersCache {
					delete(fh.buffersCache, index)
					log.Infof("force delete buffer auto index %v", index)
				}
				fh.bufferMapLock.Unlock()
			} else if processMemPercent/100 > 0.1 {
				fh.bufferMapLock.Lock()
				for index, buffer := range fh.buffersCache {
					now := time.Now()
					if now.Sub(buffer.LastUsedTime) > thresholdDuration {
						log.Infof("delete buffer auto index %v", index)
						delete(fh.buffersCache, index)
					}
				}
				fh.bufferMapLock.Unlock()
			}
			time.Sleep(5 * time.Second)
		}
	}
}
