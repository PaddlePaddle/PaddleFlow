/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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

package cache_new

import (
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	kv "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv_new"
	ufs "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs_new"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
)

type Reader interface {
	io.ReaderAt
}

type Writer interface {
	io.WriterAt
}

type Store interface {
	NewReader(name string, length int, flags uint32, ufs ufs.UnderFileStorage,
		buffers ReadBufferMap, bufferPool *BufferPool, seqReadAmount uint64) Reader
	NewWriter(name string, length int, ufsFh ufs.FileHandle) Writer
	InvalidateCache(name string, length int) error
}

type ReadCloser interface {
	io.Reader
	io.ReaderAt
	io.Closer
}

type Config struct {
	kv.Config
	BlockSize    int
	MaxReadAhead int
	Expire       time.Duration
}

type store struct {
	conf   Config
	meta   map[string]string
	client DataCacheClient
	sync.RWMutex
}

func NewCacheStore(config Config) Store {
	if config.BlockSize == 0 {
		return nil
	}
	cacheStore := &store{
		conf: config,
		meta: make(map[string]string, 100),
	}
	cacheStore.client = NewDataCache(config)
	log.Debugf("metrics register NewCacheStore")
	registerMetrics()
	return cacheStore
}

func (store *store) NewReader(name string, length int, flags uint32, ufs ufs.UnderFileStorage, buffers ReadBufferMap,
	bufferPool *BufferPool, seqReadAmount uint64) Reader {
	return &rCache{id: path.Clean(name), length: length, store: store, flags: flags, ufs: ufs,
		buffers: buffers, bufferPool: bufferPool, seqReadAmount: seqReadAmount}
}

func (store *store) NewWriter(name string, length int, fh ufs.FileHandle) Writer {
	return nil
}

func (store *store) InvalidateCache(name string, length int) error {
	write := 0
	index := 0
	name = path.Clean(name)
	store.RLock()
	keyID, ok := store.meta[name]
	store.RUnlock()
	if !ok {
		return nil
	}
	store.Lock()
	delete(store.meta, name)
	store.Unlock()
	go func() {
		for write <= length {
			key := store.key(keyID, index)
			log.Debugf("cache del key is %s and keyID %s", key, keyID)
			if store.client != nil {
				store.client.delete(key)
			}
			write += store.conf.BlockSize
			index += 1
		}
	}()
	return nil
}

func (store *store) key(keyID string, index int) string {
	hash := utils.KeyHash(keyID)
	return path.Clean(fmt.Sprintf("blocks/%d/%v_%v", hash%256, keyID, index))
}
