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

package cache

import (
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/fs/client/kv"
	"paddleflow/pkg/fs/client/ufs"
	"paddleflow/pkg/fs/client/utils"
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

type store struct {
	conf   Config
	meta   map[string]string
	client kv.Client
	sync.RWMutex
}

type Config struct {
	kv.Config
	BlockSize    int
	MaxReadAhead int
	Expire       time.Duration
}

func NewCacheStore(fsID string, config *Config) (Store, error) {
	if config.BlockSize == 0 {
		return nil, nil
	}
	kvConf := kv.Config{
		FsID:      fsID,
		Driver:    config.Driver,
		CachePath: config.CachePath,
	}
	dataCacheClient, err := kv.NewClient(kvConf)
	if err != nil {
		log.Errorf("NewDataCache fs[%s] err:%v", fsID, err)
		return nil, err
	}
	cacheStore := &store{
		conf:   *config,
		meta:   make(map[string]string, 100),
		client: dataCacheClient,
	}
	log.Debugf("metrics register NewCacheStore")
	registerMetrics()
	return cacheStore, nil
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
	// delete data cache
	if store.client != nil {
		go func() {
			for write <= length {
				key := store.key(keyID, index)
				log.Debugf("cache del key is %s and keyID %s", key, keyID)
				if err := store.client.Dels([]byte(key)); err != nil {
					log.Errorf("InvalidateCache client.Dels keyID[%s] index[%d], err:%v", keyID, index, err)
					break
				}
				write += store.conf.BlockSize
				index += 1
			}
		}()
	}
	return nil
}

func (store *store) key(keyID string, index int) string {
	hash := utils.KeyHash(keyID)
	return path.Clean(fmt.Sprintf("blocks/%d/%v_%v", hash%256, keyID, index))
}
