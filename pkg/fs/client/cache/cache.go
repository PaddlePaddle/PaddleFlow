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

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/fs/client/ufs"
	"paddleflow/pkg/fs/client/utils"
)

const (
	readAheadNum = 2
)

type store struct {
	mem  *memCache
	disk *diskCache
	conf Config
	sync.RWMutex
	meta map[string]string
}

type Config struct {
	Mem       *MemConfig
	Disk      *DiskConfig
	BlockSize int
}

type rCache struct {
	id    string
	store *store
	ufsFh ufs.FileHandle
}

func NewCacheStore(config *Config) Store {
	if (config.Mem == nil && config.Disk == nil) || config.BlockSize == 0 {
		return nil
	}
	cacheStore := &store{
		conf: *config,
		meta: make(map[string]string, 100),
	}
	if config.Mem != nil {
		cacheStore.mem = NewMemCache(config.Mem)
	}
	if config.Disk != nil {
		cacheStore.disk = NewDiskCache(config.Disk)
	}

	return cacheStore
}

func (store *store) NewReader(name string, fh ufs.FileHandle) Reader {
	return &rCache{id: path.Clean(name), store: store, ufsFh: fh}
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
			if store.mem != nil {
				store.mem.delete(key)
			}
			if store.disk != nil {
				store.disk.delete(key)
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

func (r *rCache) ReadAt(buf []byte, off int64) (int, error) {
	log.Debugf("rCache read len byte %d off %d", len(buf), off)
	if len(buf) == 0 {
		return 0, nil
	}
	var index int
	var key string

	index = r.index(int(off))
	key = r.key(index)
	blockOff := r.off(int(off))
	blockSize := r.store.conf.BlockSize
	bufSize := len(buf)

	n, ok := r.readCache(buf, key, blockOff)
	if ok {
		// 最后一个block大小未填满
		return n, nil
	}

	// todo:: readAheadNum改成可配的
	ufsBuf := make([]byte, readAheadNum*blockSize)
	n, err := r.ufsFh.ReadAt(ufsBuf, int64(index*blockSize))
	if err != nil {
		return 0, err
	}

	go r.setCache(int(off), ufsBuf, n)

	/**
	1. 当buf小于等于blockSize，会填充前len(buf)大小的block数据，多余部分舍弃
	2. buf的大小不会大于blockSize，上层已经处理
	3. ufsBUf对s3类型的操作已经做了填充处理
	*/
	copy(buf, ufsBuf[blockOff:bufSize+blockOff])

	return utils.Min(bufSize, n-blockOff), nil
}

func (r *rCache) index(off int) int {
	return off / r.store.conf.BlockSize
}

func (r *rCache) off(off int) int {
	return off % r.store.conf.BlockSize
}

func (r *rCache) key(index int) string {
	r.store.RLock()
	keyID := r.store.meta[r.id]
	r.store.RUnlock()
	if keyID == "" {
		r.store.Lock()
		r.store.meta[r.id] = uuid.NewString()
		r.store.Unlock()
	}
	hash := utils.KeyHash(keyID)
	return path.Clean(fmt.Sprintf("blocks/%d/%v_%v", hash%256, keyID, index))
}

func (r *rCache) readCache(buf []byte, key string, off int) (int, bool) {
	// memory cache
	log.Debugf("read cache key is %s", key)
	if r.store.mem != nil {
		mem, ok := r.store.mem.load(key)
		if ok {
			n, err := mem.ReadAt(buf, int64(off))
			if err != nil && err != io.EOF {
				log.Debugf("mem readAt err %v", err)
				return 0, false
			}
			return n, true
		}
	}

	// disk cache
	if r.store.disk != nil {
		file, ok := r.store.disk.load(key)
		if ok {
			n, err := file.ReadAt(buf, int64(off))
			file.Close()
			if err != nil && err != io.EOF {
				log.Debugf("disk readAt err %v", err)
				return 0, false
			}
			return n, true
		}
	}
	return 0, false
}

func (r *rCache) setCache(off int, p []byte, n int) {
	blockNum := 1
	if n > 0 {
		blockNum = (n-1)/r.store.conf.BlockSize + 1
	}

	var index int
	var key string

	left := 0
	index = r.index(off)

	for i := 0; i < int(blockNum); i++ {
		key = r.key(index + i)
		log.Debugf("cache set key is %s name %s", key, r.id)
		right := left + r.store.conf.BlockSize
		if right > n {
			right = n
		}
		if r.store.mem != nil && r.store.conf.Mem.CacheSize > 0 {
			r.store.mem.save(key, p[left:right])
		}
		if r.store.disk != nil {
			r.store.disk.save(key, p[left:right])
		}
		left += right - left
	}
}
