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

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/fs/client/ufs"
	"paddleflow/pkg/fs/client/utils"
	"paddleflow/pkg/metric"
)

const (
	maxReadAheadSize = 200 * 1024 * 1024
)

var (
	cacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_hits",
		Help: "read from cached block",
	})
	cacheMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_miss",
		Help: "missed read from cached block",
	})
	cacheMissRate = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "blockcache_hit_rate",
		Help: "cache hit rate out of all read",
	}, func() float64 {
		hitCnt := metric.GetMetricValue(cacheHits)
		missCnt := metric.GetMetricValue(cacheMiss)
		return hitCnt / (hitCnt + missCnt)
	})
	cacheWrites = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_writes",
		Help: "written cached block",
	})
	cacheDrops = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_drops",
		Help: "dropped block",
	})
	cacheEvicts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts",
		Help: "evicted cache blocks",
	})
	cacheHitBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_hit_bytes",
		Help: "read bytes from cached block",
	})
	cacheMissBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_miss_bytes",
		Help: "missed bytes from cached block",
	})
	cacheWriteBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_write_bytes",
		Help: "write bytes of cached block",
	})
	cacheReadHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "blockcache_read_hist_seconds",
		Help:    "read cached block latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.00001, 2, 20),
	})
	cacheWriteHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "blockcache_write_hist_seconds",
		Help:    "write cached block latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.00001, 2, 20),
	})

	objectReqsHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "object_request_durations_histogram_seconds",
		Help:    "Object requests latency distributions.",
		Buckets: prometheus.ExponentialBuckets(0.01, 1.5, 25),
	}, []string{"method"})
	objectReqErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "object_request_errors",
		Help: "failed requests to object store",
	})
	objectDataBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "object_request_data_bytes",
		Help: "Object requests size in bytes.",
	}, []string{"method"})

	stageBlocks = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "staging_blocks",
		Help: "Number of blocks in the staging path.",
	})
	stageBlockBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "staging_block_bytes",
		Help: "Total bytes of blocks in the staging path.",
	})
)

type store struct {
	mem  *memCache
	disk *diskCache
	conf Config
	sync.RWMutex
	meta map[string]string
}

type Config struct {
	Mem          *MemConfig
	Disk         *DiskConfig
	BlockSize    int
	MaxReadAhead int
}

type rCache struct {
	id         string
	flags      uint32
	length     int
	store      *store
	ufs        ufs.UnderFileStorage
	buffers    ReadBufferMap
	lock       sync.RWMutex
	readAMount uint64
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
	log.Debugf("metrics register NewCacheStore")
	registerMetrics()
	return cacheStore
}

func registerMetrics() {
	prometheus.Register(cacheHits)
	prometheus.Register(cacheHitBytes)
	prometheus.Register(cacheMiss)
	prometheus.Register(cacheMissBytes)
	prometheus.Register(cacheMissRate)
	prometheus.Register(cacheWrites)
	prometheus.Register(cacheWriteBytes)
	prometheus.Register(cacheDrops)
	prometheus.Register(cacheEvicts)
	prometheus.Register(cacheReadHist)
	prometheus.Register(cacheWriteHist)
}

func (store *store) NewReader(name string, length int, flags uint32, ufs ufs.UnderFileStorage, buffers ReadBufferMap, readAMount uint64) Reader {
	return &rCache{id: path.Clean(name), length: length, store: store, flags: flags, ufs: ufs, buffers: buffers, readAMount: readAMount}
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

func (r *rCache) readFromReadAhead(off int64, buf []byte) (bytesRead int, err error) {
	blockOff := r.off(int(off))
	index := r.index(int(off))
	blockSize := r.store.conf.BlockSize

	var nread int
	var indexOff uint64
	for bytesRead < len(buf) {
		indexOff = uint64(index * blockSize)
		if int(indexOff) >= r.length {
			return
		}
		r.lock.RLock()
		readAheadBuf, ok := r.buffers[indexOff]
		r.lock.RUnlock()
		if !ok {
			return
		}
		if blockOff != 0 {
			nread, err = readAheadBuf.Read(uint64(blockOff), buf[bytesRead:])
			blockOff = 0
		} else {
			nread, err = readAheadBuf.Read(uint64(blockOff), buf[bytesRead:])
		}
		if readAheadBuf.size == 0 {
			r.lock.Lock()
			delete(r.buffers, indexOff)
			r.lock.Unlock()
		}
		bytesRead += nread
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			break
		}
		if nread == 0 {
			break
		}
		index += 1
	}
	return bytesRead, nil
}

func (r *rCache) ReadAt(buf []byte, off int64) (n int, err error) {
	log.Debugf("rCache read len byte %d off %d length %v", len(buf), off, r.length)
	if len(buf) == 0 || int(off) >= r.length {
		return 0, nil
	}
	var index int
	var key string
	var uoff uint64
	var readAhead int

	index = r.index(int(off))
	key = r.key(index)
	blockOff := r.off(int(off))
	blockSize := r.store.conf.BlockSize
	start := time.Now()
	nReadFromCache, hitCache := r.readCache(buf, key, blockOff)
	if hitCache {
		// metrics
		log.Debugf("metrics cacheHits++:%d", nReadFromCache)
		cacheHits.Inc()
		cacheHitBytes.Add(float64(nReadFromCache))
		cacheReadHist.Observe(time.Since(start).Seconds())
		return nReadFromCache, nil
	}

	readAheadAmount := r.store.conf.MaxReadAhead
	if readAheadAmount == 0 {
		readAheadAmount = maxReadAheadSize
	}
	existingReadAhead := 0
	for readAheadAmount-existingReadAhead > 0 {
		uoff = uint64((index + readAhead) * blockSize)
		if int(uoff) >= r.length {
			break
		}
		readAhead += 1
		r.lock.RLock()
		_, ok := r.buffers[uoff]
		r.lock.RUnlock()
		if ok {
			continue
		}
		size := utils.Min(blockSize, r.length-int(uoff))
		if size == 0 {
			break
		}
		readBuf := &ReadBuffer{
			r:      r,
			offset: uoff,
			size:   uint32(size),
		}
		r.lock.Lock()
		r.buffers[uoff] = readBuf.Init(uoff, uint32(size), r.ufs, r.id, r.flags)
		existingReadAhead += size
		r.lock.Unlock()
	}

	n, err = r.readFromReadAhead(off, buf)
	return
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
	log.Debugf("read cache key is %s", key)
	// memory cache
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
