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
	"syscall"
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
	READAHEAD_CHUNK  = uint64(32 * 1024 * 1024)
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
	id            string
	flags         uint32
	length        int
	store         *store
	ufs           ufs.UnderFileStorage
	buffers       ReadBufferMap
	bufferPool    *BufferPool
	lock          sync.RWMutex
	seqReadAmount uint64
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
		nread, err = readAheadBuf.ReadAt(uint64(blockOff), buf[bytesRead:])
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			break
		}
		if readAheadBuf.size <= 0 && readAheadBuf.Buffer.page.ready {
			readAheadBuf.Buffer.Close()
			r.lock.Lock()
			delete(r.buffers, indexOff)
			r.lock.Unlock()
			index += 1
			blockOff = 0
			bytesRead += nread
			blockOff += nread
			break
		}
		if nread == 0 {
			break
		}
		bytesRead += nread
		blockOff += nread
	}
	return bytesRead, nil
}

func (r *rCache) readAhead(index int) (err error) {
	var uoff uint64
	blockSize := r.store.conf.BlockSize
	readAheadAmount := r.store.conf.MaxReadAhead

	if readAheadAmount == 0 {
		readAheadAmount = maxReadAheadSize
	}
	// first we get small readAhead size
	if r.seqReadAmount <= READAHEAD_CHUNK {
		readAheadAmount = utils.Max(int(READAHEAD_CHUNK), blockSize)
	}
	existingReadAhead := 0
	for readAheadAmount-existingReadAhead >= blockSize {
		uoff = uint64((index) * blockSize)
		if int(uoff) >= r.length {
			break
		}
		r.lock.RLock()
		_, ok := r.buffers[uoff]
		r.lock.RUnlock()
		if ok {
			index += 1
			existingReadAhead += blockSize
			continue
		}
		size := utils.Min(blockSize, r.length-int(uoff))
		if size == 0 {
			break
		}
		readBuf := &ReadBuffer{
			offset: uoff,
			size:   uint32(size),
			ufs:    r.ufs,
			path:   r.id,
			flags:  r.flags,
			r:      r,
			index:  index,
		}
		readAheadBuf := readBuf.Init(r.bufferPool, blockSize)
		if readAheadBuf != nil {
			r.lock.Lock()
			r.buffers[uoff] = readAheadBuf
			existingReadAhead += size
			index += 1
			r.lock.Unlock()
		} else {
			if existingReadAhead != 0 {
				return nil
			} else {
				return syscall.ENOMEM
			}
		}
	}
	return nil
}

func (r *rCache) ReadAt(buf []byte, off int64) (n int, err error) {
	log.Debugf("rCache read len byte %d off %d "+
		"length %v conf %+v buffers %v", len(buf), off, r.length, r.store.conf, len(r.buffers))
	if len(buf) == 0 || int(off) >= r.length {
		return 0, nil
	}
	var index int
	var key string

	index = r.index(int(off))
	key = r.key(index)
	blockOff := r.off(int(off))
	start := time.Now()
	nReadFromCache, hitCache := r.readCache(buf, key, blockOff)
	if hitCache {
		// metrics
		cacheHits.Inc()
		cacheHitBytes.Add(float64(nReadFromCache))
		cacheReadHist.Observe(time.Since(start).Seconds())
		log.Debugf("metrics cacheHits++:%d and index %v blockOff %v and nread %v", nReadFromCache, index, blockOff, nReadFromCache)
		return nReadFromCache, nil
	}
	err = r.readAhead(index)
	log.Debugf("read buffers map %v", len(r.buffers))
	if err == nil {
		n, err = r.readFromReadAhead(off, buf)
		return
	}
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
	keyID, ok := r.store.meta[r.id]
	r.store.RUnlock()
	if !ok {
		r.store.Lock()
		keyID = uuid.NewString()
		r.store.meta[r.id] = keyID
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

func (r *rCache) setCache(index int, p []byte, n int) {
	if n <= 0 {
		return
	}
	key := r.key(index)
	log.Debugf("cache set key is %s name %s", key, r.id)
	right := r.store.conf.BlockSize
	if right > n {
		right = n
	}
	if r.store.mem != nil && r.store.conf.Mem.CacheSize > 0 {
		r.store.mem.save(key, p[:right])
	}
	if r.store.disk != nil {
		r.store.disk.save(key, p[:right])
	}
}
