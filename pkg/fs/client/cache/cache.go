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
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"paddleflow/pkg/fs/client/ufs"
	"paddleflow/pkg/fs/client/utils"
	"paddleflow/pkg/metric"
)

const (
	maxReadAheadNum = 16
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
	Mem       *MemConfig
	Disk      *DiskConfig
	BlockSize int
}

type rCache struct {
	id     string
	flags  uint32
	length int
	store  *store
	ufs    ufs.UnderFileStorage
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

func (store *store) NewReader(name string, length int, flags uint32, ufs ufs.UnderFileStorage) Reader {
	return &rCache{id: path.Clean(name), length: length, store: store, flags: flags, ufs: ufs}
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
	start := time.Now()
	nReadFromCache, ok := r.readCache(buf, key, blockOff)
	if ok {
		// metrics
		log.Debugf("metrics cacheHits++:%d", nReadFromCache)
		cacheHits.Inc()
		cacheHitBytes.Add(float64(nReadFromCache))
		cacheReadHist.Observe(time.Since(start).Seconds())
		return nReadFromCache, nil
	}

	readAheadNum := (r.length-1)/blockSize + 1
	readAheadNum = utils.Min(maxReadAheadNum, readAheadNum-index)

	nread := int64(0)
	group := new(errgroup.Group)
	for i := 0; i < readAheadNum; i++ {
		readAhead := i
		group.Go(func() error {
			uoff := (index + readAhead) * blockSize
			reader, err := r.ufs.Get(r.id, r.flags, int64(uoff), int64(utils.Min(blockSize, r.length-uoff)))
			if err != nil {
				return err
			}
			defer reader.Close()

			ufsBuf := make([]byte, blockSize)
			n, err := io.ReadFull(reader, ufsBuf)

			/*
				io.ReadFull 断言读，必须读 len(buf) 才视为成功
				当读取的内容字节数 n == 0 时，err = io.EOF
				当 0 < n < len(buf) 时，err = io.ErrUnexpectedEOF
				当 n == len(buf) 时，err = nil
				兼容两种情况
			*/
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				return err
			}
			if n == 0 {
				return nil
			}
			r.setCache(uoff, ufsBuf, n)
			// 读取的数据为0或者预读的部分大于buf数组的长度，进行过滤
			if readAhead*blockSize-blockOff >= bufSize {
				return nil
			}
			/**
			1. ufsBuf是固定一块区域的预读，对于第一块，需要偏移blockOff获取真实数据
			2. 对于后面的块，每次读的是一整块预读区
			*/
			var m int
			if readAhead == 0 {
				// 预读区blockOff后的数据才是需要真正读的数据
				m = copy(buf, ufsBuf[blockOff:n])
			} else {
				m = copy(buf[(readAhead*blockSize-blockOff):], ufsBuf[:n])
			}
			atomic.AddInt64(&nread, int64(m))

			return nil
		})
	}
	if err := group.Wait(); err != nil {
		log.Errorf("read from ufs err: %v", err)
		return 0, err
	}
	// metrics
	log.Debugf("metrics cacheMiss++:%d", nread)
	cacheMiss.Inc()
	cacheMissBytes.Add(float64(nread))

	return int(nread), nil
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
