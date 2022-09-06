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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/monitor"
)

func registerMetrics() {
	_ = prometheus.Register(cacheHits)
	_ = prometheus.Register(cacheHitBytes)
	_ = prometheus.Register(cacheMiss)
	_ = prometheus.Register(cacheMissBytes)
	_ = prometheus.Register(cacheMissRate)
	_ = prometheus.Register(cacheWrites)
	_ = prometheus.Register(cacheWriteBytes)
	_ = prometheus.Register(cacheDrops)
	_ = prometheus.Register(cacheEvicts)
	_ = prometheus.Register(cacheReadHist)
	_ = prometheus.Register(cacheWriteHist)
}

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
		hitCnt := monitor.GetMetricValue(cacheHits)
		missCnt := monitor.GetMetricValue(cacheMiss)
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
)
