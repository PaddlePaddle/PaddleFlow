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
	"bytes"
	"time"

	"github.com/bluele/gcache"
)

type memCache struct {
	m gcache.Cache
}

type MemConfig struct {
	CacheSize int
	Expire    time.Duration
}

func NewMemCache(config *MemConfig) *memCache {
	if config.CacheSize <= 0 {
		return nil
	}
	cacheBuilder := gcache.New(config.CacheSize).LRU()
	if config.Expire > 0 {
		cacheBuilder.Expiration(config.Expire * time.Second)
	}
	return &memCache{m: cacheBuilder.Build()}
}

func (c *memCache) load(key string) (ReadCloser, bool) {
	if item, err := c.m.Get(key); err == nil {
		return NewMemReader(item.([]byte)), true
	}
	return nil, false
}

func (c *memCache) save(key string, buf []byte) {
	_ = c.m.Set(key, buf)
}

func (c *memCache) delete(key string) {
	_, ok := c.load(key)
	if ok {
		c.m.Remove(key)
	}
}

func (c *memCache) clean() {

}

type memReader struct {
	*bytes.Reader
}

func NewMemReader(buf []byte) *memReader {
	return &memReader{
		bytes.NewReader(buf),
	}
}

func (r *memReader) Close() error {
	return nil
}

var _ ReadCloser = &memReader{}

var _ Cache = &memCache{}
