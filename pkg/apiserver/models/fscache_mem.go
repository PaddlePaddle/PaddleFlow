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

package models

import (
	"errors"
	"sync"
)

type ConcurrentFSCacheMap struct {
	sync.RWMutex
	// key1:fsID key2:cacheID
	value map[string]map[string]*FSCache
}

func newFSCacheMap() *ConcurrentFSCacheMap {
	cm := new(ConcurrentFSCacheMap)
	cm.value = map[string]map[string]*FSCache{}
	return cm
}

func (cm *ConcurrentFSCacheMap) Get(key1, key2 string) *FSCache {
	cm.RLock()
	var retValue *FSCache
	if v1, ok := cm.value[key1]; ok {
		if ok {
			retValue = v1[key2]
		}
	}
	cm.RUnlock()
	return retValue
}

func (cm *ConcurrentFSCacheMap) GetBatch(key string) []FSCache {
	cm.RLock()
	var tmp []FSCache
	if v, ok := cm.value[key]; ok {
		for _, v1 := range v {
			tmp = append(tmp, *v1)
		}
	}
	cm.RUnlock()
	return tmp
}

func (cm *ConcurrentFSCacheMap) Put(key string, value *FSCache) {
	cm.Lock()
	tempV := map[string]*FSCache{}
	if v, ok := cm.value[key]; ok {
		tempV = v
	}
	tempV[value.CacheID] = value
	cm.value[key] = tempV
	cm.Unlock()
}
func (cm *ConcurrentFSCacheMap) Delete(key1, key2 string) {
	cm.Lock()
	if key1 != "" {
		if key2 != "" {
			fsMap := cm.value[key1]
			delete(fsMap, key2)
			cm.value[key1] = fsMap
		} else {
			delete(cm.value, key1)
		}
	}
	cm.Unlock()
}

func newMemFSCache() FSCacheStore {
	m := new(MemFSCache)
	m.fsCacheMap = newFSCacheMap()
	return m
}

type MemFSCache struct {
	fsCacheMap *ConcurrentFSCacheMap
}

func (mem *MemFSCache) AddFSCache(value *FSCache) error {
	mem.fsCacheMap.Put(value.FSID, value)
	return nil
}

func (mem *MemFSCache) GetFSCache(fsID string, cacheID string) (*FSCache, error) {
	return mem.fsCacheMap.Get(fsID, cacheID), nil
}

func (mem *MemFSCache) DeleteFSCache(fsID, cacheID string) error {
	mem.fsCacheMap.Delete(fsID, cacheID)
	return nil
}

func (mem *MemFSCache) ListFSCaches(fsID, cacheID string) ([]FSCache, error) {
	if fsID != "" {
		if cacheID != "" {
			retV := []FSCache{}
			retV = append(retV, *mem.fsCacheMap.Get(fsID, cacheID))
			return retV, nil
		} else {
			return mem.fsCacheMap.GetBatch(fsID), nil
		}
	}
	return nil, errors.New("No Record in Memory ")
}

func (mem *MemFSCache) UpdateFSCache() error {
	return nil
}
