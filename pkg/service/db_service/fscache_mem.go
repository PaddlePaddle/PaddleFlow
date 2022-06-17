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

package db_service

import (
	"errors"
	"sync"

	"github.com/PaddlePaddle/PaddleFlow/pkg/models"
)

type ConcurrentFSCacheMap struct {
	sync.RWMutex
	// key1:fsID key2:cacheID
	value map[string]map[string]*models.FSCache
}

func newFSCacheMap() *ConcurrentFSCacheMap {
	cm := new(ConcurrentFSCacheMap)
	cm.value = map[string]map[string]*models.FSCache{}
	return cm
}

func (cm *ConcurrentFSCacheMap) Get(key1, key2 string) *models.FSCache {
	cm.RLock()
	var retValue *models.FSCache
	if v1, ok := cm.value[key1]; ok {
		retValue = v1[key2]
	}
	cm.RUnlock()
	return retValue
}

func (cm *ConcurrentFSCacheMap) GetBatch(key string) []models.FSCache {
	cm.RLock()
	var tmp []models.FSCache
	if v, ok := cm.value[key]; ok {
		for _, v1 := range v {
			tmp = append(tmp, *v1)
		}
	}
	cm.RUnlock()
	return tmp
}

func (cm *ConcurrentFSCacheMap) Put(key string, value *models.FSCache) {
	cm.Lock()
	tempV := map[string]*models.FSCache{}
	if v, ok := cm.value[key]; ok {
		tempV = v
	}
	tempV[value.CacheID] = value
	cm.value[key] = tempV
	cm.Unlock()
}

func (cm *ConcurrentFSCacheMap) Delete(key1, key2 string) error {
	cm.Lock()
	var err error
	if cm.value != nil {
		if key1 != "" {
			if key2 != "" {
				fsMap := cm.value[key1]
				delete(fsMap, key2)
				cm.value[key1] = fsMap
			} else {
				delete(cm.value, key1)
			}
		}
	} else {
		err = errors.New("FSCache map is null")
	}
	cm.Unlock()
	return err
}

func (cm *ConcurrentFSCacheMap) Update(key, value *models.FSCache) (has bool, err error) {
	cm.Lock()
	defer cm.Unlock()
	if v1, ok := cm.value[value.FsID]; ok {
		_, ok = v1[value.CacheID]
		if ok {
			has = true
			v1[value.CacheID] = value
		}
	}
	return has, nil
}

func newMemFSCache() FSCacheStore {
	m := new(MemFSCache)
	m.fsCacheMap = newFSCacheMap()
	return m
}

type MemFSCache struct {
	fsCacheMap *ConcurrentFSCacheMap
}

func (mem *MemFSCache) Add(value *models.FSCache) error {
	mem.fsCacheMap.Put(value.FsID, value)
	return nil
}

func (mem *MemFSCache) Get(fsID string, cacheID string) (*models.FSCache, error) {
	return mem.fsCacheMap.Get(fsID, cacheID), nil
}

func (mem *MemFSCache) Delete(fsID, cacheID string) error {
	return mem.fsCacheMap.Delete(fsID, cacheID)
}

func (mem *MemFSCache) List(fsID, cacheID string) ([]models.FSCache, error) {
	var retMap []models.FSCache
	if fsID != "" {
		if cacheID != "" {
			retMap = append(retMap, *mem.fsCacheMap.Get(fsID, cacheID))
		} else {
			retMap = mem.fsCacheMap.GetBatch(fsID)
		}
	}
	return retMap, nil
}

func (mem *MemFSCache) Update(value *models.FSCache) (int64, error) {
	return 0, nil
}
