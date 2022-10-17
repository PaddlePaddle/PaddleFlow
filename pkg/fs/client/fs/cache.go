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

package fs

import (
	"time"

	"github.com/bluele/gcache"
	log "github.com/sirupsen/logrus"

	vfs "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/vfs"
)

const (
	defaultEntryExpire = 0
	defaultAttrExpire  = 10
	// 假设string+uint64是100字节，102400需要9.8MB
	defaultEntryCacheSize = 102400
	// 假设uint64+attr空间是400字节，102400需要约40MB
	defaultAttrCacheSize = 102400
)

type metaCache struct {
	entryCache gcache.Cache
	attrCache  gcache.Cache
}

func NewMetaCache(entryCacheSize, attrCacheSize int, entryExpire int, attrExpire int) *metaCache {
	log.Debugf("init metaCache: entryCacheSize[%d], attrCacheSize[%d], entryExpire[%d], attrExpire[%d]",
		entryCacheSize, attrCacheSize, entryExpire, attrExpire)
	entryCacheBuilder := gcache.New(entryCacheSize).LRU()
	if entryExpire > 0 {
		entryCacheBuilder.Expiration(time.Duration(entryExpire) * time.Second)
	}

	attrCacheBuilder := gcache.New(attrCacheSize).LRU()
	if attrExpire > 0 {
		attrCacheBuilder.Expiration(time.Duration(attrExpire) * time.Second)
	}

	return &metaCache{
		entryCache: entryCacheBuilder.Build(),
		attrCache:  attrCacheBuilder.Build(),
	}
}

func (mc *metaCache) GetEntryItem(path string) (interface{}, bool) {
	log.Debugf("Get entryCache: key[%s]", path)
	if value, err := mc.entryCache.Get(path); err == nil {
		return value, true
	} else {
		return nil, false
	}
}

func (mc *metaCache) SetEntryItem(path string, entryIno interface{}) {
	log.Debugf("set entryCache: key[%s], value[%+v]", path, entryIno)
	_ = mc.entryCache.Set(path, entryIno)
}

func (mc *metaCache) RemoveEntryItem(path string) {
	log.Infof("remove entryCache: key[%s]", path)
	if mc.entryCache.Has(path) {
		mc.entryCache.Remove(path)
	}
}

func (mc *metaCache) GetAttrItem(inode vfs.Ino) (interface{}, bool) {
	log.Debugf("Get attrCache: key[%d]", inode)
	if value, err := mc.attrCache.Get(inode); err == nil {
		return value, true
	} else {
		return nil, false
	}
}

func (mc *metaCache) SetAttrItem(inode vfs.Ino, attr interface{}) {
	log.Debugf("Set attrCache: key[%d], value[%+v]", inode, attr)
	_ = mc.attrCache.Set(inode, attr)
}

func (mc *metaCache) RemoveAttrItem(inode vfs.Ino) {
	log.Debugf("remove entryCache: key[%d]", inode)
	if mc.attrCache.Has(inode) {
		mc.attrCache.Remove(inode)
	}
}
