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

package kv

import (
	"strings"
	"sync"

	"github.com/google/btree"
)

const Mem = "mem"

type kvItem struct {
	key   string
	value []byte
}

func (it kvItem) Less(than btree.Item) bool {
	return it.key < than.(*kvItem).key
}

type memClient struct {
	sync.Mutex
	db   *btree.BTree
	item *kvItem
}

func (l *memClient) Name() string {
	return Mem
}

func (l *memClient) Get(key []byte) ([]byte, bool) {
	l.item.key = string(key)
	data := l.db.Get(l.item)
	if data != nil {
		return data.(*kvItem).value, true
	}
	return nil, false
}

func (l *memClient) Set(key, value []byte) error {
	l.item.key = string(key)
	if value == nil {
		l.db.Delete(l.item)
		return nil
	}
	l.db.ReplaceOrInsert(&kvItem{key: string(key), value: value})
	return nil
}

func (l *memClient) Dels(keys ...[]byte) error {
	for _, key := range keys {
		l.item.key = string(key)
		l.db.Delete(l.item)
	}
	return nil
}

func (l *memClient) ScanValues(prefix []byte) (map[string][]byte, error) {
	l.Lock()
	defer l.Unlock()
	begin := string(prefix)
	// end := string(l.nextKey(prefix))
	ret := make(map[string][]byte)
	l.db.AscendGreaterOrEqual(&kvItem{key: begin}, func(i btree.Item) bool {
		it := i.(*kvItem)
		if strings.HasPrefix(it.key, begin) {
			ret[it.key] = it.value
		}
		if it == nil || it.value == nil {
			return false
		}
		return true
	})
	return ret, nil
}

func NewMemClient(config Config) (Client, error) {
	client := &memClient{db: btree.New(2), item: &kvItem{}}
	return client, nil
}

func (l *memClient) nextKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	next := make([]byte, len(key))
	copy(next, key)
	p := len(next) - 1
	for {
		next[p]++
		if next[p] != 0 {
			break
		}
		p--
		if p < 0 {
			panic("can't scan keys for 0xFF")
		}
	}
	return next
}

var _ Client = &memClient{}
