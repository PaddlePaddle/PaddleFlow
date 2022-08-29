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
	ver   int
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
	data := l.db.Get(l.item)
	if data != nil {
		data.(*kvItem).ver++ // need a lock ?
		data.(*kvItem).value = value
	} else {
		l.db.ReplaceOrInsert(&kvItem{key: string(key), value: value, ver: 1})
	}
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

func (l *memClient) Exist(prefix []byte) bool {
	pre := string(prefix)
	exist := false
	l.db.AscendGreaterOrEqual(&kvItem{key: pre}, func(i btree.Item) bool {
		exist = true
		return false
	})
	return exist
}
func (l *memClient) Append(key []byte, value []byte) []byte {
	l.item.key = string(key)
	data := l.db.Get(l.item)
	if data != nil {
		item := data.(*kvItem)
		newValue := append(item.value, value...)
		l.Set(key, newValue)
		return newValue

	}
	l.Set(key, value)
	return value
}
func (l *memClient) IncrBy(key []byte, value int64) int64 {
	l.item.key = string(key)
	data := l.db.Get(l.item)
	item := data.(*kvItem)
	cnt := unmarshalCounter(item.value)
	if value != 0 {
		cnt += value
		l.Set(key, marshalCounter(cnt))
	}
	return cnt
}

var _ Client = &memClient{}

func NewMemClient(config Config) (Client, error) {
	client := &memClient{db: btree.New(2), item: &kvItem{}}
	return client, nil
}
