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
	"fmt"
	"strings"

	"github.com/google/btree"
)

type memTxn struct {
	store    *memClient
	observed map[string]int
	buffer   map[string][]byte
}

func (tx *memTxn) Get(key []byte) ([]byte, bool) {
	k := string(key)
	if v, ok := tx.buffer[k]; ok {
		return v, true
	}
	tx.store.Lock()
	defer tx.store.Unlock()
	tx.store.item.key = k
	data := tx.store.db.Get(tx.store.item)
	if data == nil {
		tx.observed[k] = 0
		return nil, false
	}
	item := data.(*kvItem)
	tx.observed[k] = item.ver
	return item.value, true
}

func (tx *memTxn) Set(key, value []byte) error {

	tx.buffer[string(key)] = value
	return nil

}

func (tx *memTxn) Dels(keys ...[]byte) error {
	for _, key := range keys {
		tx.buffer[string(key)] = nil
	}
	return nil
}

func (tx *memTxn) ScanValues(prefix []byte) (map[string][]byte, error) {

	return tx.scanRange(prefix, nextKey(prefix)), nil

}

func (tx *memTxn) Exist(Prefix []byte) bool {
	return len(tx.keyWithPrefix(Prefix)) > 0
}
func (tx *memTxn) Append(key []byte, value []byte) []byte {
	oldval, exist := tx.Get(key)
	if !exist {
		tx.Set(key, value)
		return value
	}
	newval := append(oldval, value...)
	tx.Set(key, newval)
	return newval
}
func (tx *memTxn) IncrBy(key []byte, value int64) int64 {
	oldva, exist := tx.Get(key)
	if !exist {
		tx.Set(key, marshalCounter(value))
		return value
	}
	cnt := unmarshalCounter(oldva) + value
	tx.Set(key, marshalCounter(cnt))
	return cnt
}

var _ kvTxn = (*memTxn)(nil)

//define op seq and commit transaction
func (c *memClient) Txn(f func(kvTxn) error) error {
	tx := &memTxn{
		store:    c,
		observed: make(map[string]int),
		buffer:   make(map[string][]byte),
	}
	if err := f(tx); err != nil {
		return err
	}
	if len(tx.buffer) == 0 {
		return nil
	}
	c.Lock()
	defer c.Unlock()
	for k, ver := range tx.observed {
		c.item.key = k
		data := c.db.Get(c.item)
		if data == nil && ver != 0 {
			return fmt.Errorf("write conflict: %s was version %d, now deleted", k, ver)
		} else if data != nil {
			iterm := data.(*kvItem)
			if iterm.ver > ver {
				return fmt.Errorf("write conflict: %s %d > %d", k, iterm.ver, ver)
			}
		}
	}
	for k, value := range tx.buffer {
		c.Set([]byte(k), value)
	}
	return nil
}

//utils
func (tx *memTxn) scanRange(begin_, end_ []byte) map[string][]byte {
	tx.store.Lock()
	defer tx.store.Unlock()
	begin := string(begin_)
	end := string(end_)
	ret := make(map[string][]byte)
	tx.store.db.AscendGreaterOrEqual(&kvItem{key: begin}, func(i btree.Item) bool {
		it := i.(*kvItem)
		if end == "" || it.key < end {
			tx.observed[it.key] = it.ver
			ret[it.key] = it.value
			return true
		}
		return false
	})
	return ret
}

func (tx *memTxn) keyWithPrefix(prefix_ []byte) [][]byte {
	var ret [][]byte
	tx.store.Lock()
	defer tx.store.Unlock()
	prefix := string(prefix_)
	tx.store.db.AscendGreaterOrEqual(&kvItem{key: prefix}, func(i btree.Item) bool {
		item := i.(*kvItem)
		if strings.HasPrefix(item.key, prefix) {
			tx.observed[item.key] = item.ver
			ret = append(ret, []byte(item.key))
			return true
		}
		return false
	})
	return ret
}
