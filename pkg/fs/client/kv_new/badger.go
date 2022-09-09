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

package kv_new

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v3"
	log "github.com/sirupsen/logrus"
)

type KVTxn struct {
	t *badger.Txn
}

const (
	MemType  = "mem"
	DiskType = "disk"
)

func NewBadgerClient(config Config) (KvClient, error) {
	var db *badger.DB
	var err error
	if config.Driver == MemType {
		db, err = badger.Open(badger.DefaultOptions("").WithInMemory(true))
	} else if config.Driver == DiskType {
		cachePath := filepath.Join(config.CachePath, config.FsID+".db")
		os.RemoveAll(cachePath)
		db, err = badger.Open(badger.DefaultOptions(cachePath))
	} else {
		return nil, fmt.Errorf("not found meta driver name %s", config.Driver)
	}
	if err != nil {
		return nil, err
	}
	return &kvClient{db: db}, nil
}

func (kv *KVTxn) Get(key []byte) []byte {
	item, err := kv.t.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil
	}
	if err != nil {
		log.Errorf("badger get key %s with err %v", string(key), err)
		return nil
	}
	var value []byte
	value, err = item.ValueCopy(nil)
	if err != nil {
		log.Errorf("badger value copy key %s with err %v", string(key), err)
		return nil
	}
	return value
}

func (kv *KVTxn) Set(key, value []byte) error {
	err := kv.t.Set(key, value)
	return err
}

func (kv *KVTxn) Dels(keys ...[]byte) error {

	for _, key := range keys {
		err := kv.t.Delete(key)
		if err != nil {
			log.Printf("badger del key %s with err %v", string(key), err)
			return err
		}
	}
	return nil
}

func (kv *KVTxn) ScanValues(prefix []byte) (map[string][]byte, error) {
	it := kv.t.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	result := make(map[string][]byte)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()

		err := item.Value(func(v []byte) error {
			result[string(k)] = v
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (kv *KVTxn) Exist(Prefix []byte) bool {
	it := kv.t.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	it.Seek(Prefix)
	return it.ValidForPrefix(Prefix)
}

func (kv *KVTxn) Append(key []byte, value []byte) []byte {
	panic("implement me")
}

func (kv *KVTxn) IncrBy(key []byte, value int64) int64 {
	panic("implement me")
}

type kvClient struct {
	db *badger.DB
}

func (c *kvClient) Name() string {
	return "tikv"
}

func (c *kvClient) Txn(f func(txn KvTxn) error) error {
	tx := c.db.NewTransaction(true)
	defer tx.Discard()
	var err error

	if err = f(&KVTxn{tx}); err != nil {
		return err
	}

	err = tx.Commit()
	return err
}

func (c *kvClient) NextNumber(key []byte) (uint64, error) {
	seq, err := c.db.GetSequence(key, 1)
	if err != nil {
		return 0, err
	}
	num, err := seq.Next()
	return num + 2, err
}

var _ KvTxn = &KVTxn{}
