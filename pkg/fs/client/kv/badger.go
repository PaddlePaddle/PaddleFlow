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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v3"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
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
		if config.CachePath == "" {
			return nil, fmt.Errorf("meta cache config path is not allowed empty")
		}
		cachePath := filepath.Join(config.CachePath, config.FsID+".db")
		log.Infof("meta disk cache path %v", cachePath)
		os.RemoveAll(cachePath)
		if config.FsID == "" {
			cachePath = filepath.Join(config.CachePath, strconv.Itoa(int(time.Now().Unix()))+"_"+utils.GetRandID(5))
		}
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
		log.Debugf("badger get key %s with err %v", string(key), err)
		return nil
	}
	var value []byte
	value, err = item.ValueCopy(nil)
	if err != nil {
		log.Debugf("badger value copy key %s with err %v", string(key), err)
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
			log.Debugf("badger del key %s with err %v", string(key), err)
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
		v := kv.Get(k)
		result[string(k)] = v
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
	var number int64
	buf := kv.Get(key)
	if len(buf) > 0 {
		number = parseCounter(buf)
	}
	if value != 0 {
		number += value
		_ = kv.Set(key, packCounter(number))
	}
	return number
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
		log.Debugf("txn err is %v", err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Debugf("tx commit err %v", err)
	}
	return err
}

func parseCounter(buf []byte) int64 {
	if len(buf) == 0 {
		return 0
	}
	if len(buf) != 8 {
		panic("invalid counter value")
	}
	return int64(binary.LittleEndian.Uint64(buf))
}

func packCounter(value int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(value))
	return b
}

var _ KvTxn = &KVTxn{}
