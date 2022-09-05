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
	"os"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

const LevelDB = "leveldb"

type levelDBClient struct {
	path string
	db   *leveldb.DB
}

func (l levelDBClient) Name() string {
	return LevelDB
}

func (l levelDBClient) Get(key []byte) ([]byte, bool) {
	data, err := l.db.Get(key, nil)
	if err != nil {
		return nil, false
	}
	return data, true
}

func (l levelDBClient) Set(key, value []byte) error {
	err := l.db.Put(key, value, nil)
	return err
}

func (l levelDBClient) Dels(keys ...[]byte) error {
	batch := new(leveldb.Batch)
	for _, key := range keys {
		batch.Delete(key)
	}
	err := l.db.Write(batch, nil)
	return err
}

func (l levelDBClient) ScanValues(prefix []byte) (map[string][]byte, error) {
	iter := l.db.NewIterator(util.BytesPrefix(prefix), nil)
	result := make(map[string][]byte)
	for iter.Next() {
		value := make([]byte, len(iter.Value()))
		copy(value, iter.Value())
		result[string(iter.Key())] = value
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (l levelDBClient) Exist(prefix []byte) bool {
	iter := l.db.NewIterator(util.BytesPrefix(prefix), nil)
	return iter.Last()
}
func (l levelDBClient) Append(key []byte, value []byte) []byte {
	data, exist := l.Get(key)
	if !exist {
		l.Set(key, value)
		return value
	}
	data = append(data, value...)
	l.Set(key, data)
	return data
}
func (l levelDBClient) IncrBy(key []byte, value int64) int64 {
	date, exist := l.Get(key)
	if !exist {
		l.Set(key, marshalCounter(value))
		return value
	}
	newValue := unmarshalCounter(date) + value
	l.Set(key, marshalCounter(newValue))
	return newValue

}

var _ Client = &levelDBClient{}

func NewLevelDBClient(config Config) (Client, error) {
	cachePath := filepath.Join(config.CachePath, config.FsID+"_"+utils.GetRandID(5)+".db")
	os.RemoveAll(cachePath)
	db, err := leveldb.OpenFile(cachePath, nil)
	if err != nil {
		return nil, err
	}
	return &levelDBClient{db: db}, nil
}
