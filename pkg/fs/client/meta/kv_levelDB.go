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
package meta

import (
	"os"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"

	"paddleflow/pkg/fs/utils/common"
)

const LevelDB = "leveldb"

type levelDBClient struct {
	path string
	db   *leveldb.DB
}

func (l levelDBClient) name() string {
	return LevelDB
}

func (l levelDBClient) get(key []byte) ([]byte, bool) {
	data, err := l.db.Get(key, nil)
	if err != nil {
		return nil, false
	}
	return data, true
}

func (l levelDBClient) set(key, value []byte) error {
	err := l.db.Put(key, value, nil)
	return err
}

func (l levelDBClient) dels(keys ...[]byte) error {
	batch := new(leveldb.Batch)
	for _, key := range keys {
		batch.Delete(key)
	}
	err := l.db.Write(batch, nil)
	return err
}

func (l levelDBClient) scanValues(prefix []byte) (map[string][]byte, error) {
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

func newLevelDBClient(config Config) (kvCache, error) {
	cachePath := filepath.Join(config.CachePath, config.FsID+"_"+common.GetRandID(5)+".db")
	os.RemoveAll(cachePath)
	db, err := leveldb.OpenFile(cachePath, nil)
	if err != nil {
		return nil, err
	}
	return &levelDBClient{db: db}, nil
}

var _ kvCache = &levelDBClient{}
