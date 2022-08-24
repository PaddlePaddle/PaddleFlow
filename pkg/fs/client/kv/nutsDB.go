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

	log "github.com/sirupsen/logrus"
	"github.com/xujiajun/nutsdb"
)

const (
	NutsDB     = "nutsdb"
	bucketName = "mem"
	iterNum    = 200
)

type nutsDBClient struct {
	db *nutsdb.DB
}

func (l nutsDBClient) Name() string {
	return NutsDB
}

func (l nutsDBClient) Get(key []byte) ([]byte, bool) {
	var e *nutsdb.Entry
	var err error

	errGet := l.db.View(
		func(tx *nutsdb.Tx) error {
			if e, err = tx.Get(bucketName, key); err != nil {
				return err
			}
			return nil
		})
	if errGet != nil {
		return nil, false
	}
	return e.Value, true
}

func (l nutsDBClient) Set(key, value []byte) error {
	if err := l.db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.Put(bucketName, key, value, 0); err != nil {
				return err
			}
			return nil
		}); err != nil {
		return err
	}
	return nil
}

func (l nutsDBClient) Dels(keys ...[]byte) error {
	for _, key := range keys {
		if err := l.db.Update(
			func(tx *nutsdb.Tx) error {
				if err := tx.Delete(bucketName, key); err != nil {
					return err
				}
				return nil
			}); err != nil {
			return err
		}
	}
	return nil
}

func (l nutsDBClient) ScanValues(prefix []byte) (map[string][]byte, error) {
	result := make(map[string][]byte)
	var off int
	var err error
	var entries nutsdb.Entries

	for {
		if errScan := l.db.View(
			func(tx *nutsdb.Tx) error {
				if entries, _, err = tx.PrefixScan(bucketName, prefix, off, iterNum); err != nil {
					return err
				} else {
					for _, entry := range entries {
						result[string(entry.Key)] = entry.Value
					}
				}
				return nil
			}); errScan != nil {
			return nil, errScan
		}
		if len(entries) < iterNum {
			break
		}
		off += len(entries)
	}
	return result, nil
}

func (l nutsDBClient) Append(key []byte, value []byte) []byte {
	return nil
}

func (l nutsDBClient) IncrBy(key []byte, value int64) int64 {
	return 0
}

func (l nutsDBClient) Exist(prefix []byte) bool {
	return false
}

func NewNutsClient(config Config) (Client, error) {
	opt := nutsdb.DefaultOptions
	os.RemoveAll(config.CachePath)
	os.MkdirAll(config.CachePath, 0755)
	opt.Dir = config.CachePath
	opt.EntryIdxMode = nutsdb.HintKeyValAndRAMIdxMode
	db, err := nutsdb.Open(opt)
	if err != nil {
		log.Errorf("mem db open err %v", err)
	}

	return &nutsDBClient{db: db}, nil
}

var _ Client = &nutsDBClient{}
