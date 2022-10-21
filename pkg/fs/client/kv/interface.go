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

type Config struct {
	FsID      string
	Driver    string
	CachePath string
	Capacity  int64
}

type KvTxn interface {
	Get(key []byte) []byte
	Set(key, value []byte) error
	Dels(keys ...[]byte) error
	ScanValues(prefix []byte) (map[string][]byte, error)
	Exist(Prefix []byte) bool
	Append(key []byte, value []byte) []byte
	IncrBy(key []byte, value int64) int64
}

type KvClient interface {
	Name() string
	Txn(f func(KvTxn) error) error
}
