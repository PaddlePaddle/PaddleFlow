package kv

import "time"

type KvCache interface {
	Name() string
	Get(key []byte) ([]byte, bool)
	Set(key, value []byte) error
	Dels(keys ...[]byte) error
	ScanValues(prefix []byte) (map[string][]byte, error)
}

type MetaConfig struct {
	AttrCacheExpire    time.Duration
	EntryCacheExpire   time.Duration
	FsID               string
	Driver             string
	CachePath          string
	AttrCacheSize      uint64
	EntryAttrCacheSize uint64
}
