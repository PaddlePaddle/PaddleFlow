package kv

import "encoding/binary"

type Client interface {
	Name() string
	Get(key []byte) ([]byte, bool)
	Set(key, value []byte) error
	Dels(keys ...[]byte) error
	ScanValues(prefix []byte) (map[string][]byte, error)
	Exist(prefix []byte) bool
	Append(key []byte, value []byte) []byte
	IncrBy(key []byte, value int64) int64
}

type Config struct {
	FsID      string
	Driver    string
	CachePath string
}

type kvTxn interface {
	Get(key []byte) ([]byte, bool)
	Set(key, value []byte) error
	Dels(keys ...[]byte) error
	ScanValues(prefix []byte) (map[string][]byte, error)
	Exist(Prefix []byte) bool
	Append(key []byte, value []byte) []byte
	IncrBy(key []byte, value int64) int64
}

func unmarshalCounter(buf []byte) int64 {
	if len(buf) == 0 {
		return 0
	}
	if len(buf) != 8 {
		panic("invalid counter value")
	}
	return int64(binary.LittleEndian.Uint64(buf))
}

func marshalCounter(value int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(value))
	return b
}

func nextKey(key []byte) []byte {
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
