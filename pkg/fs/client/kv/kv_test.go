package kv

import (
	"flag"
	"testing"

	"github.com/google/btree"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

func newLevelDB() *leveldb.DB {
	o := &opt.Options{
		DisableBlockCache:      true,
		BlockRestartInterval:   5,
		BlockSize:              80,
		Compression:            opt.NoCompression,
		OpenFilesCacheCapacity: -1,
		Strict:                 opt.StrictAll,
		WriteBuffer:            1000,
		CompactionTableSize:    2000,
	}
	storage := storage.NewMemStorage()
	db, _ := leveldb.Open(storage, o)
	return db
}

func newMemDB() *btree.BTree {
	btreeDegree := flag.Int("degree", 32, "B-Tree degree")
	return btree.New(*btreeDegree)
}

func TestLevelDB(t *testing.T) {
	db := newLevelDB()
	lc := &levelDBClient{db: db}
	lc.Set([]byte("akey"), []byte("aval"))
	aval, exit := lc.Get([]byte("akey"))
	assert.True(t, exit, true)
	assert.Equal(t, []byte("aval"), aval)
	assert.True(t, lc.Exist([]byte("ak")), true)
	assert.False(t, lc.Exist([]byte("b")), false)
}

func TestMemDB(t *testing.T) {
	mc, _ := NewMemClient(Config{})
	mc.Set([]byte("akey"), []byte("aval"))
	aval, exit := mc.Get([]byte("akey"))
	assert.True(t, exit, true)
	assert.Equal(t, []byte("aval"), aval)
	assert.True(t, mc.Exist([]byte("ak")), true)
	assert.False(t, mc.Exist([]byte("bk")), false)
}
