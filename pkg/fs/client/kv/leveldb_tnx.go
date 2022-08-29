package kv

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type levdbTnx struct {
	t *leveldb.Transaction
}

func (tx *levdbTnx) Get(key []byte) ([]byte, bool) {
	data, err := tx.t.Get(key, &opt.ReadOptions{})
	if err != nil {
		return nil, false
	}
	return data, true
}
func (tx *levdbTnx) Set(key, value []byte) error {
	return tx.t.Put(key, value, &opt.WriteOptions{})
}
func (tx *levdbTnx) Dels(keys ...[]byte) error {
	batch := new(leveldb.Batch)
	for _, key := range keys {
		batch.Delete(key)
	}
	return tx.t.Write(batch, &opt.WriteOptions{})

}
func (tx *levdbTnx) ScanValues(prefix []byte) (map[string][]byte, error) {
	iter := tx.t.NewIterator(util.BytesPrefix(prefix), nil)
	ret := make(map[string][]byte)
	for iter.Next() {
		value := make([]byte, len(iter.Value()))
		copy(value, iter.Value())
		ret[string(iter.Key())] = value
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return nil, err
	}
	return ret, nil
}
func (tx *levdbTnx) Exist(prefix []byte) bool {
	iter := tx.t.NewIterator(util.BytesPrefix(prefix), nil)
	return iter.Last()

}
func (tx *levdbTnx) Append(key []byte, value []byte) []byte {
	oldval, exist := tx.Get(key)
	if !exist {
		tx.Set(key, value)
		return value
	}
	newval := append(oldval, value...)
	tx.Set(key, newval)
	return newval
}
func (tx *levdbTnx) IncrBy(key []byte, value int64) int64 {
	oldcnt, exist := tx.Get(key)
	if !exist {
		tx.Set(key, marshalCounter(value))
		return value
	}
	newcnt := unmarshalCounter(oldcnt) + value
	tx.Set(key, marshalCounter(newcnt))
	return newcnt
}

func (l *levelDBClient) Txn(f func(kvTxn) error) (err error) {
	t, err := l.db.OpenTransaction()
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			fe, ok := r.(error)
			if ok {
				err = fe
			} else {
				panic(r)
			}
		}
	}()
	if err = f(&levdbTnx{t}); err != nil {
		return err
	}
	err = t.Commit()
	if err != nil {
		t.Discard()
		return err
	}
	return nil
}
