package kv

import "fmt"

type Client interface {
	Name() string
	Get(key []byte) ([]byte, bool)
	Set(key, value []byte) error
	Dels(keys ...[]byte) error
	ScanValues(prefix []byte) (map[string][]byte, error)
}

type Config struct {
	FsID      string
	Driver    string
	CachePath string
}

func NewClient(config Config) (Client, error) {
	var client Client
	var err error
	switch config.Driver {
	case LevelDB:
		client, err = NewLevelDBClient(config)
	case NutsDB:
		client, err = NewNutsClient(config)
	case Mem:
		client, err = NewMemClient(config)
	default:
		return nil, fmt.Errorf("unknown meta client")
	}
	return client, err
}
