package kv

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
