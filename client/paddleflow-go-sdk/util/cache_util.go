package util



type CacheProcess interface {
	ClearCache()
	WriteCache(data []byte)
	GetCache() map[string]string
}




