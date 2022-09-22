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

package cache_new

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

const (
	FileClient = "fileClient"
	CacheDir   = "datacache"
)

var _ DataCacheClient = &fileDataCache{}

type cacheItem struct {
	size    int64
	expTime time.Time
}

type fileDataCache struct {
	sync.RWMutex
	dir      string
	capacity int64
	used     int64
	expire   time.Duration
	keys     sync.Map
}

func newFileClient(config Config) DataCacheClient {
	d := &fileDataCache{
		dir:    config.CachePath,
		expire: config.Expire,
	}

	if err := os.MkdirAll(config.CachePath, 0755); err != nil {
		log.Errorf("newFileClient os.MkdirAll [%s] err: %v", config.CachePath, err)
		return nil
	}
	if err := d.updateCapacity(); err != nil {
		log.Errorf("newFileClient d.updateCapacity err: %v", err)
		return nil
	}
	// 后续加stop channel
	go func() {
		for {
			d.clean()
			time.Sleep(10 * time.Second)
		}
	}()

	return d
}

func (c *fileDataCache) load(key string) (ReadCloser, bool) {
	if c.dir == "" {
		return nil, false
	}

	if !c.exist(key) {
		return nil, false
	}

	path := c.cachePath(key)
	_, err := os.Stat(path)

	if os.IsNotExist(err) {
		return nil, false
	}

	if err != nil {
		log.Errorf("stat cache file[%s] failed: %v", path, err)
		return nil, false
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	return f, true
}

func (c *fileDataCache) save(key string, buf []byte) {
	if c.dir == "" {
		return
	}
	cacheSize := int64(len(buf))
	if c.used+cacheSize >= c.capacity {
		// todo：clean支持带参数，释放多少容量。
		c.clean()
	}

	// 清理之后还是没有足够的容量，则跳过
	if c.used+cacheSize >= c.capacity {
		return
	}
	path := c.cachePath(key)
	c.createDir(filepath.Dir(path))
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		log.Errorf("open tmp file[%s] failed: %v", tmp, err)
		return
	}
	_, err = f.Write(buf)
	if err != nil {
		log.Errorf("write tmp file[%s] failed: %v", tmp, err)
		_ = f.Close()
		_ = os.Remove(tmp)
		return
	}
	err = f.Close()
	if err != nil {
		log.Errorf("close tmp file[%s] failed: %v", tmp, err)
		_ = os.Remove(tmp)
		return
	}
	err = os.Rename(tmp, path)
	if err != nil {
		log.Errorf("rename file %s -> %s failed: %v", tmp, path, err)
		_ = os.Remove(tmp)
		return
	}

	c.keys.Store(key, &cacheItem{
		expTime: time.Now().Add(c.expire),
		size:    cacheSize,
	})
	return
}

func (c *fileDataCache) delete(key string) {
	path := c.cachePath(key)
	_, ok := c.load(key)
	if ok {
		c.keys.Delete(key)
	}
	if path != "" {
		go os.Remove(path)
	}
}

func (c *fileDataCache) clean() {
	// 1. 首先清理掉已过期文件
	c.keys.Range(func(key, value interface{}) bool {
		cache := value.(*cacheItem)
		if time.Since(cache.expTime) >= 0 {
			c.delete(key.(string))
		}
		return true
	})

	cacheDir := filepath.Join(c.dir, CacheDir)
	if c.dir == "/" || c.dir == "" {
		return
	}
	// 2. 清理目录下存在，但是c.keys中不存在的的文件,
	if errCheck := filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Debugf("prevent panic by handling failure accessing a path %q: %v", path, err)
			return err
		}
		if info == nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}

		key := c.getKeyFromCachePath(path)
		_, ok := c.keys.Load(key)
		if ok {
			return nil
		}
		if strings.HasSuffix(path, "tmp") {
			return nil
		}
		return os.Remove(path)
	}); errCheck != nil {
		log.Debugf("data cache clean: filepath.Walk failed: %v", errCheck)
	}
	if errCheck := c.updateCapacity(); errCheck != nil {
		log.Debugf("data cache clean: updateCapacity failed: %v", errCheck)
	}
}

func (c *fileDataCache) cachePath(key string) string {
	return filepath.Join(c.dir, CacheDir, key)
}

func (c *fileDataCache) getKeyFromCachePath(path string) string {
	return strings.TrimPrefix(path, filepath.Join(c.dir, CacheDir)+"/")
}

func (c *fileDataCache) createDir(dir string) {
	os.MkdirAll(dir, 0755)
}

func (c *fileDataCache) exist(key string) bool {
	value, ok := c.keys.Load(key)
	if !ok {
		return false
	} else {
		cache := value.(*cacheItem)
		if time.Until(cache.expTime) <= 0 {
			return false
		}
	}
	return true
}

func (c *fileDataCache) updateCapacity() error {
	output, err := utils.ExecCmdWithTimeout("df", []string{"-k", c.dir})
	if err != nil {
		log.Errorf("df %s %v ", c.dir, err)
		return err
	}

	buf := bufio.NewReader(bytes.NewReader(output))
	for {
		if line, _, err := buf.ReadLine(); err != nil {
			return err
		} else {
			if strings.HasPrefix(string(line), "Filesystem") {
				continue
			}
			strSlice := strings.Fields(strings.TrimSpace(string(line)))
			if len(strSlice) < 6 {
				continue
			}

			total, err := strconv.ParseInt(strSlice[1], 10, 64)
			if err != nil {
				log.Errorf("parse str[%s] failed: %v", strSlice[1], err)
				return err
			}
			used, err := strconv.ParseInt(strSlice[2], 10, 64)
			if err != nil {
				log.Errorf("parse str[%s] failed: %v", strSlice[2], err)
				return err
			}
			c.Lock()
			c.capacity = total * 1024
			c.used = used * 1024
			c.Unlock()
			return nil
		}
	}
}
