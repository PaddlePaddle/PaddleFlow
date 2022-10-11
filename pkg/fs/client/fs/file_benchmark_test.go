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

package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	cache "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/cache"
	kv "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv"
	meta "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta"
	vfs "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/vfs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

// FS一层的gcache有影响，关掉之后性能对比明显多了
func newPfsTestWithOutVfsLevelCache() (*FileSystem, error) {
	os.MkdirAll("./mock", 0755)
	DataCachePath = "./mock-cache"
	testFsMeta := common.FSMeta{
		UfsType: common.LocalType,
		Properties: map[string]string{
			common.RootKey: "./mock",
		},
		SubPath: "./mock",
	}
	vfsConfig := vfs.InitConfig(
		vfs.WithDataCacheConfig(cache.Config{
			BlockSize:    BlockSize,
			MaxReadAhead: MaxReadAheadNum,
			Expire:       DataCacheExpire,
			Config: kv.Config{
				CachePath: DataCachePath,
			},
		}),
		vfs.WithMetaConfig(meta.Config{
			AttrCacheExpire:  MetaCacheExpire,
			EntryCacheExpire: EntryCacheExpire,
			PathCacheExpire:  PathCacheExpire,
			Config: kv.Config{
				Driver:    Driver,
				CachePath: MetaCachePath,
			},
		}),
	)
	pfs, err := NewFileSystem(testFsMeta, nil, true, false, "", vfsConfig)
	if err != nil {
		return nil, err
	}
	return pfs, nil
}

func Benchmark_PathWithOutCache(b *testing.B) {
	b.StopTimer()
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	os.RemoveAll("./mock-meta")
	defer func() {
		os.RemoveAll("./mock")
		os.RemoveAll("./mock-cache")
		os.RemoveAll("./mock-meta")
	}()
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 10,
		Expire:       10 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	m := meta.Config{
		AttrCacheExpire:  100 * time.Second,
		EntryCacheExpire: 100 * time.Second,
		PathCacheExpire:  0 * time.Second,
		Config: kv.Config{
			Driver:    kv.DiskType,
			CachePath: "./mock-meta",
		},
	}
	SetMetaCache(m)
	client, err := newPfsTestWithOutVfsLevelCache()
	assert.Nil(b, err)
	dirs := make([]string, 101)
	for i := 1; i < len(dirs); i++ {
		dirs[i] = fmt.Sprintf("dir%d", i)
	}
	dir1 := &strings.Builder{}
	for i := 1; i <= 100; i++ {
		dir1.WriteString(dirs[i])
		err = client.Mkdir(dir1.String(), 0755)
		assert.Nil(b, err)
		dir1.WriteString("/")
	}
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	mode := 0644
	b.N = 100
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		file := fmt.Sprintf("file-%d", i)
		_, err = client.Create(filepath.Join(dir1.String(), file), uint32(mode), uint32(flags))
		assert.Nil(b, err)
	}
}

func Benchmark_PathWithcache(b *testing.B) {
	b.StopTimer()
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	os.RemoveAll("./mock-meta")
	defer func() {
		os.RemoveAll("./mock")
		os.RemoveAll("./mock-cache")
		os.RemoveAll("./mock-meta")
	}()
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 10,
		Expire:       10 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	m := meta.Config{
		AttrCacheExpire:  100 * time.Second,
		EntryCacheExpire: 100 * time.Second,
		PathCacheExpire:  4 * time.Second,
		Config: kv.Config{
			Driver:    kv.DiskType,
			CachePath: "./mock-meta",
		},
	}
	SetMetaCache(m)
	client, err := newPfsTestWithOutVfsLevelCache()
	assert.Nil(b, err)
	dirs := make([]string, 101)
	for i := 1; i < len(dirs); i++ {
		dirs[i] = fmt.Sprintf("dir%d", i)
	}
	dir1 := &strings.Builder{}
	for i := 1; i <= 100; i++ {
		dir1.WriteString(dirs[i])
		err = client.Mkdir(dir1.String(), 0755)
		assert.Nil(b, err)
		dir1.WriteString("/")
	}
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	mode := 0644
	b.N = 100
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		file := fmt.Sprintf("file-%d", i)
		_, err = client.Create(filepath.Join(dir1.String(), file), uint32(mode), uint32(flags))
		assert.Nil(b, err)
	}
}
