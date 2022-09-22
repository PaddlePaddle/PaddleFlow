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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/cache"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/vfs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

func newPfsTest() (*FileSystem, error) {
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
			Config: kv.Config{
				Driver:    Driver,
				CachePath: MetaCachePath,
			},
		}),
	)
	pfs, err := NewFileSystem(testFsMeta, nil, true, true, "", vfsConfig)
	if err != nil {
		return nil, err
	}
	return pfs, nil
}

func TestFSClient_readAt_BigOff(t *testing.T) {
	// 测试off越界的情况
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer func() {
		os.RemoveAll("./mock")
		os.RemoveAll("./mock-cache")
	}()
	d := cache.Config{
		BlockSize:    2,
		MaxReadAhead: 100,
		Config: kv.Config{
			Driver: kv.NutsDB,
		},
	}
	SetDataCache(d)
	client, err := newPfsTest()
	assert.Equal(t, err, nil)

	path := "testRead"
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	mode := 0666
	writer, err := client.Create(path, uint32(flags), uint32(mode))
	assert.Equal(t, err, nil)

	writeString := "12345678"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, err, nil)
	writer.Close()

	var reader *File
	var buf []byte
	var n int

	n = 10
	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	buf = make([]byte, n)
	n, err = reader.ReadAt(buf, 11)
	assert.Equal(t, 0, n)
	reader.Close()

	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	buf = make([]byte, 3)
	n, err = reader.ReadAt(buf, 1)
	assert.Equal(t, 3, n)
	assert.Equal(t, nil, err)

	n = 10
	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	buf = make([]byte, n)
	n, err = reader.ReadAt(buf, 8)
	assert.Equal(t, 0, n)
	reader.Close()
}

func TestFsStat(t *testing.T) {
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer func() {
		os.RemoveAll("./mock")
		os.RemoveAll("./mock-cache")
	}()
	d := cache.Config{
		BlockSize:    4,
		MaxReadAhead: 10,
		Expire:       10 * time.Second,
		Config: kv.Config{
			Driver:    kv.NutsDB,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client, err := newPfsTest()
	assert.Equal(t, err, nil)

	path := "test1"
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	mode := 0644
	writer, err := client.Create(path, uint32(flags), uint32(mode))
	assert.Equal(t, nil, err)
	writeNum := 5500
	n, err := writer.Write([]byte(getRandomString(writeNum)))
	assert.Equal(t, nil, err)
	assert.Equal(t, writeNum, n)
	err = writer.Close()
	assert.Equal(t, nil, err)
	info, err := client.Stat(path)
	assert.Equal(t, nil, err)
	info2, err := client.Stat(path)
	assert.Equal(t, nil, err)
	assert.Equal(t, info2.ModTime(), info.ModTime())
}

func TestFS_read_readAt(t *testing.T) {
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer func() {
		os.RemoveAll("./mock")
		os.RemoveAll("./mock-cache")
	}()
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 10,
		Expire:       10 * time.Second,
		Config: kv.Config{
			Driver:    kv.NutsDB,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	// new client
	client, err := newPfsTest()
	assert.Equal(t, err, nil)
	// 创建文件
	path := "test1"
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	mode := 0644
	writer, err := client.Create(path, uint32(flags), uint32(mode))
	assert.Equal(t, nil, err)
	writeNum := 5500
	n, err := writer.Write([]byte(getRandomString(writeNum)))
	assert.Equal(t, nil, err)
	assert.Equal(t, writeNum, n)
	err = writer.Close()
	assert.Equal(t, nil, err)
	path = "test"
	writer, err = client.Create(path, uint32(flags), uint32(mode))
	assert.Equal(t, nil, err)
	writeString := "123456789abcdefghijklmn123456789abcedfegijklmn111222333444555666777"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, err, nil)
	var reader *File
	var buf []byte

	n = 10
	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	buf = make([]byte, n)
	n, err = reader.Read(buf)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "123456789a")
	n2 := 2
	buf = make([]byte, n2)
	n, err = reader.ReadAt(buf, 3)
	assert.Equal(t, n2, n)
	assert.Equal(t, "45", string(buf))
	n3 := 10
	buf = make([]byte, n3)
	n, err = reader.Read(buf)
	assert.Equal(t, n3, n)
	assert.Equal(t, "bcdefghijk", string(buf))

	reader.Close()
	// next read cache
	n = 10
	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	buf = make([]byte, n)
	n, err = reader.Read(buf)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "123456789a")

	n2 = 2
	buf = make([]byte, n2)
	n, err = reader.ReadAt(buf, 3)
	assert.Equal(t, n2, n)
	assert.Equal(t, "45", string(buf))

	n3 = 10
	buf = make([]byte, n3)
	n, err = reader.Read(buf)
	assert.Equal(t, n3, n)
	assert.Equal(t, "bcdefghijk", string(buf))
}

func TestReadAtCocurrent(t *testing.T) {
	// 使用单个block的情况
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer func() {
		os.RemoveAll("./mock")
		os.RemoveAll("./mock-cache")
	}()
	d := cache.Config{
		BlockSize:    3,
		MaxReadAhead: 10,
		Expire:       10 * time.Second,
		Config: kv.Config{
			Driver:    kv.NutsDB,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client, err := newPfsTest()
	assert.Equal(t, err, nil)

	path := "testRead"
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	mode := 0666
	writer, err := client.Create(path, uint32(flags), uint32(mode))
	assert.Equal(t, err, nil)

	writeString := "123456789"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, err, nil)
	writer.Close()

	var reader *File

	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	var wg sync.WaitGroup
	g := 10
	wg.Add(g)
	for i := 0; i < g; i++ {
		go func() {
			n1 := 3
			buf := make([]byte, n1)
			n2, err := reader.ReadAt(buf, 2)
			assert.Equal(t, nil, err)
			assert.Equal(t, len(buf), n2)
			assert.Equal(t, "345", string(buf))

			n3 := 4
			buf2 := make([]byte, n3)
			n4, err := reader.ReadAt(buf2, 3)
			assert.Equal(t, nil, err)
			assert.Equal(t, len(buf2), n4)
			assert.Equal(t, "4567", string(buf2))
			wg.Done()
		}()
	}
	wg.Wait()

	g = 10
	wg.Add(g)
	for i := 0; i < g; i++ {
		go func() {
			n1 := 3
			buf := make([]byte, n1)
			n2, err := reader.ReadAt(buf, 2)
			assert.Equal(t, nil, err)
			assert.Equal(t, len(buf), n2)
			assert.Equal(t, "345", string(buf))

			n3 := 4
			buf2 := make([]byte, n3)
			n4, err := reader.ReadAt(buf2, 3)
			assert.Equal(t, nil, err)
			assert.Equal(t, len(buf2), n4)
			assert.Equal(t, "4567", string(buf2))
			wg.Done()
		}()
	}
	wg.Wait()
	reader.Close()
}

func TestFSClient_readAt(t *testing.T) {
	// 使用单个block的情况
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer func() {
		os.RemoveAll("./mock")
		os.RemoveAll("./mock-cache")
	}()
	d := cache.Config{
		BlockSize:    2,
		MaxReadAhead: 10,
		Expire:       10 * time.Second,
		Config: kv.Config{
			Driver:    kv.NutsDB,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client, err := newPfsTest()
	assert.Equal(t, err, nil)

	path := "testRead"
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	mode := 0666
	writer, err := client.Create(path, uint32(flags), uint32(mode))
	assert.Equal(t, err, nil)

	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, err, nil)
	writer.Close()

	var reader *File
	var buf []byte
	var n int

	n = 10
	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	buf = make([]byte, n)
	n, err = reader.ReadAt(buf, 2)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "st String ")
	buf = make([]byte, n)
	n, err = reader.ReadAt(buf, int64(n+2))
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "for Client")
	reader.Close()

	n = 2
	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	buf = make([]byte, n)
	n, err = reader.ReadAt(buf, 0)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "te")
	reader.Close()
}

func TestFSClient_readAtwithsmallBlock_2(t *testing.T) {
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer func() {
		os.RemoveAll("./mock")
		os.RemoveAll("./mock-cache")
	}()
	d := cache.Config{
		BlockSize:    3,
		MaxReadAhead: 100,
		Config: kv.Config{
			Driver: kv.NutsDB,
		},
	}
	SetDataCache(d)
	client, err := newPfsTest()
	assert.Equal(t, err, nil)

	path := "testRead"
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	mode := 0644
	writer, err := client.Create(path, uint32(flags), uint32(mode))
	assert.Equal(t, err, nil)

	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, err, nil)
	writer.Close()

	var reader *File
	var buf []byte
	var n int

	n = 5
	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	buf = make([]byte, 5)
	off := 2
	n, err = reader.ReadAt(buf, int64(off))
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "st St")

	buf = make([]byte, 5)
	n, err = reader.ReadAt(buf, int64(n+off))
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "ring ")

	buf = make([]byte, 10)
	n, err = reader.ReadAt(buf, int64(10+off))
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "for Client")
	reader.Close()

	n = 2
	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	buf = make([]byte, n)
	n, err = reader.ReadAt(buf, 0)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "te")
	reader.Close()
	// 等待1s，缓存写完成再删除mock-cache目录
	time.Sleep(1 * time.Second)

	err = os.RemoveAll("./mock")
	assert.Equal(t, err, nil)
	err = os.RemoveAll("./mock-cache")
	assert.Equal(t, err, nil)
}

func TestFSClient_readAtwithsmallBlock_1(t *testing.T) {
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")

	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 100,
		Expire:       10 * time.Second,
		Config: kv.Config{
			Driver:    kv.NutsDB,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)

	// new client
	client, err := newPfsTest()
	assert.Equal(t, err, nil)

	// 创建文件
	path := "testRead"
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	mode := 0644
	writer, err := client.Create(path, uint32(flags), uint32(mode))
	assert.Equal(t, err, nil)

	// 写文件
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, err, nil)
	writer.Close()

	var reader *File
	var buf []byte
	var n int

	n = 5
	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	buf = make([]byte, 5)
	off := 2
	n, err = reader.ReadAt(buf, int64(off))
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "st St")

	buf = make([]byte, 5)
	n, err = reader.ReadAt(buf, int64(n+off))
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "ring ")

	buf = make([]byte, 10)
	n, err = reader.ReadAt(buf, int64(10+off))
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "for Client")
	reader.Close()

	n = 2
	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	buf = make([]byte, n)
	n, err = reader.ReadAt(buf, 0)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "te")
	reader.Close()

	time.Sleep(1 * time.Second)

	err = os.RemoveAll("./mock")
	assert.Equal(t, err, nil)
	err = os.RemoveAll("./mock-cache")
	assert.Equal(t, err, nil)
}

func TestFSClient_readAtNotEnoughMem(t *testing.T) {
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")

	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 1,
		Expire:       0,
		Config: kv.Config{
			Driver:    kv.NutsDB,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)

	// new client
	client, err := newPfsTest()
	assert.Equal(t, err, nil)

	// 创建文件
	path := "testRead"
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	mode := 0644
	writer, err := client.Create(path, uint32(flags), uint32(mode))
	assert.Equal(t, err, nil)

	// 写文件
	writeString := "1234567890abcdefgh"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, err, nil)
	writer.Close()

	var reader *File
	var buf []byte
	var n int

	n = 5
	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	buf = make([]byte, 5)
	off := 2
	n, err = reader.ReadAt(buf, int64(off))
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "34567")
	p := setPoolNil()
	time.Sleep(1 * time.Second)

	buf = make([]byte, 5)
	n, err = reader.ReadAt(buf, 3)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, string(buf), "45678")
	p.Reset()

	buf = make([]byte, 10)
	n, err = reader.ReadAt(buf, 5)
	assert.Equal(t, 10, n)
	assert.Equal(t, "67890abcde", string(buf[:n]))
	reader.Close()

}
