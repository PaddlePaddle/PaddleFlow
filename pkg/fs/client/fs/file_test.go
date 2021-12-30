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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/vfs"
)

func newPfsTest() (*FileSystem, error) {
	os.MkdirAll("./mock", 0755)
	DiskCachePath = "./mock-cache"
	testFsMeta := base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}
	vfsConfig := vfs.InitConfig(
		vfs.WithMemorySize(MemCacheSize),
		vfs.WithMemoryExpire(MemCacheExpire),
		vfs.WithBlockSize(BlockSize),
		vfs.WithDiskExpire(DiskCacheExpire),
		vfs.WithDiskCachePath(DiskCachePath),
	)
	pfs, err := NewFileSystem(testFsMeta, nil, true, true, "", vfsConfig)
	if err != nil {
		return nil, err
	}
	return pfs, nil
}

func TestFSClient_readAt(t *testing.T) {
	// 使用单个block的情况
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
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

	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
}

func TestFSClient_readAtwithsmallBlock_2(t *testing.T) {
	// 使用多个block的情况
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")

	SetBlockSize(2)
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

	SetBlockSize(1)
	SetDiskCache("./mock-cache", 10)

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
