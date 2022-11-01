/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	pathlib "path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	cache "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/cache"
	kv "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv"
	meta "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/vfs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

func getTestFSClient(t *testing.T) FSClient {
	os.MkdirAll("./mock", 0755)
	testFsMeta := common.FSMeta{
		UfsType: common.LocalType,
		Properties: map[string]string{
			common.RootKey: "./mock",
		},
		SubPath: "./mock",
	}
	/**testFsMeta := base.FSMeta{
		UfsType: base.SFTPType,
		ServerAddress: "172.18.84.218:8001",
		Properties: map[string]string{
			"address": "172.18.84.218:8001",
			"user": "test",
			"password": "Test@123",
			"root": "/data",
		},
		SubPath: "/data",
	}**/
	DataCachePath = "./mock-cache"
	fsclient, err := NewFSClientForTest(testFsMeta)
	assert.Equal(t, nil, err)
	return fsclient
}

func TestNewFSClientForTest(t *testing.T) {
	os.MkdirAll("./mock", 0755)
	testFsMeta := common.FSMeta{
		UfsType: common.LocalType,
		Properties: map[string]string{
			common.RootKey: "./mock",
		},
	}
	_, err := NewFSClientForTest(testFsMeta)
	assert.Equal(t, nil, err)
}

func TestFSClient_bigBuf(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 4,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)

	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, nil, err)
	writer.Close()

	buf := make([]byte, 500)
	n, err := openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, 22)
}

func TestOpenWriteMetaConsistence(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		Expire: 1 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)
	writeString := "123"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, nil, err)

	fInfo, err := client.Stat(path)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(3), fInfo.Size())
	writeString = "456"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, nil, err)
	time.Sleep(2 * time.Second)

	fInfo, err = client.Stat(path)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(6), fInfo.Size())

}

func TestFSClient_case1(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 4,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	newPath := "/mock/test1"
	newDir1 := "/mock/Dir1"
	newDir2 := "mock/Dir2/Dir1"
	newDir3 := "/mock/Dir3"
	newDir4 := "/mock/renamedir"
	err := client.MkdirAll(newDir2, 0755)
	assert.Equal(t, nil, err)
	err = client.Mkdir(newDir3, 0755)
	assert.Equal(t, nil, err)
	file, err := client.Create(newPath)
	assert.Equal(t, nil, err)
	assert.NotNil(t, file)
	file.Write([]byte("test create file"))
	file.Write([]byte("test append write"))
	file.Close()
	newFile, err := client.Open(newPath)
	assert.Equal(t, nil, err)
	assert.NotNil(t, newFile)
	buffer := make([]byte, 200)
	n, err := newFile.Read(buffer)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, 33)
	assert.Equal(t, "test create filetest append write", string(buffer[0:n]))
	newFile.Close()
	newFile, err = client.Open(newPath)
	assert.Equal(t, nil, err)
	assert.NotNil(t, newFile)
	buffer = make([]byte, 10)
	context, err := ioutil.ReadAll(newFile)
	assert.Equal(t, nil, err)
	assert.Equal(t, "test create filetest append write", string(context))
	newFile.Close()
	err = client.Mkdir(newDir1, 0755)
	assert.Equal(t, nil, err)
	err = client.Chmod(newDir1, 0777)
	assert.Equal(t, nil, err)
	err = client.Chown(newDir1, 601, 601)
	assert.Equal(t, nil, err)
	err = client.Rename(newDir1, newDir4)
	assert.Equal(t, nil, err)
	err = client.Chmod(newDir4, 0755)
	assert.Equal(t, nil, err)
	dirs, err := client.Readdirnames("/mock", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, len(dirs))

	_, err = client.IsDir(newDir4)
	assert.Equal(t, nil, err)
	dirInfos, err := client.ListDir("/mock")
	assert.Equal(t, nil, err)
	assert.Equal(t, len(dirInfos), 4)
	isEmpty, err := client.IsEmptyDir(newDir2)
	assert.Equal(t, nil, err)
	assert.Equal(t, isEmpty, true)
	isEmpty, err = client.IsEmptyDir("/mock")
	assert.Equal(t, nil, err)
	assert.Equal(t, isEmpty, false)
	err = client.Remove(newDir3)
	assert.Equal(t, nil, err)
	exist, err := client.Exist(newDir3)
	assert.Equal(t, nil, err)
	assert.Equal(t, exist, false)
	size, err := client.Size(newPath)
	assert.Equal(t, nil, err)
	assert.Equal(t, size, int64(33))
	copyDir := "/mock1"
	err = client.Mkdir(copyDir, 0755)
	assert.Equal(t, nil, err)
	err = client.Copy("/mock", copyDir)
	assert.Equal(t, nil, err)
	copyDir1 := "/mock2"
	err = client.Copy("/mock/test1", copyDir1)
	assert.Equal(t, nil, err)
	err = client.RemoveAll(copyDir1)
	assert.Equal(t, nil, err)
	err = client.RemoveAll(copyDir)
	assert.Equal(t, nil, err)
	srcbuffer := strings.NewReader("test save file")
	err = client.SaveFile(srcbuffer, "/mock", "test2")
	assert.Equal(t, nil, err)
	n, err = client.CreateFile("/mock/test3", []byte("test create file: test3"))
	assert.Equal(t, nil, err)
	assert.Equal(t, 23, n)
	info, err := client.Stat("/mock/test3")
	assert.Equal(t, nil, err)
	assert.Equal(t, info.IsDir(), false)
}

func TestFSClient_read(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    400,
		MaxReadAhead: 4,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, nil, err)
	writer.Close()

	var reader io.ReadCloser
	var buf []byte
	var n int

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)

	reader, err = client.Open(path)
	assert.Equal(t, nil, err)
	readn := 5
	buf1 := make([]byte, readn)
	buf2 := make([]byte, len([]byte(writeString))-readn)
	n1, err := reader.Read(buf1)
	n2, err := reader.Read(buf2)
	assert.Equal(t, nil, err)
	assert.Equal(t, n1+n2, len([]byte(writeString)))
	assert.Equal(t, writeString, string(buf1)+string(buf2))
	reader.Close()
	time.Sleep(1 * time.Second)
}

func BenchmarkMemCachedRead(b *testing.B) {
	clean()
	defer clean()
	testFsMeta := common.FSMeta{
		UfsType: common.LocalType,
		Properties: map[string]string{
			common.RootKey: "./mock",
		},
		SubPath: "./mock",
	}

	d := cache.Config{
		BlockSize:    1 << 23,
		MaxReadAhead: 100,
		Config: kv.Config{
			Driver: kv.MemType,
		},
	}
	SetDataCache(d)

	DataCachePath = "./mock-cache"
	client, err := NewFSClientForTest(testFsMeta)
	if err != nil {
		b.Fatalf("new client fail %v", err)
	}

	path := "testRead"
	writer, err := client.Create(path)
	if err != nil {
		b.Fatalf("create client fail %v", err)
	}
	n := 1000
	writeString := getRandomString(n)
	_, err = writer.Write([]byte(writeString))
	if err != nil {
		b.Fatalf("write client fail %v", err)
	}
	buf := make([]byte, n)
	_, err = openAndRead(client, path, buf)
	if err != nil {
		b.Fatalf("openAndRead fail %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = make([]byte, n)
		_, err = openAndRead(client, path, buf)
		if err != nil {
			b.Fatalf("openAndRead fail %v", err)
		}
	}

}

func BenchmarkUnCachedRead(b *testing.B) {
	clean()
	defer clean()
	testFsMeta := common.FSMeta{
		UfsType: common.LocalType,
		Properties: map[string]string{
			common.RootKey: "./mock",
		},
		SubPath: "./mock",
	}
	d := cache.Config{
		BlockSize:    0,
		MaxReadAhead: 0,
	}
	SetDataCache(d)
	DataCachePath = "./mock-cache"
	client, err := NewFSClientForTest(testFsMeta)
	if err != nil {
		b.Fatalf("new client fail %v", err)
	}

	path := "testRead"
	writer, err := client.Create(path)
	if err != nil {
		b.Fatalf("create client fail %v", err)
	}
	n := 1000
	writeString := getRandomString(n)
	_, err = writer.Write([]byte(writeString))
	if err != nil {
		b.Fatalf("write client fail %v", err)
	}
	buf := make([]byte, n)
	_, err = openAndRead(client, path, buf)
	if err != nil {
		b.Fatalf("openAndRead fail %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = make([]byte, n)
		_, err = openAndRead(client, path, buf)
		if err != nil {
			b.Fatalf("openAndRead fail %v", err)
		}
	}
}

// 生成随机字符串
func getRandomString(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

func openAndRead(client FSClient, path string, buf []byte) (int, error) {
	reader, err := client.Open(path)
	if err != nil {
		log.Errorf("openAndRead open err %v", err)
		return 0, err
	}
	n, err := reader.Read(buf)
	if err != nil {
		log.Errorf("openAndRead read err %v", err)
		return 0, err
	}
	err = reader.Close()
	if err != nil {
		log.Errorf("openAndRead close err %v", err)
		return 0, err
	}
	return n, nil
}

func TestPoolOK(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    3,
		MaxReadAhead: 20 * 1024 * 1024,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)
	writeString := "123456789"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, nil, err)
	writer.Close()

	var buf []byte
	var n int

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)

	buf2 := make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf2)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf2))
	assert.Equal(t, string(buf2), writeString)
}

func TestFSClient_read_with_small_block_1(t *testing.T) {
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 2,
		Expire:       10 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	clean()
	defer clean()

	pathReal := "../../../../example/hoursing_price/run.yaml"
	bufLen := 1000
	bufExpect := make([]byte, bufLen)
	nExpect, err := readFile(pathReal, bufExpect)
	assert.Equal(t, nil, err)

	DataCachePath = "./mock-cache"
	client := getTestFSClient(t)

	path := "testRead_small_1"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)

	_, err = writer.Write(bufExpect[0:nExpect])
	assert.Equal(t, nil, err)
	writer.Close()

	buf := make([]byte, bufLen)
	n, err := openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, nExpect, n)
	assert.Equal(t, string(bufExpect), string(buf))

	buf = make([]byte, bufLen)
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, nExpect, n)
	assert.Equal(t, string(bufExpect), string(buf))

	reader, err := client.Open(path)
	assert.Equal(t, nil, err)
	readn := 5
	buf1 := make([]byte, readn)
	buf2 := make([]byte, nExpect-readn)
	n1, err := reader.Read(buf1)
	n2, err := reader.Read(buf2)
	assert.Equal(t, nil, err)
	assert.Equal(t, nExpect, n1+n2)
	reader.Close()

	assert.Equal(t, string(bufExpect[0:nExpect]), string(buf1)+string(buf2))
}

func TestFSClient_read_with_small_block_2(t *testing.T) {
	d := cache.Config{
		BlockSize:    2,
		MaxReadAhead: 4,
		Expire:       10 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	clean()
	defer clean()

	pathReal := "../../../../example/hoursing_price/run.yaml"
	bufLen := 1000
	bufExpect := make([]byte, bufLen)
	nExpect, err := readFile(pathReal, bufExpect)
	assert.Equal(t, nil, err)

	DataCachePath = "./mock-cache"
	client := getTestFSClient(t)

	path := "testRead_small_1"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)

	_, err = writer.Write(bufExpect[0:nExpect])
	assert.Equal(t, nil, err)
	writer.Close()

	buf := make([]byte, bufLen)
	n, err := openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, nExpect, n)
	assert.Equal(t, string(bufExpect), string(buf))
	time.Sleep(3 * time.Second)

	buf = make([]byte, bufLen)
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, nExpect, n)
	assert.Equal(t, string(bufExpect), string(buf))

	reader, err := client.Open(path)
	assert.Equal(t, nil, err)
	readn := 5
	buf1 := make([]byte, readn)
	buf2 := make([]byte, nExpect-readn)
	n1, err := reader.Read(buf1)
	n2, err := reader.Read(buf2)
	reader.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, nExpect, n1+n2)

	assert.Equal(t, string(bufExpect[0:nExpect]), string(buf1)+string(buf2))
	time.Sleep(1 * time.Second)
}

func readFile(path string, buf []byte) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	return file.Read(buf)
}

func TestFSClient_diskCache_Read(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    200,
		MaxReadAhead: 4,
		Expire:       10 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)

	DataCachePath = "./mock-cache"
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, nil, err)
	writer.Close()

	// var reader io.ReadCloser
	var buf []byte
	var n int

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(1 * time.Second)

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(1 * time.Second)

	// diskBlocksize = 1
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	d = cache.Config{
		BlockSize:    1,
		MaxReadAhead: 4,
		Expire:       10 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client = getTestFSClient(t)
	path = "testRead"
	writer, err = client.Create(path)
	assert.Equal(t, nil, err)
	writeString = "test String for Client"
	_, err = writer.Write([]byte(writeString))
	writer.Close()
	assert.Equal(t, nil, err)

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(3 * time.Second)

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(3 * time.Second)
}

func TestFSClient_diskCache_Read_Expire(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 4,
		Expire:       6 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, nil, err)
	writer.Close()

	buf := make([]byte, len([]byte(writeString)))
	n, err := openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)

	time.Sleep(2 * time.Second)
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
}

func TestFSClient_diskCache_Read_Full(t *testing.T) {
	// 缓存满了看下发生什么
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer os.RemoveAll("./mock")
	defer os.RemoveAll("./mock-cache")
	d := cache.Config{
		BlockSize:    1 << 23,
		MaxReadAhead: 4,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, nil, err)
	writer.Close()

	buf := make([]byte, len([]byte(writeString)))
	n, err := openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)

	time.Sleep(2 * time.Second)
	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(2 * time.Second)

	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
}

func TestFSClient_cache_read(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    5,
		MaxReadAhead: 4,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, nil, err)
	writer.Close()

	buf := make([]byte, len([]byte(writeString)))
	n, err := openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)

	time.Sleep(2 * time.Second)
	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(2 * time.Second)

	path2 := "test2"
	client.Rename(path, path2)
	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path2, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(2 * time.Second)

	testMvString := "123456789"
	client.CreateFile(path, []byte(testMvString))
	buf = make([]byte, len([]byte(testMvString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), testMvString)

	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
}

func TestFSClient_Concurrent_Read(t *testing.T) {
	// 缓存满了看下发生什么
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    5,
		MaxReadAhead: 4,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)
	writeString := "abcdefgghhhdfd123124125"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, nil, err)
	writer.Close()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, len([]byte(writeString)))
			n, err := openAndRead(client, path, buf)
			assert.Equal(t, nil, err)
			assert.Equal(t, n, len(buf))
			assert.Equal(t, string(buf), writeString)
		}()
	}
	wg.Wait()
}

func TestMetaEntryCache(t *testing.T) {
	clean()
	defer clean()
	client := getTestFSClient2(t)
	newDir := "/Dir/dir1"
	err := client.MkdirAll(newDir, 0755)
	assert.Equal(t, nil, err)
	_, err = client.Create("/Dir/file1")
	assert.Equal(t, nil, err)
	list, err := client.Readdirnames("/Dir", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(list))
	list, err = client.Readdirnames("/Dir", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(list))
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			list, err = client.Readdirnames("/Dir", -1)
			defer wg.Done()
		}()
	}
	wg.Wait()

	newDir2 := "/Dir/dir2"
	err = client.MkdirAll(newDir2, 0755)
	assert.Equal(t, nil, err)
	_, err = client.Create("/Dir/file2")
	assert.Equal(t, nil, err)
	list, err = client.Readdirnames("/Dir", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, len(list))

	err = client.Remove("/Dir/file2")
	assert.Equal(t, nil, err)
	err = client.Remove(newDir2)
	assert.Equal(t, nil, err)
	err = client.Rename("/Dir/file1", "/Dir/file3")
	list, err = client.Readdirnames("/Dir", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(list))

	client.MkdirAll("/Dir2", 0755)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		dir := "/Dir2/dir" + strconv.Itoa(i)
		file := "/Dir2/file" + strconv.Itoa(i)
		go func() {
			defer wg.Done()
			err = client.MkdirAll(dir, 0755)
			_, err = client.Create(file)
			client.Rename(file, file+"_")
		}()
	}
	wg.Wait()
	list, err = client.Readdirnames("/Dir2", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 20, len(list))
}

func TestMetaAttrCache(t *testing.T) {
	clean()
	defer clean()
	client := getTestFSClient2(t)
	newDir := "/Dir"
	err := client.MkdirAll(newDir, 0755)
	assert.Equal(t, nil, err)

	file1 := "/Dir/file1"
	file2 := "/Dir/file2"
	newFile, err := client.Create(file1)
	assert.Equal(t, nil, err)
	assert.NotNil(t, newFile)
	w := []byte("123456789")
	n, err := newFile.Write(w)
	assert.Equal(t, nil, err)
	assert.Equal(t, 9, n)
	stat, err := client.Stat(file1)
	assert.Equal(t, nil, err)
	assert.Equal(t, "file1", stat.Name())
	assert.Equal(t, int64(9), stat.Size())
	err = client.Rename(file1, file2)
	assert.Equal(t, nil, err)
	stat, err = client.Stat(file2)
	assert.Equal(t, nil, err)
	assert.Equal(t, "file2", stat.Name())
	assert.Equal(t, int64(9), stat.Size())
	stat, err = client.Stat(file1)
	assert.Equal(t, nil, stat)
}

func NewFSClientForTestWithNoClientCache(fsMeta common.FSMeta) (*PFSClient, error) {
	vfsConfig := vfs.InitConfig(
		vfs.WithDataCacheConfig(cache.Config{
			BlockSize:    BlockSize,
			MaxReadAhead: MaxReadAheadNum,
			Expire:       DataCacheExpire,
			Config: kv.Config{
				Driver:    kv.MemType,
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
	pfs, err := NewFileSystem(fsMeta, nil, true, false, "", vfsConfig)
	if err != nil {
		return nil, err
	}

	client := PFSClient{}
	client.pfs = pfs
	return &client, nil
}

func getTestFSClient2(t *testing.T) FSClient {
	os.MkdirAll("./mock", 0755)
	testFsMeta := common.FSMeta{
		UfsType: common.LocalType,
		Properties: map[string]string{
			common.RootKey: "./mock",
		},
		SubPath: "./mock",
	}
	DataCachePath = "./mock-cache"
	fsclient, err := NewFSClientForTestWithNoClientCache(testFsMeta)
	assert.Equal(t, nil, err)
	return fsclient
}

func TestMeta(t *testing.T) {
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer os.RemoveAll("./mock")
	defer os.RemoveAll("./mock-cache")
	db := "./level.db"
	os.RemoveAll(db)
	defer os.RemoveAll(db)
	var err error
	SetMetaCache(meta.Config{
		AttrCacheExpire:  100 * time.Second,
		EntryCacheExpire: 100 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: db,
		},
	})
	client := getTestFSClient2(t)

	newDir := "/one/ond-dire1"
	err = client.MkdirAll(newDir, 0755)
	assert.Equal(t, nil, err)
	_, err = client.Create("/one/file1")
	assert.Equal(t, nil, err)
	list, err := client.Readdirnames("/one", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(list))
	list, err = client.Readdirnames("/one", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(list))
	assert.NotEqual(t, list[0], list[1])
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			list, err = client.Readdirnames("/one", -1)
			defer wg.Done()
		}()
	}
	wg.Wait()

	newDir2 := "/one/dir2"
	err = client.MkdirAll(newDir2, 0755)
	assert.Equal(t, nil, err)
	_, err = client.Create("/one/file2")
	assert.Equal(t, nil, err)
	list, err = client.Readdirnames("/one", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, len(list))

	err = client.Remove("/one/file2")
	assert.Equal(t, nil, err)
	err = client.Remove(newDir2)
	assert.Equal(t, nil, err)
	err = client.Rename("/one/file1", "/one/file3")
	list, err = client.Readdirnames("/one", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(list))

	n := 50
	err = client.MkdirAll("/twice", 0755)
	for i := 0; i < n; i++ {
		wg.Add(1)
		dir := "/twice/dir" + strconv.Itoa(i)
		file := "/twice/file" + strconv.Itoa(i)
		go func() {
			defer wg.Done()
			err = client.MkdirAll(dir, 0755)
			if err != nil {
				log.Errorf("mkdir err %v", err)
			}
			_, err = client.Create(file)
			if err != nil {
				log.Errorf("Create err %v", err)
			}
			err = client.Rename(file, file+"_")
			if err != nil {
				log.Errorf("Rename err %v", err)
			}
		}()
	}
	wg.Wait()
	list, err = client.Readdirnames("/twice", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2*n, len(list))

	list, err = client.Readdirnames("/twice", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2*n, len(list))

	dir3 := "/dir3"
	err = client.MkdirAll(dir3, 0755)
	assert.Equal(t, nil, err)
	info, err := client.Stat(dir3)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, info.IsDir())
	assert.Equal(t, info.Name(), "dir3")

	path1 := "/abc"
	_, err = client.CreateFile(dir3+path1, []byte("123456"))
	assert.Equal(t, nil, err)
	info, err = client.Stat(dir3 + path1)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(6), info.Size())
	assert.Equal(t, "abc", info.Name())
	assert.Equal(t, info.IsDir(), false)
}

func setPoolNil() *gomonkey.Patches {
	var p *cache.BufferPool
	patch := gomonkey.ApplyMethod(reflect.TypeOf(p), "RequestMBuf", func(_ *cache.BufferPool, size uint64, block bool, blockSize int) (buf []byte) {
		return nil
	})
	return patch
}

func TestReadWithNotEnoughMem(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    5,
		MaxReadAhead: 4,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver: kv.MemType,
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, nil, err)
	writer.Close()

	buf := make([]byte, len([]byte(writeString)))
	n, err := openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)

	time.Sleep(2 * time.Second)
	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(2 * time.Second)
	p := setPoolNil()
	defer func() {
		p.Reset()
	}()
	path2 := "test2"
	client.Rename(path, path2)
	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path2, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(2 * time.Second)

	testMvString := "123456789"
	client.CreateFile(path, []byte(testMvString))
	buf = make([]byte, len([]byte(testMvString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), testMvString)

	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
}

func TestFSClient_read_with_small_block_1_not_enough_mem(t *testing.T) {
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 2,
		Expire:       10 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	clean()
	defer clean()

	pathReal := "../../../../example/hoursing_price/run.yaml"
	bufLen := 1000
	bufExpect := make([]byte, bufLen)
	nExpect, err := readFile(pathReal, bufExpect)
	assert.Equal(t, nil, err)

	DataCachePath = "./mock-cache"
	client := getTestFSClient(t)

	path := "testRead_small_1"
	writer, err := client.Create(path)
	assert.Equal(t, nil, err)

	_, err = writer.Write(bufExpect[0:nExpect])
	assert.Equal(t, nil, err)
	_ = writer.Close()

	buf := make([]byte, bufLen)
	n, err := openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, nExpect, n)
	assert.Equal(t, string(bufExpect), string(buf))

	buf = make([]byte, bufLen)
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, nExpect, n)
	assert.Equal(t, string(bufExpect), string(buf))

	p := setPoolNil()
	defer func() {
		p.Reset()
	}()
	reader, err := client.Open(path)
	assert.Equal(t, nil, err)
	readn := 5
	buf1 := make([]byte, readn)
	buf2 := make([]byte, nExpect-readn)
	n1, err := reader.Read(buf1)
	n2, err := reader.Read(buf2)
	assert.Equal(t, nil, err)
	assert.Equal(t, nExpect, n1+n2)
	_ = reader.Close()

	assert.Equal(t, string(bufExpect[0:nExpect]), string(buf1)+string(buf2))
}

func TestParentTimeWithCache(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 4,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	newDir1 := "/dir/mock1"
	err := client.MkdirAll(newDir1, 0755)
	assert.Equal(t, nil, err)
	stat1_1, err := client.Stat(newDir1)
	assert.Equal(t, nil, err)
	newDir2 := "/dir/mock1/mock2"
	err = client.Mkdir(newDir2, 0755)
	assert.Equal(t, nil, err)
	stat1, err := client.Stat(newDir1)
	assert.Equal(t, nil, err)
	stat2, err := client.Stat(newDir2)
	assert.Equal(t, nil, err)

	if !stat1.ModTime().After(stat1_1.ModTime()) {
		t.Errorf("modify time not correct stat1[%v], stat1_1[%v]", stat1.ModTime(), stat1_1.ModTime())
		return
	}
	if !stat1.ModTime().Equal(stat2.ModTime()) {
		t.Errorf("stat1 time %v not equal stat2 time %v", stat1.ModTime(), stat2.ModTime())
		return
	}
	listDirs, err := client.ListDir(newDir1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(listDirs))
	assert.Equal(t, "mock2", listDirs[0].Name())

	file1 := "/dir/mock1/file1"
	file, err := client.Create(file1)
	assert.Equal(t, nil, err)
	assert.NotNil(t, file)
	_, _ = file.Write([]byte("test create file"))
	_, _ = file.Write([]byte("test append write"))
	_ = file.Close()
	newFile, err := client.Open(file1)
	assert.Equal(t, nil, err)
	assert.NotNil(t, newFile)
	buffer := make([]byte, 200)
	n, err := newFile.Read(buffer)
	assert.Equal(t, nil, err)
	assert.Equal(t, n, 33)
	assert.Equal(t, "test create filetest append write", string(buffer[0:n]))
	newFile.Close()
	newFile, err = client.Open(file1)
	assert.Equal(t, nil, err)
	assert.NotNil(t, newFile)
	buffer = make([]byte, 10)
	context, err := ioutil.ReadAll(newFile)
	assert.Equal(t, nil, err)
	assert.Equal(t, "test create filetest append write", string(context))

	stat1, err = client.Stat(newDir1)
	assert.Equal(t, nil, err)
	stat2, err = client.Stat(newDir2)
	assert.Equal(t, nil, err)
	if !stat1.ModTime().After(stat2.ModTime()) {
		t.Errorf("stat1 time %v not after stat2 time %v", stat1.ModTime(), stat2.ModTime())
		return
	}
	listDirs, err = client.ListDir(newDir1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(listDirs))

	err = client.Remove(file1)
	assert.Equal(t, nil, err)
	stat1_2, err := client.Stat(newDir1)
	assert.Equal(t, nil, err)
	if !stat1_2.ModTime().After(stat1.ModTime()) {
		t.Errorf("stat1_2 time %v not after stat1 time %v", stat1_2.ModTime(), stat1.ModTime())
		return
	}
	listDirs, err = client.ListDir(newDir1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(listDirs))

	dstPath := "/dir2"
	err = client.Rename(newDir1, dstPath)
	assert.Equal(t, nil, err)

	listDirs, err = client.ListDir(newDir1)
	assert.Equal(t, syscall.ENOENT, err)
	listDirs, err = client.ListDir(dstPath)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(listDirs))
	assert.Equal(t, "mock2", listDirs[0].Name())
}

func TestParentTimeWithCacheExpired(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 4,
		Expire:       0 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	newDir1 := "/dir/mock1"
	err := client.MkdirAll(newDir1, 0755)
	assert.Equal(t, nil, err)
	stat1_1, err := client.Stat(newDir1)
	assert.Equal(t, nil, err)
	newDir2 := "/dir/mock1/mock2"
	err = client.Mkdir(newDir2, 0755)
	assert.Equal(t, nil, err)
	stat1, err := client.Stat(newDir1)
	assert.Equal(t, nil, err)
	stat2, err := client.Stat(newDir2)
	assert.Equal(t, nil, err)

	if !stat1.ModTime().After(stat1_1.ModTime()) {
		t.Errorf("modify time not correct stat1[%v], stat1_1[%v]", stat1.ModTime(), stat1_1.ModTime())
		return
	}
	if !stat1.ModTime().Equal(stat2.ModTime()) {
		t.Errorf("stat1 time %v not equal stat2 time %v", stat1.ModTime(), stat2.ModTime())
		return
	}
	listDirs, err := client.ListDir(newDir1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(listDirs))
	assert.Equal(t, "mock2", listDirs[0].Name())

	dstPath := "/dir2"
	err = client.Rename(newDir1, dstPath)
	assert.Equal(t, nil, err)

	listDirs, err = client.ListDir(newDir1)
	assert.Equal(t, syscall.ENOENT, err)
}

func TestRename(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 4,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	newDir := "/dir/a/b/c/"
	err := client.MkdirAll(newDir, 0755)
	assert.Equal(t, nil, err)
	stat1_c_1, err := client.Stat(newDir)
	assert.Equal(t, nil, err)
	file1 := "/dir/a/b/c/file1"
	file, err := client.Create(file1)
	assert.Equal(t, nil, err)
	assert.NotNil(t, file)
	stat1_c_2, err := client.Stat(newDir)
	assert.Equal(t, nil, err)
	if !stat1_c_2.ModTime().After(stat1_c_1.ModTime()) {
		t.Errorf("modify time not correct stat1_c_1[%v], stat1_c_2[%v]", stat1_c_1.ModTime(), stat1_c_2.ModTime())
		return
	}
	err = client.Mkdir(pathlib.Join(newDir, "d"), 0777)
	assert.Equal(t, nil, err)
	stat1_c_3, err := client.Stat(newDir)
	assert.Equal(t, nil, err)
	if !stat1_c_3.ModTime().After(stat1_c_2.ModTime()) {
		t.Errorf("modify time not correct stat1_c_2[%v], stat1_c_3[%v]", stat1_c_2.ModTime(), stat1_c_3.ModTime())
		return
	}

	file2 := pathlib.Join(newDir, "d", "file2")
	file, err = client.Create(file2)
	assert.Equal(t, nil, err)
	assert.NotNil(t, file)

	file3 := pathlib.Join(newDir, "d", "file3")
	file, err = client.Create(file3)
	assert.Equal(t, nil, err)
	assert.NotNil(t, file)

	list, err := client.ListDir(newDir)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(list))

	list2, err := client.ListDir(pathlib.Join(newDir, "d"))
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(list2))

	err = client.Rename("/dir/a/b/c/file1", "/dir/a/b/c/file2")
	assert.Equal(t, nil, err)
	stat1_c_4, err := client.Stat(newDir)
	assert.Equal(t, nil, err)
	if !stat1_c_4.ModTime().After(stat1_c_3.ModTime()) {
		t.Errorf("modify time not correct stat1_c_3[%v], stat1_c_4[%v]", stat1_c_3.ModTime(), stat1_c_4.ModTime())
		return
	}

	err = client.Rename("/dir/a/b/c", "/dir/a/b/f")
	assert.Equal(t, nil, err)

	list3, err := client.ListDir("/dir/a/b/f")
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(list3))

	list4, err := client.ListDir("/dir/a/b/f/d")
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(list4))

	_, err = client.ListDir("/dir/a/b/c")
	assert.Equal(t, syscall.ENOENT, err)

	_, err = client.ListDir("/dir/a/b/c/d")
	assert.Equal(t, syscall.ENOENT, err)
}

func TestConcurrentCreateAndDelete(t *testing.T) {
	clean()
	defer clean()
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 4,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getTestFSClient(t)
	newDir1 := "/foo/"
	err := client.MkdirAll(newDir1, 0777)
	assert.Equal(t, nil, err)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		newFile := filepath.Join(newDir1, strconv.Itoa(i))
		go func() {
			defer wg.Done()
			file, err := client.Create(newFile)
			// parent meta == nil:syscall.ENOENT, dir is deleted: syscall.ENOTDIR
			if err != nil && err != syscall.ENOENT && err != syscall.ENOTDIR {
				t.Errorf("client create err %v", err)
				return
			}
			if err != nil {
				return
			}
			_ = file.Close()
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		errRemove := client.RemoveAll(newDir1)
		// removeall == unlink + rmdir, if unlink all, but when create command occurs after unlink, rmdir has error `directory not empty`
		if errRemove != nil && errRemove != syscall.ENOTEMPTY {
			assert.Equal(t, nil, errRemove)
		}
	}()
	wg.Wait()
}

func clean() {
	_ = os.RemoveAll("./mock")
	_ = os.RemoveAll("./mock-cache")
}
