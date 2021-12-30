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
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/fs/client/base"
)

func getTestFSClient(t *testing.T) FSClient {
	os.MkdirAll("./mock", 0755)
	testFsMeta := base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
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
	DiskCachePath = "./mock-cache"
	fsclient, err := NewFSClientForTest(testFsMeta)
	assert.Equal(t, err, nil)
	return fsclient
}

func TestNewFSClientForTest(t *testing.T) {
	os.MkdirAll("./mock", 0755)
	testFsMeta := base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
	}
	_, err := NewFSClientForTest(testFsMeta)
	assert.Equal(t, err, nil)
}

func TestFSClient_case1(t *testing.T) {
	os.RemoveAll("./mock")
	os.RemoveAll("./mock1")
	os.RemoveAll("./mock")
	client := getTestFSClient(t)
	newPath := "/mock/test1"
	newDir1 := "/mock/Dir1"
	newDir2 := "mock/Dir2/Dir1"
	newDir3 := "/mock/Dir3"
	newDir4 := "/mock/renamedir"
	err := client.MkdirAll(newDir2, 0755)
	assert.Equal(t, err, nil)
	err = client.Mkdir(newDir3, 0755)
	assert.Equal(t, err, nil)
	file, err := client.Create(newPath)
	assert.Equal(t, err, nil)
	assert.NotNil(t, file)
	file.Write([]byte("test create file"))
	file.Write([]byte("test append write"))
	file.Close()
	newFile, err := client.Open(newPath)
	assert.Equal(t, err, nil)
	assert.NotNil(t, newFile)
	buffer := make([]byte, 200)
	n, err := newFile.Read(buffer)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, 33)
	assert.Equal(t, "test create filetest append write", string(buffer[0:n]))
	newFile.Close()
	newFile, err = client.Open(newPath)
	assert.Equal(t, err, nil)
	assert.NotNil(t, newFile)
	buffer = make([]byte, 10)
	context, err := ioutil.ReadAll(newFile)
	assert.Equal(t, err, nil)
	assert.Equal(t, "test create filetest append write", string(context))
	newFile.Close()
	err = client.Mkdir(newDir1, 0755)
	assert.Equal(t, err, nil)
	err = client.Chmod(newDir1, 0777)
	assert.Equal(t, err, nil)
	err = client.Rename(newDir1, newDir4)
	assert.Equal(t, err, nil)
	err = client.Chmod(newDir4, 0755)
	assert.Equal(t, err, nil)
	dirs, err := client.Readdirnames("/mock", -1)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(dirs), 4)
	_, err = client.IsDir(newDir4)
	assert.Equal(t, err, nil)
	dirInfos, err := client.ListDir("/mock")
	assert.Equal(t, err, nil)
	assert.Equal(t, len(dirInfos), 4)
	isEmpty, err := client.IsEmptyDir(newDir2)
	assert.Equal(t, err, nil)
	assert.Equal(t, isEmpty, true)
	isEmpty, err = client.IsEmptyDir("/mock")
	assert.Equal(t, err, nil)
	assert.Equal(t, isEmpty, false)
	err = client.Remove(newDir3)
	assert.Equal(t, err, nil)
	exist, err := client.Exist(newDir3)
	assert.Equal(t, err, nil)
	assert.Equal(t, exist, false)
	size, err := client.Size(newPath)
	assert.Equal(t, err, nil)
	assert.Equal(t, size, int64(33))
	copyDir := "/mock1"
	err = client.Mkdir(copyDir, 0755)
	assert.Equal(t, err, nil)
	err = client.Copy("/mock", copyDir)
	assert.Equal(t, err, nil)
	copyDir1 := "/mock2"
	err = client.Copy("/mock/test1", copyDir1)
	assert.Equal(t, err, nil)
	err = client.RemoveAll(copyDir1)
	assert.Equal(t, err, nil)
	err = client.RemoveAll(copyDir)
	assert.Equal(t, err, nil)
	srcbuffer := strings.NewReader("test save file")
	err = client.SaveFile(srcbuffer, "/mock", "test2")
	assert.Equal(t, err, nil)
	n, err = client.CreateFile("/mock/test3", []byte("test create file: test3"))
	assert.Equal(t, err, nil)
	assert.Equal(t, 23, n)
	info, err := client.Stat("/mock/test3")
	assert.Equal(t, err, nil)
	assert.Equal(t, info.IsDir(), false)

	os.RemoveAll("./mock")
	os.RemoveAll("./mock1")
	os.RemoveAll("./mock-cache")
}

func TestFSClient_read(t *testing.T) {
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer os.RemoveAll("./mock-cache")
	defer os.RemoveAll("./mock")

	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, err, nil)
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, err, nil)
	writer.Close()

	var reader io.ReadCloser
	var buf []byte
	var n int

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)

	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	readn := 5
	buf1 := make([]byte, readn)
	buf2 := make([]byte, len([]byte(writeString))-readn)
	n1, err := reader.Read(buf1)
	n2, err := reader.Read(buf2)
	assert.Equal(t, err, nil)
	assert.Equal(t, n1+n2, len([]byte(writeString)))
	reader.Close()
	time.Sleep(1 * time.Second)
}

func BenchmarkMemCachedRead(b *testing.B) {
	os.RemoveAll("./mock")
	os.MkdirAll("./mock", 0755)
	defer os.RemoveAll("./mock")
	defer os.RemoveAll("./mock-cache")
	testFsMeta := base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}
	SetBlockSize(1 << 23)
	DiskCachePath = "./mock-cache"
	client, err := NewFSClientForTest(testFsMeta)
	if err != nil {
		b.Fatalf("new client fail %v", err)
	}

	path := "testRead"
	writer, err := client.Create(path)
	if err != nil {
		b.Fatalf("create client fail %v", err)
	}
	n := 100
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
	os.RemoveAll("./mock")
	os.MkdirAll("./mock", 0755)
	defer os.RemoveAll("./mock")
	defer os.RemoveAll("./mock-cache")
	testFsMeta := base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}
	SetBlockSize(0)

	DiskCachePath = "./mock-cache"
	client, err := NewFSClientForTest(testFsMeta)
	if err != nil {
		b.Fatalf("new client fail %v", err)
	}

	path := "testRead"
	writer, err := client.Create(path)
	if err != nil {
		b.Fatalf("create client fail %v", err)
	}
	n := 100
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
		return 0, err
	}
	n, err := reader.Read(buf)
	if err != nil {
		return 0, err
	}
	err = reader.Close()
	if err != nil {
		return 0, err
	}
	return n, nil
}

func TestFSClient_read_with_small_block_1(t *testing.T) {
	SetBlockSize(1)
	SetDiskCache("./mock-cache", 10)
	os.MkdirAll("./mock", 0755)
	os.MkdirAll("./mock-cache", 0755)
	defer os.RemoveAll("./mock")
	defer os.RemoveAll("./mock-cache")

	pathReal := "../../../../example/hoursing_price/run.yaml"
	bufLen := 1000
	bufExpect := make([]byte, bufLen)
	nExpect, err := readFile(pathReal, bufExpect)
	assert.Equal(t, nil, err)

	DiskCachePath = "./mock-cache"
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
	assert.Equal(t, err, nil)
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
	SetBlockSize(2)

	defer os.RemoveAll("./mock")
	defer os.RemoveAll("./mock-cache")

	pathReal := "../../../../example/hoursing_price/run.yaml"
	bufLen := 1000
	bufExpect := make([]byte, bufLen)
	nExpect, err := readFile(pathReal, bufExpect)
	assert.Equal(t, nil, err)

	DiskCachePath = "./mock-cache"
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
	assert.Equal(t, err, nil)
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
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	SetBlockSize(1 << 23)
	SetMemCache(0, 0)

	DiskCachePath = "./mock-cache"
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, err, nil)
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, err, nil)
	writer.Close()

	// var reader io.ReadCloser
	var buf []byte
	var n int

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(1 * time.Second)

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(1 * time.Second)

	// diskBlocksize = 1
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	SetBlockSize(1)
	SetMemCache(0, 0)
	client = getTestFSClient(t)
	path = "testRead"
	writer, err = client.Create(path)
	assert.Equal(t, err, nil)
	writeString = "test String for Client"
	_, err = writer.Write([]byte(writeString))
	writer.Close()
	assert.Equal(t, err, nil)

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(3 * time.Second)

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(3 * time.Second)

	os.RemoveAll("./mock-cache")
	os.RemoveAll("./mock")
}

func TestFSClient_diskCache_Read_Expire(t *testing.T) {
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	// defer os.RemoveAll("./mock")
	// defer os.RemoveAll("./datacache")
	SetBlockSize(1 << 23)
	SetMemCache(0, 0)
	SetDiskCache("./mock-cache", 6*time.Second)
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, err, nil)
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, err, nil)
	writer.Close()

	buf := make([]byte, len([]byte(writeString)))
	n, err := openAndRead(client, path, buf)
	assert.Equal(t, err, nil)
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
	// defer os.RemoveAll("./mock")
	// defer os.RemoveAll("./datacache")
	SetBlockSize(1 << 23)
	SetMemCache(0, 0)
	SetDiskCache("./mock-cache", DiskCacheExpire)
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, err, nil)
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, err, nil)
	writer.Close()

	buf := make([]byte, len([]byte(writeString)))
	n, err := openAndRead(client, path, buf)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)

	time.Sleep(2 * time.Second)
	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(2 * time.Second)

	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
}

func TestFSClient_cache_read(t *testing.T) {
	// 缓存满了看下发生什么
	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	// defer os.RemoveAll("./mock")
	// defer os.RemoveAll("./datacache")
	SetBlockSize(5)
	SetDiskCache("./mock-cache", DiskCacheExpire)
	client := getTestFSClient(t)
	path := "testRead"
	writer, err := client.Create(path)
	assert.Equal(t, err, nil)
	writeString := "test String for Client"
	_, err = writer.Write([]byte(writeString))
	assert.Equal(t, err, nil)
	writer.Close()

	buf := make([]byte, len([]byte(writeString)))
	n, err := openAndRead(client, path, buf)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)

	time.Sleep(2 * time.Second)
	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(2 * time.Second)

	path2 := "test2"
	client.Rename(path, path2)
	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path2, buf)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), writeString)
	time.Sleep(2 * time.Second)

	testMvString := "123456789"
	client.CreateFile(path, []byte(testMvString))
	buf = make([]byte, len([]byte(testMvString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, err, nil)
	assert.Equal(t, n, len(buf))
	assert.Equal(t, string(buf), testMvString)

	os.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
}
