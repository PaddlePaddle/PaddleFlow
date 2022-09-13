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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	cache "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/cache_new"
	kv "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv_new"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

func getS3TestFsClient(t *testing.T) FSClient {
	if os.Getenv("S3_AK") == "" {
		log.Errorf("S3 Client Init Fail")
		return nil
	}
	testFsMeta := common.FSMeta{
		UfsType:       common.S3Type,
		ServerAddress: os.Getenv("S3_SERVER_ADDRESS"),
		Properties: map[string]string{
			"accessKey": os.Getenv("S3_AK"),
			"bucket":    os.Getenv("S3_BUCKET"),
			"endpoint":  os.Getenv("S3_ENDPOINT"),
			"region":    os.Getenv("S3_REGION"),
			"secretKey": os.Getenv("S3_SK_ENCRYPTED"),
		},
		SubPath: os.Getenv("S3_PATH"),
	}
	DataCachePath = "./mock-cache"
	fsclient, err := NewFSClientForTest(testFsMeta)
	assert.Equal(t, nil, err)
	return fsclient
}

func TestS3lient_base_1(t *testing.T) {
	client := getS3TestFsClient(t)
	if client == nil {
		return
	}
	assert.NotNil(t, client)

	client.RemoveAll("./mock")
	client.RemoveAll("./mock1")
	os.RemoveAll("./mock-cache")
	os.RemoveAll("./tmp")
	defer func() {
		client.RemoveAll("./mock")
		client.RemoveAll("./mock1")
		os.RemoveAll("./mock-cache")
		os.RemoveAll("./tmp")
	}()

	newPath := "/mock/createFile"
	newDir1 := "/mock/Dir1"
	newDir2 := "mock/Dir2/Dir1"
	newDir3 := "/mock/Dir3"
	newDir4 := "/mock/Renamedir"

	err := client.MkdirAll(newDir2, 0755)
	assert.Equal(t, nil, err)
	err = client.Mkdir(newDir3, 0755)
	assert.Equal(t, nil, err)

	file, err := client.Create(newPath)
	assert.Equal(t, nil, err)
	assert.NotNil(t, file)

	file.Write([]byte("0123456789"))  // n = 10
	file.Write([]byte("abcdefghijk")) // n = 11
	file.Write([]byte("\n\n"))        // n = 2
	file.Close()

	newFile, err := client.Open(newPath)
	assert.Equal(t, nil, err)
	assert.NotNil(t, newFile)

	buffer := make([]byte, 200)
	n, err := newFile.Read(buffer)
	assert.Equal(t, nil, err)
	assert.Equal(t, 23, n)
	assert.Equal(t, string(buffer[0:n]), "0123456789abcdefghijk\n\n")
	newFile.Close()

	newFile, err = client.Open(newPath)
	assert.Equal(t, nil, err)
	assert.NotNil(t, newFile)
	context, err := ioutil.ReadAll(newFile)
	assert.Equal(t, nil, err)
	assert.Equal(t, string(context), "0123456789abcdefghijk\n\n")
	newFile.Close()

	err = client.Mkdir(newDir1, 0755)
	assert.Equal(t, nil, err)
	err = client.Chmod(newDir1, 0777)
	assert.Equal(t, nil, err)
	// todo:: s3目之间的重命名暂时不支持，后续需要加下
	// err = client.Rename(newDir1, newDir4)
	// assert.Equal(t, nil, err)
	err = client.Mkdir(newDir4, 0755)
	assert.Equal(t, nil, err)
	err = client.Chmod(newDir4, 0755)
	assert.Equal(t, nil, err)
	err = client.RemoveAll(newDir1)
	assert.Equal(t, nil, err)

	dirs, err := client.Readdirnames("/mock", -1)
	assert.Equal(t, nil, err)
	fmt.Printf("======: %+v", dirs)
	assert.Equal(t, 4, len(dirs))
	_, err = client.IsDir(newDir4)
	assert.Equal(t, nil, err)
	dirInfos, err := client.ListDir("/mock")
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, len(dirInfos))
	isEmpty, err := client.IsEmptyDir(newDir2)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, isEmpty)
	isEmpty, err = client.IsEmptyDir("/mock")
	assert.Equal(t, nil, err)
	assert.Equal(t, false, isEmpty)

	err = client.Remove(newDir3)
	assert.Equal(t, nil, err)
	exist, err := client.Exist(newDir3)
	assert.Equal(t, nil, err)
	assert.Equal(t, false, exist)
	size, err := client.Size(newPath)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(23), size)

	copyDir := "/mock1"
	err = client.Mkdir(copyDir, 0755)
	assert.Equal(t, nil, err)
	err = client.Copy("/mock", copyDir)
	assert.Equal(t, nil, err)
	copyDir1 := "/mock2"
	err = client.Copy(newPath, copyDir1)
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
	assert.Equal(t, n, 23)

	info, err := client.Stat("/mock/test3")
	assert.Equal(t, nil, err)
	assert.Equal(t, false, info.IsDir())
}

func TestS3Client_read(t *testing.T) {
	d := cache.Config{
		BlockSize:    200,
		MaxReadAhead: 4,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)

	client := getS3TestFsClient(t)
	if client == nil {
		return
	}
	assert.NotNil(t, client)

	client.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer func() {
		client.RemoveAll("./mock")
		os.RemoveAll("./mock-cache")
		os.RemoveAll("./tmp")
	}()

	err := client.Mkdir("./mock", 0755)
	assert.Equal(t, nil, err)

	path := "./mock/testRead"
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
	assert.Equal(t, len(buf), n)
	assert.Equal(t, writeString, string(buf))

	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, writeString, string(buf))

	time.Sleep(3 * time.Second)
	buf = make([]byte, len([]byte(writeString)))
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, writeString, string(buf))

	reader, err = client.Open(path)
	assert.Equal(t, err, nil)
	readn := 5
	buf1 := make([]byte, readn)
	buf2 := make([]byte, len([]byte(writeString))-readn)
	n1, err := reader.Read(buf1)
	n2, err := reader.Read(buf2)
	assert.Equal(t, nil, err)
	assert.Equal(t, len([]byte(writeString)), n1+n2)
	assert.Equal(t, string(buf1)+string(buf2), writeString)
	reader.Close()
	time.Sleep(1 * time.Second)
}

func TestS3Client_read_with_small_block(t *testing.T) {
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 40,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)

	client := getS3TestFsClient(t)
	if client == nil {
		return
	}
	assert.NotNil(t, client)

	client.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer func() {
		client.RemoveAll("./mock")
		os.RemoveAll("./mock-cache")
		os.RemoveAll("./tmp")
	}()

	pathReal := "../../../../example/hoursing_price/run.yaml"
	bufLen := 1000
	bufExpect := make([]byte, bufLen)
	nExpect, err := readFile(pathReal, bufExpect)
	assert.Equal(t, nil, err)

	DataCachePath = "./mock-cache"

	err = client.Mkdir("./mock", 0755)
	assert.Equal(t, nil, err)

	path := "./mock/testRead_small_1"
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
	p := setPoolNil()
	n1, err := reader.Read(buf1)
	p.Reset()
	n2, err := reader.Read(buf2)
	assert.Equal(t, nil, err)
	assert.Equal(t, nExpect, n1+n2)
	reader.Close()

	assert.Equal(t, string(bufExpect[0:nExpect]), string(buf1)+string(buf2))
}

func TestS3Client_readMemAndDisk(t *testing.T) {
	d := cache.Config{
		BlockSize:    3,
		MaxReadAhead: 4000,
	}
	SetDataCache(d)

	client := getS3TestFsClient(t)
	if client == nil {
		return
	}
	assert.NotNil(t, client)

	client.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer func() {
		client.RemoveAll("./mock")
		os.RemoveAll("./mock-cache")
		os.RemoveAll("./tmp")
	}()

	err := client.Mkdir("./mock", 0755)
	assert.Equal(t, nil, err)

	pathReal := "../../../../example/hoursing_price/run.yaml"
	bufLen := 1000
	bufExpect := make([]byte, bufLen)
	nExpect, err := readFile(pathReal, bufExpect)
	assert.Equal(t, nil, err)

	path := "./mock/file"
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
}

func TestS3Client_read_with_small_block_with_no_no_mem(t *testing.T) {
	d := cache.Config{
		BlockSize:    1,
		MaxReadAhead: 40,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)

	client := getS3TestFsClient(t)
	if client == nil {
		return
	}
	assert.NotNil(t, client)

	client.RemoveAll("./mock")
	os.RemoveAll("./mock-cache")
	defer func() {
		client.RemoveAll("./mock")
		os.RemoveAll("./mock-cache")
		os.RemoveAll("./tmp")
	}()

	pathReal := "../../../../example/hoursing_price/run.yaml"
	bufLen := 1000
	bufExpect := make([]byte, bufLen)
	nExpect, err := readFile(pathReal, bufExpect)
	assert.Equal(t, nil, err)

	DataCachePath = "./mock-cache"

	err = client.Mkdir("./mock", 0755)
	assert.Equal(t, nil, err)

	path := "./mock/testRead_small_1"
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

	p := setPoolNil()
	buf = make([]byte, bufLen)
	n, err = openAndRead(client, path, buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, nExpect, n)
	assert.Equal(t, string(bufExpect), string(buf))

	p.Reset()
	log.Infof("s3begin read err %v", path)

	reader, err := client.Open(path)
	assert.Equal(t, err, nil)
	readn := 5
	buf1 := make([]byte, readn)
	buf2 := make([]byte, nExpect-readn)
	n1, err := reader.Read(buf1)
	p = setPoolNil()
	n2, err := reader.Read(buf2)
	assert.Equal(t, nil, err)
	assert.Equal(t, nExpect, n1+n2)
	reader.Close()
	p.Reset()

	assert.Equal(t, string(bufExpect[0:nExpect]), string(buf1)+string(buf2))
}
