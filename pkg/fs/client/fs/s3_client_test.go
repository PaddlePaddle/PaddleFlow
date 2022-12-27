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
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	cache "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/cache"
	kv "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

const (
	TESTACCESSKEY  = "11111111111111111111eeeeeeeeeeeee"
	TESTSECRETKEY  = "11111111111111111111eeeeeeeeeeeee"
	TESTREGION     = "beijing-test"
	TESTBUCKETNAME = "testbucket"
	flags          = os.O_RDWR | os.O_CREATE | os.O_TRUNC
	mode           = 0666
)

func newS3Service() string {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())

	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(TESTACCESSKEY, TESTSECRETKEY, ""),
		Endpoint:         aws.String(ts.URL),
		Region:           aws.String(TESTREGION),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession, _ := session.NewSession(s3Config)

	s3Client := s3.New(newSession)
	cparams := &s3.CreateBucketInput{
		Bucket: aws.String(TESTBUCKETNAME),
	}
	// Create a new bucket using the CreateBucket call.
	_, _ = s3Client.CreateBucket(cparams)
	ports := strings.Split(ts.URL, ":")
	return fmt.Sprintf("http://localhost:%s", ports[2])
}

func getS3TestFsClientFake(t *testing.T) FSClient {
	endPoint := newS3Service()
	testFsMeta := common.FSMeta{
		UfsType:       common.S3Type,
		ServerAddress: endPoint,
		Properties: map[string]string{
			"accessKey":        TESTACCESSKEY,
			"bucket":           TESTBUCKETNAME,
			"endpoint":         endPoint,
			"region":           TESTREGION,
			"secretKey":        TESTSECRETKEY,
			"s3ForcePathStyle": "true",
		},
	}
	fsclient, err := NewFSClientForTest(testFsMeta)
	assert.Equal(t, nil, err)
	return fsclient
}

func TestS3lient_base_1(t *testing.T) {
	client := getS3TestFsClientFake(t)
	assert.NotNil(t, client)

	defer func() {
		os.RemoveAll("./tmp")
	}()

	newPath := "createFile"
	newDir1 := "mock/Dir1"
	newDir2 := "mock/Dir2/Dir1"
	newDir3 := "mock/Dir3"
	newDir4 := "mock/Renamedir"

	err := client.MkdirAll(newDir2, 0755)
	assert.Equal(t, nil, err)
	err = client.Mkdir(newDir3, 0755)
	assert.Equal(t, nil, err)

	file, err := client.Create(newPath)
	assert.Equal(t, nil, err)
	assert.NotNil(t, file)
	num, err := file.Write([]byte("0123456789")) // n = 10
	assert.Nil(t, err)
	assert.Equal(t, 10, num)
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

	err = client.Mkdir(newDir4, 0755)
	assert.Equal(t, nil, err)
	err = client.Chmod(newDir4, 0755)
	assert.Equal(t, nil, err)
	err = client.RemoveAll(newDir1)
	assert.Equal(t, nil, err)

	_, err = client.IsDir(newDir4)
	assert.Equal(t, nil, err)
	dirInfos, err := client.ListDir("/mock")
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, len(dirInfos))

	srcbuffer := strings.NewReader("test save file")
	err = client.SaveFile(srcbuffer, "/mock", "test2")
	assert.Equal(t, nil, err)
	n, err = client.CreateFile("/mock/test3", []byte("test create file: test3"))
	assert.Equal(t, nil, err)
	assert.Equal(t, n, 23)

}

func TestReadBigDir(t *testing.T) {

	client := getS3TestFsClientFake(t)
	assert.NotNil(t, client)

	defer func() {
		os.RemoveAll("./tmp")
	}()

	m := meta.Config{
		AttrCacheExpire:  10 * time.Minute,
		EntryCacheExpire: 10 * time.Minute,
		PathCacheExpire:  10 * time.Second,
		Config: kv.Config{
			Driver:    kv.DiskType,
			CachePath: "./mock-meta",
		},
	}
	SetMetaCache(m)
	entryCnt := 10000
	var err error
	for i := 0; i < entryCnt/2; i++ {
		_, err = client.Create(fmt.Sprintf("entry-file-%d", i))
		assert.Nil(t, err)
		if err != nil {
			fmt.Print(err.Error())
			time.Sleep(10 * time.Second)
		}
	}
	for i := entryCnt / 2; i < entryCnt; i++ {
		err := client.Mkdir(fmt.Sprintf("entry-dir-%d", i), 0755)
		assert.Nil(t, err)
	}
	entries, err := client.ListDir("/")
	assert.Nil(t, err)
	//有个.stats目录
	assert.Equal(t, entryCnt, len(entries)-1)
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

	client := getS3TestFsClientFake(t)
	assert.NotNil(t, client)
	defer func() {
		os.RemoveAll("./mock-cache")
		os.RemoveAll("./tmp")
	}()

	err := client.Mkdir("mock", 0755)
	assert.Equal(t, nil, err)

	path := "mock/testRead"
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

	fileReader, err := client.Open(path)
	assert.Nil(t, err)
	assert.NotNil(t, fileReader)
	n, err = fileReader.Read(buf)
	assert.Nil(t, err)
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
		BlockSize:    2,
		MaxReadAhead: 40,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)

	client := getS3TestFsClientFake(t)
	assert.NotNil(t, client)
	defer func() {
		os.RemoveAll("./mock-cache")
		os.RemoveAll("./tmp")
	}()

	pathReal := "../../../../example/hoursing_price/run.yaml"
	bufLen := 1000
	bufExpect := make([]byte, bufLen)
	nExpect, err := readFile(pathReal, bufExpect)
	assert.Equal(t, nil, err)

	err = client.Mkdir("mock", 0755)
	assert.Equal(t, nil, err)

	path := "mock/testRead_small_1"
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
	assert.Nil(t, err)
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

	client := getS3TestFsClientFake(t)
	if client == nil {
		return
	}
	assert.NotNil(t, client)
	defer func() {
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
		BlockSize:    2,
		MaxReadAhead: 40,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)

	client := getS3TestFsClientFake(t)
	assert.NotNil(t, client)
	defer func() {
		os.RemoveAll("./mock-cache")
		os.RemoveAll("./tmp")
	}()

	pathReal := "../../../../example/hoursing_price/run.yaml"
	bufLen := 1000
	bufExpect := make([]byte, bufLen)
	nExpect, err := readFile(pathReal, bufExpect)
	assert.Equal(t, nil, err)

	err = client.Mkdir("mock", 0755)
	assert.Equal(t, nil, err)

	path := "mock/testRead_small_1"
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

func TestReaname(t *testing.T) {
	dc := cache.Config{
		BlockSize:    20,
		MaxReadAhead: 40,
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(dc)
	m := meta.Config{
		AttrCacheExpire:  100 * time.Second,
		EntryCacheExpire: 100 * time.Second,
		PathCacheExpire:  2 * time.Second,
		Config: kv.Config{
			Driver: kv.MemType,
		},
	}
	SetMetaCache(m)
	client := getS3TestFsClientFake(t)
	assert.NotNil(t, client)
	defer func() {
		os.RemoveAll("./tmp")
		os.RemoveAll("./mock-cache")
	}()
	_, err := client.Create("11.txt")
	assert.Nil(t, err)
	err = client.Rename("11.txt", "11.log")
	assert.Nil(t, err)
	dirEntry, err := client.ListDir("")
	assert.Nil(t, err)
	assert.Equal(t, "11.log", dirEntry[0].Name())

}
