package fs

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/cache"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv"
	meta "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs/object"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/vfs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

const (
	Ori_ak      = "ori_ak"
	Ori_sk      = "ori_sk"
	Ori_Bucket  = "ori_bucket"
	Ori_subpath = "ori_subpath"
)

var testBigFileName = "random-big-file.bin"
var testSmallFileName = "random-small-file.bin"

func TestS3(t *testing.T) {
	if os.Getenv(Ori_ak) == "" || os.Getenv(Ori_subpath) == "" || os.Getenv(Ori_subpath) == "/" {
		log.Info("not ready")
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())
	d := cache.Config{
		BlockSize:    (1 + rand.Intn(100)) * 1024 * 1024,
		MaxReadAhead: 1 + rand.Intn(300*1024*1024),
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	log.Infof("test s3 cache %+v", d)
	SetDataCache(d)
	client := getS3Client(t)
	defer func() {
		err := client.Remove(testBigFileName)
		assert.Equal(t, nil, err)
		err = client.Remove(testSmallFileName)
		assert.Equal(t, nil, err)
		os.Remove(testBigFileName)
		os.Remove(testSmallFileName)
		os.RemoveAll("./tmp")
		os.RemoveAll("./mock-cache")
	}()
	client.RemoveAll("/")
	testBigFile(t, client)
	testSmallFile(t, client)
	testMkdirAndList(t, client)
	testRename(t, client, "s3")
	testSetAttr(t, client)

	metaConfig := meta.Config{
		Config: kv.Config{
			Driver: meta.MemDriver,
		},
		AttrCacheExpire:  5,
		EntryCacheExpire: 5,
	}
	SetMetaCache(metaConfig)
	clientWithMetaCache := getBosClient(t)
	testReadDir(t, client, clientWithMetaCache)

}

func TestBos(t *testing.T) {
	if os.Getenv(Ori_ak) == "" || os.Getenv(Ori_subpath) == "" || os.Getenv(Ori_subpath) == "/" {
		log.Info("not ready")
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())
	d := cache.Config{
		BlockSize:    (1 + rand.Intn(100)) * 1024 * 1024,
		MaxReadAhead: 1 + rand.Intn(300*1024*1024),
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	log.Infof("test s3 cache %+v", d)
	SetDataCache(d)
	client := getBosClient(t)
	defer func() {
		err := client.Remove(testBigFileName)
		assert.Equal(t, nil, err)
		err = client.Remove(testSmallFileName)
		assert.Equal(t, nil, err)
		os.Remove(testBigFileName)
		os.Remove(testSmallFileName)
		os.RemoveAll("./tmp")
		os.RemoveAll("./mock-cache")
	}()
	client.RemoveAll("/")
	testBigFile(t, client)
	testSmallFile(t, client)
	testMkdirAndList(t, client)
	testRename(t, client, "bos")

}

func getBosClient(t *testing.T) FSClient {
	testFsMeta := common.FSMeta{
		UfsType: common.BosType,
		Type:    common.BosType,
		Properties: map[string]string{
			common.Type:      common.BosType,
			common.Region:    "bj",
			common.Endpoint:  "bj.bcebos.com",
			common.Bucket:    os.Getenv(Ori_Bucket),
			common.SubPath:   os.Getenv(Ori_subpath),
			common.AccessKey: os.Getenv(Ori_ak),
			common.SecretKey: os.Getenv(Ori_sk),
		},
		SubPath: os.Getenv(Ori_subpath),
	}
	DataCachePath = "./mock-cache"
	fsclient, err := NewFSClientForTest(testFsMeta)
	assert.Equal(t, nil, err)
	return fsclient
}

func getS3Client(t *testing.T) FSClient {
	testFsMeta := common.FSMeta{
		UfsType: common.S3Type,
		Type:    common.S3Type,
		Properties: map[string]string{
			common.Type:      common.S3Type,
			common.Region:    "bj",
			common.Endpoint:  "s3.bj.bcebos.com",
			common.Bucket:    os.Getenv(Ori_Bucket),
			common.SubPath:   os.Getenv(Ori_subpath),
			common.AccessKey: os.Getenv(Ori_ak),
			common.SecretKey: os.Getenv(Ori_sk),
		},
		SubPath: os.Getenv(Ori_subpath),
	}
	DataCachePath = "./mock-cache"
	fsclient, err := NewFSClientForTest(testFsMeta)
	assert.Equal(t, nil, err)
	return fsclient
}

func testBigFile(t *testing.T, client FSClient) {
	fileSize := createLocalFile(t, true)
	n := testWrite(t, client, true)
	assert.Equal(t, n, int64(fileSize))
	testStates(t, client, fileSize, testBigFileName)
	md5Str := localMd5(t, testBigFileName)
	testRead(t, client, testBigFileName, md5Str)
	testRead(t, client, testBigFileName, md5Str)
}

func test6GBigFile(t *testing.T, client FSClient) {
	fileSize := 5*1024*1024*1024 + rand.Intn(200000) // 5g + rand
	log.Infof("test6GBigFile file size %d", fileSize)

	name := "5g.file"
	renameName := "5g.file.rename"
	f, err := os.Create(name)
	assert.Equal(t, nil, err)
	defer func() {
		f.Close()
	}()

	buf := make([]byte, 4096) // 4KB buffer
	for i := 0; i < fileSize/len(buf); i++ {
		rand.Read(buf)
		_, err := f.Write(buf)
		if err != nil {
			assert.Equal(t, nil, err)
		}
	}

	remaining := fileSize % len(buf)
	if remaining > 0 {
		buf = make([]byte, remaining)
		rand.Read(buf)
		_, err := f.Write(buf)
		if err != nil {
			assert.Equal(t, nil, err)
		}
	}
	assert.Equal(t, nil, err)

	in, err := os.Open(name)
	assert.Equal(t, nil, err)
	defer in.Close()

	out, err := client.Create(name)
	assert.Equal(t, nil, err)
	defer out.Close()

	n, err := io.Copy(out, in)
	assert.Equal(t, nil, err)

	out.Close()
	in.Close()
	time.Sleep(2 * time.Second)
	assert.Equal(t, n, int64(fileSize))

	err = client.Rename(name, renameName)
	assert.Equal(t, nil, err)
	stat_, err := client.Stat(renameName)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(fileSize), stat_.Size())
}

func localMd5(t *testing.T, name string) string {
	fh, err := os.Open(name)
	assert.Equal(t, nil, err)
	defer fh.Close()
	data, err := ioutil.ReadAll(fh)

	m5 := md5.Sum(data)
	md5str := fmt.Sprintf("%x", m5) // 将[]byte转成16进制
	return md5str
}
func testSmallFile(t *testing.T, client FSClient) {
	fileSize := createLocalFile(t, false)
	n := testWrite(t, client, false)
	assert.Equal(t, n, int64(fileSize))
	testStates(t, client, fileSize, testSmallFileName)
	md5Str := localMd5(t, testSmallFileName)
	testRead(t, client, testSmallFileName, md5Str)
	testRead(t, client, testSmallFileName, md5Str)
}

func testWrite(t *testing.T, client FSClient, fileType bool) int64 {
	var name string
	if fileType {
		name = testBigFileName
	} else {
		name = testSmallFileName
	}
	in, err := os.Open(name)
	assert.Equal(t, nil, err)
	defer in.Close()

	out, err := client.Create(name)
	assert.Equal(t, nil, err)
	defer out.Close()

	n, err := io.Copy(out, in)
	assert.Equal(t, nil, err)

	return n
}

func createLocalFile(t *testing.T, fileType bool) int {
	var fileSize int
	if fileType {
		fileSize = (200 + rand.Intn(100)) * 1024 * 1024 // 200MB-300MB
	} else {
		fileSize = (500 + rand.Intn(1500)) * 1024 // 500k-20MB
	}
	log.Infof("fileType %v and fileSize %v", fileType, fileSize)

	var name string
	if fileType {
		name = testBigFileName
	} else {
		name = testSmallFileName
	}
	f, err := os.Create(name)
	assert.Equal(t, nil, err)
	defer func() {
		f.Close()
	}()

	buf := make([]byte, 4096) // 4KB buffer
	for i := 0; i < fileSize/len(buf); i++ {
		rand.Read(buf)
		_, err := f.Write(buf)
		if err != nil {
			assert.Equal(t, nil, err)
		}
	}

	remaining := fileSize % len(buf)
	if remaining > 0 {
		buf = make([]byte, remaining)
		rand.Read(buf)
		_, err := f.Write(buf)
		if err != nil {
			assert.Equal(t, nil, err)
		}
	}
	assert.Equal(t, nil, err)
	return fileSize
}

func testStates(t *testing.T, client FSClient, fileSize int, name string) {
	size, err := client.Size(name)
	assert.Equal(t, nil, err)
	assert.Equal(t, size, int64(fileSize))
}

func testRead(t *testing.T, client FSClient, name string, md5sum string) {
	fh, err := client.Open(name)
	assert.Equal(t, nil, err)
	defer fh.Close()
	data, err := ioutil.ReadAll(fh)
	assert.Equal(t, nil, err)
	readMd5 := md5.Sum(data)
	md5str := fmt.Sprintf("%x", readMd5)
	assert.Equal(t, md5str, md5sum)
}

func testMkdirAndList(t *testing.T, client FSClient) {
	dirNamePrefix := "smallDirs"
	dirName := "smallDirs/a/b/c/d/e"
	renameName := "smallDirs/a/b/c/d/h"
	err := client.Mkdir(dirNamePrefix, 0755)
	assert.Equal(t, nil, err)
	defer func() {
		err = client.RemoveAll(dirNamePrefix)
		assert.Equal(t, nil, err)
	}()
	err = client.MkdirAll(dirName, 0755)
	assert.Equal(t, nil, err)
	num := 1 + rand.Intn(50)
	fileSize := 4096 + rand.Intn(100)*4096 + rand.Intn(4096)
	log.Infof("fileSize %v", fileSize)
	for i := 0; i < num; i++ {
		f, err := client.Create(filepath.Join(dirName, strconv.Itoa(i)))
		assert.Equal(t, nil, err)
		buf := make([]byte, 4096) // 4KB buffer
		for i := 0; i < fileSize/len(buf); i++ {
			rand.Read(buf)
			_, err := f.Write(buf)
			if err != nil {
				assert.Equal(t, nil, err)
			}
		}

		remaining := fileSize % len(buf)
		if remaining > 0 {
			buf = make([]byte, remaining)
			rand.Read(buf)
			_, err := f.Write(buf)
			if err != nil {
				assert.Equal(t, nil, err)
			}
		}
		assert.Equal(t, nil, err)
		f.Close()
	}

	listDir, err := client.ListDir(dirName)
	assert.Equal(t, nil, err)
	assert.Equal(t, num, len(listDir))
	for _, file := range listDir {
		assert.Equal(t, fileSize, int(file.Size()))
	}
	err = client.Rename(dirName, renameName)
	assert.Equal(t, nil, err)
	err = client.RemoveAll(dirNamePrefix)
	assert.Equal(t, nil, err)
}

func testRename(t *testing.T, client FSClient, fsType string) {
	fromDir := "fromDir"
	client.RemoveAll(fromDir)
	defer client.RemoveAll(fromDir)
	assert.Nil(t, client.Mkdir(fromDir, 0755))
	entryCnt := 100 + rand.Intn(10)
	for i := 0; i < entryCnt; i++ {
		_, err := client.Create(filepath.Join(fromDir, strconv.Itoa(i)))
		assert.Equal(t, nil, err)
	}
	toDir := "toDir"
	client.RemoveAll(toDir)
	defer client.RemoveAll(toDir)
	assert.Nil(t, nil, client.Rename(fromDir, toDir))
	entrys, err := client.ListDir(toDir)
	assert.Nil(t, err)
	assert.Equal(t, entryCnt, len(entrys))

	// test rename failed
	if fsType == "s3" {
		p1 := gomonkey.ApplyMethod(reflect.TypeOf(&object.S3Storage{}), "Copy", func(_ *object.S3Storage, newKey string, copySource string) error {
			return errors.New("test copy err")
		})
		defer p1.Reset()
		assert.Nil(t, client.Mkdir(fromDir, 0755))
		err = client.Rename(fromDir, "toErrDir")
		assert.NotNil(t, err)
	}
}

func TestS3Rename(t *testing.T) {
	if os.Getenv(Ori_ak) == "" || os.Getenv(Ori_subpath) == "" || os.Getenv(Ori_subpath) == "/" {
		log.Info("not ready")
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())
	d := cache.Config{
		BlockSize:    (1 + rand.Intn(100)) * 1024 * 1024,
		MaxReadAhead: 1 + rand.Intn(300*1024*1024),
		Expire:       1 * time.Second,
		Config: kv.Config{
			Driver:    kv.MemType,
			CachePath: "./mock-cache",
		},
	}
	SetDataCache(d)
	client := getS3Client(t)
	defer func() {
		client.RemoveAll("/")
	}()

	test6GBigFile(t, client)
	return

}

func testSetAttr(t *testing.T, client FSClient) {
	file := "setAttr.test"
	fh, err := client.Create("setAttr.test")
	assert.Nil(t, err)
	assert.Nil(t, fh.Close())
	defer client.Remove(file)
	// set Mtime only
	attr, err := client.SetAttr(file, uint32(32), 0, 0, 0, 10, 12, 12, 13, 0)
	assert.Nil(t, err)
	assert.Equal(t, time.Unix(0, int64(12)*1e9+int64(13)), attr.ModTime())
	// set Atime only
	attr, err = client.SetAttr(file, uint32(16), 0, 0, 0, 10, 12, 120, 133, 0)
	assert.Nil(t, err)
	assert.Equal(t, time.Unix(0, int64(12)*1e9+int64(13)), attr.ModTime())
	p1 := gomonkey.ApplyMethod(reflect.TypeOf(&vfs.VFS{}), "SetAttr", func(_ *vfs.VFS, ctx *meta.Context, ino vfs.Ino, set, mode, uid, gid uint32, atime, mtime int64, atimensec, mtimensec uint32, size uint64) (entry *meta.Entry, err syscall.Errno) {
		return nil, syscall.EACCES
	})
	defer p1.Reset()
	_, err = client.SetAttr(file, uint32(16), 0, 0, 0, 10, 12, 120, 133, 0)
	assert.NotNil(t, err)
}

func testReadDir(t *testing.T, clientWithOutMetaCache, clientWithMetaCache FSClient) {
	clientWithOutMetaCache.RemoveAll("testDir")
	assert.Nil(t, clientWithOutMetaCache.Mkdir("testDir", 0755))
	defer clientWithOutMetaCache.RemoveAll("testDir")
	_, err := clientWithOutMetaCache.CreateFile("testDir/a", []byte("aaaa"))
	assert.Nil(t, err)
	_, err = clientWithOutMetaCache.CreateFile("testDir/b", []byte("bbbb"))
	assert.Nil(t, err)
	entries1, err := clientWithMetaCache.ListDir("testDir")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(entries1))
	assert.Nil(t, clientWithOutMetaCache.Remove("testDir/a"))
	_, err = clientWithMetaCache.ListDir("testDir")
	assert.Nil(t, err)
}
