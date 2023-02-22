package ufs

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	fsCommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/kubeflow/common/pkg/util"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	Ori_ak     = "ori_ak"
	Ori_sk     = "ori_sk"
	Ori_Bucket = "ori_bucket"
)

func TestBOS(t *testing.T) {
	defer os.RemoveAll("./tmp")

	properties := make(map[string]interface{})
	if os.Getenv(Ori_ak) == "" {
		log.Info("bos no ak")
		t.SkipNow()
	}
	properties[fsCommon.Type] = fsCommon.BosType
	properties[fsCommon.Region] = "bj"
	properties[fsCommon.Endpoint] = "bj.bcebos.com"
	properties[fsCommon.Bucket] = os.Getenv(Ori_Bucket)
	properties[fsCommon.SubPath] = "test_ut_" + util.RandString(10)
	properties[fsCommon.AccessKey] = os.Getenv(Ori_ak)
	properties[fsCommon.SecretKey] = os.Getenv(Ori_sk)

	bosfs, err := NewObjectFileSystem(properties)
	assert.Nil(t, err)
	fs := TestObj{
		UnderFileStorage: bosfs,
		testDir:          "test",
	}
	testObjectStorage(t, &fs)

}

func TestS3(t *testing.T) {
	defer os.RemoveAll("./tmp")
	properties := make(map[string]interface{})
	if os.Getenv(Ori_ak) == "" {
		log.Info("s3 no ak")
		t.SkipNow()
	}
	properties[fsCommon.Type] = fsCommon.S3Type
	properties[fsCommon.Region] = "bj"
	properties[fsCommon.Endpoint] = "s3.bj.bcebos.com"
	properties[fsCommon.Bucket] = os.Getenv(Ori_Bucket)
	properties[fsCommon.SubPath] = "test_ut_" + util.RandString(10)
	properties[fsCommon.AccessKey] = os.Getenv(Ori_ak)
	properties[fsCommon.SecretKey] = os.Getenv(Ori_sk)

	bosfs, err := NewObjectFileSystem(properties)
	assert.Nil(t, err)
	fs := TestObj{
		UnderFileStorage: bosfs,
		testDir:          "test",
	}
	testObjectStorage(t, &fs)
}

type TestObj struct {
	UnderFileStorage
	testDir string
}

// 每项测试最后都要保证test目录为空，不然没法删除
func testObjectStorage(t *testing.T, fs *TestObj) {
	err := fs.Mkdir(fs.testDir, 755)
	assert.Nil(t, err)
	defer fs.Rmdir(fs.testDir)
	fs.testDirOp(t)
	fs.testSmallFileRW(t)
	fs.testRename(t)
	err = fs.Chown("test", 1, 1)
	assert.Nil(t, err)
	err = fs.Truncate("test", 1)
	assert.Nil(t, err)
	err = fs.Access("test", 1, 1, 1)
	assert.Nil(t, err)
	err = fs.Link("1", "2")
	err = fs.Mknod("1", 0755, 1)
	_, err = fs.GetXAttr("1", "2")
	_, err = fs.ListXAttr("1")
	err = fs.RemoveXAttr("1", "2")
	err = fs.SetXAttr("1", "2", []byte{}, 1)
	_, err = fs.Open("1", 2, 4)
	err = fs.Symlink("123", "123")
	_, err = fs.Readlink("123")
	err = fs.Put("123", nil)

	// fs.testTruncat(t)
	// fs.testMPU(t)
}

func (fs *TestObj) testDirOp(t *testing.T) {
	var err error
	// fs.TestDir
	// fs.TestDirCreate
	err = fs.Mkdir(filepath.Join(fs.testDir, "dir1"), 755)
	assert.Nil(t, err)
	err = fs.Mkdir(filepath.Join(fs.testDir, "dir2"), 755)
	assert.Nil(t, err)

	// fs.TestDirRead
	dirEntrys, err := fs.ReadDir(fs.testDir)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(dirEntrys))

	// fs.TestDirInfo
	info, err := fs.GetAttr(fs.testDir)
	assert.Nil(t, err)
	assert.Equal(t, true, info.IsDir)

	// testRmdir
	err = fs.Rmdir(filepath.Join(fs.testDir, "dir1"))
	assert.Nil(t, err)
	dirEntrys, err = fs.ReadDir(fs.testDir)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(dirEntrys))

	err = fs.Rmdir(filepath.Join(fs.testDir, "dir2"))
	assert.Nil(t, err)

	// testFsStateInfo
	fsInfo := fs.StatFs("/")
	assert.Equal(t, *fsInfo, base.StatfsOut{
		Blocks:  0x1000000,
		Bfree:   0x1000000,
		Bavail:  0x1000000,
		Ffree:   0x1000000,
		Bsize:   0x1000000,
		NameLen: 1023,
	})

}

func (fs *TestObj) testSmallFileRW(t *testing.T) {

	// testCreate
	file := filepath.Join(fs.testDir, "foo")
	fh, err := fs.Create(file, uint32(flags), mode)
	assert.Nil(t, err)

	// testWrite
	data := []byte("testStorage")
	_, err = fh.Write(data, 0)
	assert.Nil(t, err)
	fh.Release()

	// testRead
	fh, err = fs.Open(file, syscall.O_RDONLY, 11)
	assert.Nil(t, err)
	buffer := make([]byte, 4)
	num, err := fh.Read(buffer, 0)
	assert.Nil(t, err)
	assert.Equal(t, 4, num)

	// testUnlink
	err = fs.Unlink(file)
	assert.Nil(t, err)

}

func (fs *TestObj) testMPU(t *testing.T) {
	var letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	content := make([]byte, 16*1024*1024)
	for i := 0; i < len(content); i++ {
		content[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	mpuFile := filepath.Join(fs.testDir, "mpuFile")
	fh, err := fs.Create(mpuFile, uint32(os.O_WRONLY|os.O_CREATE), 0755)
	assert.Nil(t, err)
	fh.Flush()
	fh.Release()

	fh, err = fs.Open(mpuFile, syscall.O_WRONLY, 0)
	assert.Nil(t, err)
	objfh, ok := fh.(*objectFileHandle)
	assert.Equal(t, ok, true)
	writted, err := objfh.Write(content, 0)
	assert.Nil(t, err)
	assert.Equal(t, uint32(16*1024*1024), writted)
	assert.Nil(t, objfh.MPU())
	objfh.Release()

	buf := make([]byte, 16*1024*1024)
	fhread, err := fs.Open(mpuFile, syscall.O_RDONLY, 16*1024*1024)
	assert.Nil(t, err)
	readed, err := fhread.Read(buf, 0)
	assert.Nil(t, err)
	assert.Equal(t, readed, 16*1024*1024)
	assert.Equal(t, buf, content)

	err = fs.Unlink(mpuFile)
	assert.Nil(t, err)
}

func (fs *TestObj) createTestFile(name string, content string) error {
	fh, err := fs.Create(name, uint32(os.O_WRONLY|os.O_CREATE), 0755)
	if err == syscall.EEXIST {
		fs.Unlink(name)
		fh, err = fs.Create(name, uint32(os.O_WRONLY|os.O_CREATE), 0755)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	fh.Write([]byte(content), 0)
	fh.Flush()
	fh.Release()
	return nil
}

func (fs *TestObj) testRename(t *testing.T) {
	var err error
	rename1 := filepath.Join(fs.testDir, "a")
	rename2 := filepath.Join(fs.testDir, "b")
	err = fs.Mkdir(rename1, 0)
	assert.Equal(t, nil, err)

	n := 10
	for i := 0; i < n; i++ {
		err = fs.createTestFile(filepath.Join(rename1, strconv.Itoa(i)), strconv.Itoa(i*i))
		assert.Equal(t, nil, err)
	}
	err = fs.Rename(rename1, rename2)
	assert.Equal(t, nil, err)

	list, err := fs.ReadDir(rename2)
	assert.Equal(t, nil, err)
	assert.Equal(t, 10, len(list))
	for i := 0; i < n; i++ {
		err = fs.Unlink(filepath.Join(rename2, strconv.Itoa(i)))
		assert.Equal(t, nil, err)
	}
	assert.Nil(t, fs.Rmdir(rename2))

}

func (fs *TestObj) testTruncat(t *testing.T) {
	file := filepath.Join(fs.testDir, "truncate")
	fh, err := fs.Create(file, uint32(flags), mode)
	assert.NotNil(t, fh)
	assert.Nil(t, err)

	data := []byte("hello world")
	_, err = fh.Write(data, 0)
	assert.Nil(t, err)
	fh.Flush()
	fh.Release()

	finfo, err := fs.GetAttr(file)
	assert.Nil(t, err)

	assert.Equal(t, int64(11), finfo.Size)

	// fh
	fh, err = fs.Open(file, uint32(os.O_WRONLY), uint64(finfo.Size))
	assert.Nil(t, err)

	// truncate 0 -> 4444
	err = fh.Truncate(4444)
	assert.Nil(t, err)
	finfo, err = fs.GetAttr(file)
	assert.Nil(t, err)
	assert.Equal(t, int64(4444), finfo.Size)

	// truncate 4444 -> 22
	err = fh.Truncate(22)
	assert.Nil(t, err)
	finfo, err = fs.GetAttr(file)
	assert.Nil(t, err)
	assert.Equal(t, int64(22), finfo.Size)
	assert.Nil(t, fs.Unlink(file))
}
