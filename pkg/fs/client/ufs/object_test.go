package ufs

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/kubeflow/common/pkg/util"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/go-sdk/service"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs/object"
	fsCommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
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
	properties[fsCommon.Group] = "root-group"
	properties[fsCommon.Owner] = "root"

	bosfs, err := NewObjectFileSystem(properties)
	assert.Nil(t, err)
	fs := TestObj{
		UnderFileStorage: bosfs,
		testDir:          "test",
	}
	testObjectStorage(t, &fs)
}

func TestSTS(t *testing.T) {
	defer os.RemoveAll("./tmp")

	properties := make(map[string]interface{})
	properties[fsCommon.Type] = fsCommon.BosType
	properties[fsCommon.Region] = "bj"
	properties[fsCommon.Endpoint] = "bj.bcebos.com"
	properties[fsCommon.Bucket] = "zxx"
	properties[fsCommon.SubPath] = "test_ut_" + util.RandString(10)
	properties[fsCommon.AccessKey] = "1111"
	properties[fsCommon.SecretKey] = "abasdfsadfgdfgadfsagsdgdsg"
	properties[fsCommon.Token] = "fsadgsadgasdgasdgasdg"
	properties[fsCommon.FsName] = "fsbb"
	properties[fsCommon.Group] = "root-group"
	properties[fsCommon.Owner] = "root"
	properties[fsCommon.StsServer] = "127.0.0.1:8999"

	_, err := NewObjectFileSystem(properties)
	assert.NotNil(t, err, nil)
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

func Test_newStsServerClient(t *testing.T) {
	type args struct {
		serverAddress string
	}
	tests := []struct {
		name    string
		args    args
		want    *service.PaddleFlowClient
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "test-err",
			args: args{"127.0.0.1:8999"},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newStsServerClient(tt.args.serverAddress)
			if !tt.wantErr(t, err, fmt.Sprintf("newStsServerClient(%v)", tt.args.serverAddress)) {
				return
			}
		})
	}
}

func Test_objectFileHandle_Read(t *testing.T) {
	type fields struct {
		mpuInfo        mpuInfo
		storage        object.ObjectStorage
		name           string
		key            string
		size           uint64
		flags          uint32
		writeTmpfile   *os.File
		canWrite       chan struct{}
		writeSrcReader io.ReadCloser
		mu             sync.RWMutex
		writeDirty     bool
	}
	type args struct {
		dest []byte
		off  uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:   "off >= len(dest)",
			fields: fields{},
			args: args{
				dest: []byte("12"),
				off:  5,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return false
			},
			want: 0,
		},
		{
			name: "fh size == 0 ",
			fields: fields{
				size: 0,
			},
			args: args{
				dest: []byte("414244444"),
				off:  5,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return false
			},
			want: 0,
		},
		{
			name: "endpos > size ",
			fields: fields{
				size:    3,
				storage: object.S3Storage{},
			},
			args: args{
				dest: []byte("414244444"),
				off:  5,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
			want: 0,
		},
	}

	a := &object.S3Storage{}
	var p1 = gomonkey.ApplyMethod(reflect.TypeOf(a), "Get",
		func(a *object.S3Storage, key string, off, limit int64) (io.ReadCloser, error) {
			return nil, fmt.Errorf("get err")
		})
	defer p1.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fh := &objectFileHandle{
				mpuInfo:        tt.fields.mpuInfo,
				storage:        tt.fields.storage,
				name:           tt.fields.name,
				key:            tt.fields.key,
				size:           tt.fields.size,
				flags:          tt.fields.flags,
				writeTmpfile:   tt.fields.writeTmpfile,
				canWrite:       tt.fields.canWrite,
				writeSrcReader: tt.fields.writeSrcReader,
				mu:             tt.fields.mu,
				writeDirty:     tt.fields.writeDirty,
			}
			got, err := fh.Read(tt.args.dest, tt.args.off)
			if !tt.wantErr(t, err, fmt.Sprintf("Read(%v, %v)", tt.args.dest, tt.args.off)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Read(%v, %v)", tt.args.dest, tt.args.off)
		})
	}
}

func Test_objectFileHandle_Write(t *testing.T) {
	type fields struct {
		mpuInfo        mpuInfo
		storage        object.ObjectStorage
		name           string
		key            string
		size           uint64
		flags          uint32
		writeTmpfile   *os.File
		canWrite       chan struct{}
		writeSrcReader io.ReadCloser
		mu             sync.RWMutex
		writeDirty     bool
	}
	type args struct {
		data []byte
		off  uint64
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantWritten uint32
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name:   "write empty",
			fields: fields{},
			args: args{
				data: []byte{},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
		{
			name:   "writeTmpfile empty",
			fields: fields{},
			args: args{
				data: []byte("1245"),
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fh := &objectFileHandle{
				mpuInfo:        tt.fields.mpuInfo,
				storage:        tt.fields.storage,
				name:           tt.fields.name,
				key:            tt.fields.key,
				size:           tt.fields.size,
				flags:          tt.fields.flags,
				writeTmpfile:   tt.fields.writeTmpfile,
				canWrite:       tt.fields.canWrite,
				writeSrcReader: tt.fields.writeSrcReader,
				mu:             tt.fields.mu,
				writeDirty:     tt.fields.writeDirty,
			}
			gotWritten, err := fh.Write(tt.args.data, tt.args.off)
			if !tt.wantErr(t, err, fmt.Sprintf("Write(%v, %v)", tt.args.data, tt.args.off)) {
				return
			}
			assert.Equalf(t, tt.wantWritten, gotWritten, "Write(%v, %v)", tt.args.data, tt.args.off)
		})
	}
}

func Test_objectFileHandle_Release(t *testing.T) {
	type fields struct {
		mpuInfo        mpuInfo
		storage        object.ObjectStorage
		name           string
		key            string
		size           uint64
		flags          uint32
		writeTmpfile   *os.File
		canWrite       chan struct{}
		writeSrcReader io.ReadCloser
		mu             sync.RWMutex
		writeDirty     bool
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "ok",
			fields: fields{
				writeDirty: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fh := &objectFileHandle{
				mpuInfo:        tt.fields.mpuInfo,
				storage:        tt.fields.storage,
				name:           tt.fields.name,
				key:            tt.fields.key,
				size:           tt.fields.size,
				flags:          tt.fields.flags,
				writeTmpfile:   tt.fields.writeTmpfile,
				canWrite:       tt.fields.canWrite,
				writeSrcReader: tt.fields.writeSrcReader,
				mu:             tt.fields.mu,
				writeDirty:     tt.fields.writeDirty,
			}
			fh.Release()
		})
	}
}

func Test_objectFileHandle_Fsync(t *testing.T) {
	type fields struct {
		mpuInfo        mpuInfo
		storage        object.ObjectStorage
		name           string
		key            string
		size           uint64
		flags          uint32
		writeTmpfile   *os.File
		canWrite       chan struct{}
		writeSrcReader io.ReadCloser
		mu             sync.RWMutex
		writeDirty     bool
	}
	type args struct {
		flags int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "ok",
			args: args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fh := &objectFileHandle{
				mpuInfo:        tt.fields.mpuInfo,
				storage:        tt.fields.storage,
				name:           tt.fields.name,
				key:            tt.fields.key,
				size:           tt.fields.size,
				flags:          tt.fields.flags,
				writeTmpfile:   tt.fields.writeTmpfile,
				canWrite:       tt.fields.canWrite,
				writeSrcReader: tt.fields.writeSrcReader,
				mu:             tt.fields.mu,
				writeDirty:     tt.fields.writeDirty,
			}
			tt.wantErr(t, fh.Fsync(tt.args.flags), fmt.Sprintf("Fsync(%v)", tt.args.flags))
		})
	}
}

func Test_objectFileHandle_Truncate(t *testing.T) {
	type fields struct {
		mpuInfo        mpuInfo
		storage        object.ObjectStorage
		name           string
		key            string
		size           uint64
		flags          uint32
		writeTmpfile   *os.File
		canWrite       chan struct{}
		writeSrcReader io.ReadCloser
		mu             sync.RWMutex
		writeDirty     bool
	}
	type args struct {
		size uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:   "writeTmpfile err",
			fields: fields{},
			args:   args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fh := &objectFileHandle{
				mpuInfo:        tt.fields.mpuInfo,
				storage:        tt.fields.storage,
				name:           tt.fields.name,
				key:            tt.fields.key,
				size:           tt.fields.size,
				flags:          tt.fields.flags,
				writeTmpfile:   tt.fields.writeTmpfile,
				canWrite:       tt.fields.canWrite,
				writeSrcReader: tt.fields.writeSrcReader,
				mu:             tt.fields.mu,
				writeDirty:     tt.fields.writeDirty,
			}
			tt.wantErr(t, fh.Truncate(tt.args.size), fmt.Sprintf("Truncate(%v)", tt.args.size))
		})
	}
}

func Test_objectFileHandle_Allocate(t *testing.T) {
	type fields struct {
		mpuInfo        mpuInfo
		storage        object.ObjectStorage
		name           string
		key            string
		size           uint64
		flags          uint32
		writeTmpfile   *os.File
		canWrite       chan struct{}
		writeSrcReader io.ReadCloser
		mu             sync.RWMutex
		writeDirty     bool
	}
	type args struct {
		off  uint64
		size uint64
		mode uint32
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "ok",
			args: args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fh := &objectFileHandle{
				mpuInfo:        tt.fields.mpuInfo,
				storage:        tt.fields.storage,
				name:           tt.fields.name,
				key:            tt.fields.key,
				size:           tt.fields.size,
				flags:          tt.fields.flags,
				writeTmpfile:   tt.fields.writeTmpfile,
				canWrite:       tt.fields.canWrite,
				writeSrcReader: tt.fields.writeSrcReader,
				mu:             tt.fields.mu,
				writeDirty:     tt.fields.writeDirty,
			}
			tt.wantErr(t, fh.Allocate(tt.args.off, tt.args.size, tt.args.mode), fmt.Sprintf("Allocate(%v, %v, %v)", tt.args.off, tt.args.size, tt.args.mode))
		})
	}
}

func Test_objectFileSystem_Utimens(t *testing.T) {
	type fields struct {
		subPath     string
		dirMode     int
		fileMode    int
		storage     object.ObjectStorage
		defaultTime time.Time
		Mutex       sync.Mutex
		chunkPool   *sync.Pool
	}
	type args struct {
		name  string
		Atime *time.Time
		Mtime *time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "ok",
			args: args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &objectFileSystem{
				subPath:     tt.fields.subPath,
				dirMode:     tt.fields.dirMode,
				fileMode:    tt.fields.fileMode,
				storage:     tt.fields.storage,
				defaultTime: tt.fields.defaultTime,
				Mutex:       tt.fields.Mutex,
				chunkPool:   tt.fields.chunkPool,
			}
			tt.wantErr(t, fs.Utimens(tt.args.name, tt.args.Atime, tt.args.Mtime), fmt.Sprintf("Utimens(%v, %v, %v)", tt.args.name, tt.args.Atime, tt.args.Mtime))
		})
	}
}

func Test_objectFileSystem_String(t *testing.T) {
	type fields struct {
		subPath     string
		dirMode     int
		fileMode    int
		storage     object.ObjectStorage
		defaultTime time.Time
		Mutex       sync.Mutex
		chunkPool   *sync.Pool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "ok",
			want: "object",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &objectFileSystem{
				subPath:     tt.fields.subPath,
				dirMode:     tt.fields.dirMode,
				fileMode:    tt.fields.fileMode,
				storage:     tt.fields.storage,
				defaultTime: tt.fields.defaultTime,
				Mutex:       tt.fields.Mutex,
				chunkPool:   tt.fields.chunkPool,
			}
			assert.Equalf(t, tt.want, fs.String(), "String()")
		})
	}
}

func Test_objectFileHandle_partAndChunkSize(t *testing.T) {
	type fields struct {
		mpuInfo        mpuInfo
		storage        object.ObjectStorage
		name           string
		key            string
		size           uint64
		flags          uint32
		writeTmpfile   *os.File
		canWrite       chan struct{}
		writeSrcReader io.ReadCloser
		mu             sync.RWMutex
		writeDirty     bool
	}
	type args struct {
		fileSize int64
	}
	tests := []struct {
		name              string
		fields            fields
		args              args
		wantPartSize      int64
		wantChunkSize     int64
		wantPartsPerChunk int64
	}{
		{
			name:   "<8gib",
			fields: fields{},
			args: args{
				7 * GiB,
			},
			wantPartSize:      8 * MiB,
			wantChunkSize:     MPUChunkSize,
			wantPartsPerChunk: 128,
		},
		{
			name:   "72gib",
			fields: fields{},
			args: args{
				72 * GiB,
			},
			wantPartSize:      64 * MiB,
			wantChunkSize:     MPUChunkSize,
			wantPartsPerChunk: 16,
		},
		{
			name:   "1024gib",
			fields: fields{},
			args: args{
				1024 * GiB,
			},
			wantPartSize:      512 * MiB,
			wantChunkSize:     MPUChunkSize,
			wantPartsPerChunk: 2,
		},
		{
			name:   "3tb",
			fields: fields{},
			args: args{
				3 * 1024 * GiB,
			},
			wantPartSize:      1 * GiB,
			wantChunkSize:     MPUChunkSize,
			wantPartsPerChunk: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fh := &objectFileHandle{
				mpuInfo:        tt.fields.mpuInfo,
				storage:        tt.fields.storage,
				name:           tt.fields.name,
				key:            tt.fields.key,
				size:           tt.fields.size,
				flags:          tt.fields.flags,
				writeTmpfile:   tt.fields.writeTmpfile,
				canWrite:       tt.fields.canWrite,
				writeSrcReader: tt.fields.writeSrcReader,
				mu:             tt.fields.mu,
				writeDirty:     tt.fields.writeDirty,
			}
			gotPartSize, gotChunkSize, gotPartsPerChunk := fh.partAndChunkSize(tt.args.fileSize)
			assert.Equalf(t, tt.wantPartSize, gotPartSize, "partAndChunkSize(%v)", tt.args.fileSize)
			assert.Equalf(t, tt.wantChunkSize, gotChunkSize, "partAndChunkSize(%v)", tt.args.fileSize)
			assert.Equalf(t, tt.wantPartsPerChunk, gotPartsPerChunk, "partAndChunkSize(%v)", tt.args.fileSize)
		})
	}
}

func Test_objectFileHandle_MPU(t *testing.T) {
	type fields struct {
		mpuInfo        mpuInfo
		storage        object.ObjectStorage
		name           string
		key            string
		size           uint64
		flags          uint32
		writeTmpfile   *os.File
		canWrite       chan struct{}
		writeSrcReader io.ReadCloser
		mu             sync.RWMutex
		writeDirty     bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "err1",
			fields: fields{
				storage: object.S3Storage{},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
		{
			name: "err2",
			fields: fields{
				storage: object.S3Storage{},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
	}

	a := &object.S3Storage{}
	var p1 = gomonkey.ApplyMethod(reflect.TypeOf(a), "CreateMultipartUpload",
		func(a *object.S3Storage, key string) (*object.MultipartCommitOutPut, error) {
			return nil, fmt.Errorf("createMultipartUpload err")
		})
	defer p1.Reset()
	b := &objectFileHandle{}
	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(b), "serialMPUTillEnd",
		func(a *objectFileHandle, key string) error {
			return fmt.Errorf("serialMPUTillEnd err")
		})
	defer p2.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fh := &objectFileHandle{
				mpuInfo:        tt.fields.mpuInfo,
				storage:        tt.fields.storage,
				name:           tt.fields.name,
				key:            tt.fields.key,
				size:           tt.fields.size,
				flags:          tt.fields.flags,
				writeTmpfile:   tt.fields.writeTmpfile,
				canWrite:       tt.fields.canWrite,
				writeSrcReader: tt.fields.writeSrcReader,
				mu:             tt.fields.mu,
				writeDirty:     tt.fields.writeDirty,
			}
			tt.wantErr(t, fh.MPU(), fmt.Sprintf("MPU()"))
		})
		if tt.name == "err1" {
			p1 = gomonkey.ApplyMethod(reflect.TypeOf(a), "CreateMultipartUpload",
				func(a *object.S3Storage, key string) (*object.MultipartCommitOutPut, error) {
					return &object.MultipartCommitOutPut{UploadId: "t"}, nil
				})
		}
	}
}
