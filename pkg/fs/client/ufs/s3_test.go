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

package ufs

import (
	"fmt"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	fsCommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

//***************mocks3service
const (
	TESTACCESSKEY  = "11111111111111111111111eeeeeeeee"
	TESTSECRETKEY  = "11111111111111111111111eeeeeeeee"
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

// =================== test utils ===================//
func NewS3FSForTest() (UnderFileStorage, error) {
	endPoint := newS3Service()
	properties := make(map[string]interface{})
	properties[common.Endpoint] = endPoint
	properties[common.Region] = TESTREGION
	properties[common.Bucket] = TESTBUCKETNAME
	properties[common.AccessKey] = TESTACCESSKEY
	properties[common.SecretKey] = TESTSECRETKEY
	properties[common.SubPath] = ""
	properties[fsCommon.S3ForcePathStyle] = "true"
	fs, err := NewS3FileSystem(properties)
	return fs, err
}

func TestS3DirOp(t *testing.T) {
	fs, err := NewS3FSForTest()
	defer func() {
		os.RemoveAll("./tmp")
	}()
	assert.NotNil(t, fs)
	assert.Nil(t, err)
	//test create dir
	err = fs.Mkdir("dir1", 755)
	assert.Nil(t, err)
	err = fs.Mkdir("dir2", 755)
	assert.Nil(t, err)

	// test get dir attr
	info, err := fs.GetAttr("dir2")
	assert.Nil(t, err)
	assert.True(t, true, info.IsDir)

	err = fs.Mkdir("dir1/dir1-1/", 755)
	assert.Nil(t, err)
	err = fs.Mkdir("dir1/dir1-2/", 755)
	assert.Nil(t, err)

	//test read dir
	dirEntry, err := fs.ReadDir("dir1")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(dirEntry))
	for _, entry := range dirEntry {
		println(entry.Name)
	}

	//test rm dir
	err = fs.Rmdir("dir1/dir1-1")
	assert.Nil(t, err)
	entrys, err := fs.ReadDir("dir1")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(entrys))

	err = fs.Rmdir("dir1/dir1-2")
	assert.Nil(t, err)

}

func TestS3FileOp(t *testing.T) {
	fs, err := NewS3FSForTest()
	defer func() {
		os.RemoveAll("./tmp")
	}()
	assert.NotNil(t, fs)
	assert.Nil(t, err)
	//test create
	dir := "dir"
	err = fs.Mkdir(dir, 755)
	assert.Nil(t, err)
	file := dir + "/foo"
	fh, err := fs.Create(file, uint32(flags), mode)
	assert.Nil(t, err)

	//test write
	data := []byte("hello world")
	_, err = fh.Write(data, 0)
	assert.Nil(t, err)
	fh.Flush()
	fh.Release()

	//test read
	fh, err = fs.Open(file, syscall.O_RDONLY, 11)
	assert.Nil(t, err)
	buffer := make([]byte, 4)
	num, err := fh.Read(buffer, 0)
	assert.Nil(t, err)
	assert.Equal(t, 4, num)

	//test unlink
	entrys, err := fs.ReadDir(dir)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(entrys))
	err = fs.Unlink(file)
	assert.Nil(t, err)
	entrys, err = fs.ReadDir(dir)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(entrys))
}
func TestS3Truncat(t *testing.T) {
	defer func() {
		os.RemoveAll("./tmp")
	}()
	fs, err := NewS3FSForTest()
	assert.Equal(t, nil, err)
	assert.NotNil(t, fs)

	file := "truncate"

	fh, err := fs.Create(file, uint32(flags), mode)
	assert.NotNil(t, fh)
	assert.Nil(t, err)

	data := []byte("hello world")
	_, err = fh.Write(data, 0)
	assert.Nil(t, err)
	fh.Flush()
	fh.Release()

	finfo, err := fs.GetAttr(file)
	if err != nil {
		fmt.Printf("err: %s", err.Error())
	}
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
}

func Test_s3FileSystem_getFullPath(t *testing.T) {
	type fields struct {
		bucket      string
		subpath     string
		dirMode     int
		fileMode    int
		sess        *session.Session
		s3          *s3.S3
		defaultTime time.Time
		Mutex       sync.Mutex
		chunkPool   *sync.Pool
	}
	type args struct {
		name string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "file",
			fields: fields{
				subpath: Delimiter,
			},
			args: args{
				name: "a",
			},
			want: "a",
		},
		{
			name: "dir",
			fields: fields{
				subpath: Delimiter,
			},
			args: args{
				name: "a/",
			},
			want: "a/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &s3FileSystem{
				bucket:      tt.fields.bucket,
				subpath:     tt.fields.subpath,
				dirMode:     tt.fields.dirMode,
				fileMode:    tt.fields.fileMode,
				sess:        tt.fields.sess,
				s3:          tt.fields.s3,
				defaultTime: tt.fields.defaultTime,
				Mutex:       tt.fields.Mutex,
				chunkPool:   tt.fields.chunkPool,
			}
			assert.Equalf(t, tt.want, fs.getFullPath(tt.args.name), "getFullPath(%v)", tt.args.name)
		})
	}
}

func Test_s3FileSystem_list(t *testing.T) {
	type fields struct {
		bucket      string
		subpath     string
		dirMode     int
		fileMode    int
		sess        *session.Session
		s3          *s3.S3
		defaultTime time.Time
		Mutex       sync.Mutex
		chunkPool   *sync.Pool
	}
	type args struct {
		name              string
		continuationToken string
		limit             int
		recursive         bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []base.FileInfo
		want1   string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "list fail",
			fields: fields{
				subpath: Delimiter,
			},
			args: args{
				name: Delimiter,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
	}
	a := &s3.S3{}
	var p3 = gomonkey.ApplyMethod(reflect.TypeOf(a), "ListObjectsV2",
		func(a *s3.S3, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
			return nil, fmt.Errorf("list err")
		})
	defer p3.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &s3FileSystem{
				bucket:      tt.fields.bucket,
				subpath:     tt.fields.subpath,
				dirMode:     tt.fields.dirMode,
				fileMode:    tt.fields.fileMode,
				sess:        tt.fields.sess,
				s3:          tt.fields.s3,
				defaultTime: tt.fields.defaultTime,
				Mutex:       tt.fields.Mutex,
				chunkPool:   tt.fields.chunkPool,
			}
			got, got1, err := fs.list(tt.args.name, tt.args.continuationToken, tt.args.limit, tt.args.recursive)
			if !tt.wantErr(t, err, fmt.Sprintf("list(%v, %v, %v, %v)", tt.args.name, tt.args.continuationToken, tt.args.limit, tt.args.recursive)) {
				return
			}
			assert.Equalf(t, tt.want, got, "list(%v, %v, %v, %v)", tt.args.name, tt.args.continuationToken, tt.args.limit, tt.args.recursive)
			assert.Equalf(t, tt.want1, got1, "list(%v, %v, %v, %v)", tt.args.name, tt.args.continuationToken, tt.args.limit, tt.args.recursive)
		})
	}
}

// func cleanS3TestDir(fs UnderFileStorage, dir string) error {
// 	list, err := fs.ReadDir(dir)
// 	if err != nil {
// 		return err
// 	}
// 	for _, v := range list {
// 		fs.Unlink(dir + v.Name)
// 	}
// 	err = fs.Rmdir(dir)
// 	return err
// }

// func createS3TestFile(fs UnderFileStorage, name string, content string) error {
// 	fh, err := fs.Create(name, uint32(os.O_WRONLY|os.O_CREATE), 0755)
// 	if err == syscall.EEXIST {
// 		fs.Unlink(name)
// 		fh, err = fs.Create(name, uint32(os.O_WRONLY|os.O_CREATE), 0755)
// 		if err != nil {
// 			return err
// 		}
// 	} else if err != nil {
// 		return err
// 	}
// 	fh.Write([]byte(content), 0)
// 	fh.Flush()
// 	fh.Release()
// 	return nil
// }

// func TestS3Rename_ReNameOk(t *testing.T) {
// 	fs, err := NewS3FSForTest()
// 	assert.Equal(t, nil, err)
// 	if fs == nil {
// 		return
// 	}

// 	// rename("any", "not_exists") = ok
// 	rename1 := "renametest/a/"
// 	rename2 := "renametest/b/"
// 	err = fs.Mkdir(rename1, 0)
// 	assert.Equal(t, nil, err)

// 	n := 10
// 	for i := 0; i < n; i++ {
// 		err = createS3TestFile(fs, rename1+strconv.Itoa(i), strconv.Itoa(i*i))
// 		assert.Equal(t, nil, err)
// 	}
// 	err = fs.Rename(rename1, rename2)
// 	assert.Equal(t, nil, err)

// 	list, err := fs.ReadDir(rename2)
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, 10, len(list))

// 	list, err = fs.ReadDir(rename1)
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, 0, len(list))

// 	// rename("file1", "file2") = ok
// 	fileRename := "fRname"
// 	err = fs.Rename(rename2+strconv.Itoa(n-1), rename2+fileRename)
// 	assert.Equal(t, nil, err)
// 	ioR, err := fs.Get(rename2+fileRename, uint32(os.O_RDONLY), 0, 10)
// 	assert.Equal(t, nil, err)
// 	d := make([]byte, 2)
// 	ioR.Read(d)
// 	assert.Equal(t, string(d), strconv.Itoa((n-1)*(n-1)))

// 	cleanS3TestDir(fs, rename2)

// 	// rename("empty_dir1", "empty_dir2") = ok
// 	rename3 := "renametest/c/"
// 	rename4 := "renametest/d/"
// 	err = fs.Mkdir(rename3, 0)
// 	assert.Equal(t, nil, err)
// 	err = fs.Mkdir(rename4, 0)
// 	assert.Equal(t, nil, err)
// 	err = fs.Rename(rename3, rename4)
// 	assert.Equal(t, nil, err)
// 	cleanS3TestDir(fs, rename4)

// 	// rename("nonempty_dir1", "empty_dir2") = ok
// 	rename5 := "renametest/e/"
// 	rename6 := "renametest/f/"
// 	err = fs.Mkdir(rename5, 0)
// 	assert.Equal(t, nil, err)
// 	err = createS3TestFile(fs, rename5+"testfile", "test for rename")
// 	assert.Equal(t, nil, err)
// 	err = fs.Mkdir(rename6, 0)
// 	assert.Equal(t, nil, err)
// 	err = fs.Rename(rename5, rename6)
// 	assert.Equal(t, nil, err)
// 	list, err = fs.ReadDir(rename6)
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, "testfile", list[0].Name)
// 	cleanS3TestDir(fs, rename6)
// }
