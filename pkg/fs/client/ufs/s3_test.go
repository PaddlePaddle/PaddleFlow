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
	"os"
	"reflect"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

// =================== test utils ===================//

func NewS3FSForTest() (UnderFileStorage, error) {
	if os.Getenv("S3_AK") == "" {
		log.Errorf("S3 Client Init Fail")
		return nil, nil
	}
	properties := make(map[string]interface{})
	properties[common.Endpoint] = os.Getenv("S3_ENDPOINT")
	properties[common.Region] = os.Getenv("S3_REGION")
	properties[common.Bucket] = os.Getenv("S3_BUCKET")
	properties[common.AccessKey] = os.Getenv("S3_AK")
	properties[common.SecretKey] = os.Getenv("S3_SK_ENCRYPTED")
	properties[common.SubPath] = os.Getenv("S3_PATH")

	fs, err := NewS3FileSystem(properties)
	return fs, err
}

func cleanS3TestDir(fs UnderFileStorage, dir string) error {
	list, err := fs.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, v := range list {
		fs.Unlink(dir + v.Name)
	}
	err = fs.Rmdir(dir)
	return err
}

func createS3TestFile(fs UnderFileStorage, name string, content string) error {
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

// =================== test utils ends ===================//

func TestS3(t *testing.T) {
	fs, err := NewS3FSForTest()
	assert.NoError(t, err)
	if fs == nil {
		return
	}

	dir := "/helloWorld/"
	if _, err := fs.GetAttr(dir); err == nil {
		fs.Rmdir(dir)
	}

	err = fs.Mkdir(dir, 0)
	assert.Equal(t, nil, err)

	finfo, err := fs.GetAttr(dir)
	assert.NotNil(t, finfo)
	assert.True(t, finfo.IsDir)

	fileName := dir + "hello"
	fh, err := fs.Create(fileName, uint32(os.O_WRONLY|os.O_CREATE), 0755)
	if err == syscall.EEXIST {
		fs.Unlink(fileName)
		fh, err = fs.Create(fileName, uint32(os.O_WRONLY|os.O_CREATE), 0755)
		assert.NoError(t, err)
	}

	content := []byte("hello world")
	fh.Write(content, 0)
	fh.Flush()
	fh.Release()

	fh, err = fs.Open(fileName, uint32(os.O_RDONLY), 11)
	assert.NoError(t, err)
	buf := make([]byte, 4)
	n, e := fh.Read(buf, 0)
	assert.Nil(t, e)
	assert.Equal(t, 4, n)
	fh.Release()

	// test no prefix
	_, err = fs.Open("helloWorld/hello", uint32(os.O_WRONLY), 11)
	assert.Nil(t, err)

	entries, err := fs.ReadDir("")
	assert.NoError(t, err)
	assert.Less(t, 0, len(entries))

	cleanS3TestDir(fs, dir)
}

func TestS3Truncate(t *testing.T) {
	fs, err := NewS3FSForTest()
	assert.Equal(t, nil, err)
	if fs == nil {
		return
	}
	dir := "test_truncate/"
	file := dir + "to_trunc"
	err = fs.Mkdir(dir, 0)
	assert.Nil(t, err)
	err = createS3TestFile(fs, file, "")
	assert.Nil(t, err)
	finfo, err := fs.GetAttr(file)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), finfo.Size)

	// fh
	fh, err := fs.Open(file, uint32(os.O_WRONLY), uint64(finfo.Size))
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

	err = cleanS3TestDir(fs, dir)
	assert.Nil(t, err)
}

func TestS3IsEmptyDir(t *testing.T) {
	fs, err := NewS3FSForTest()
	assert.Equal(t, nil, err)
	if fs == nil {
		return
	}
	dir1 := "rename/a1/"
	err = fs.Mkdir(dir1, 0)
	assert.Equal(t, nil, err)
	isEmptyDir, err := fs.(*s3FileSystem).isEmptyDir(dir1)
	assert.Equal(t, true, isEmptyDir)
	assert.Equal(t, nil, err)
	err = createS3TestFile(fs, dir1+"abc", "tttt")
	assert.Equal(t, nil, err)
	isEmptyDir, err = fs.(*s3FileSystem).isEmptyDir(dir1)
	assert.Equal(t, true, isEmptyDir)
	assert.Equal(t, syscall.ENOTEMPTY, err)
	cleanS3TestDir(fs, dir1)
}

func TestS3Rename_ReNameErr(t *testing.T) {
	fs, err := NewS3FSForTest()
	assert.Equal(t, nil, err)
	if fs == nil {
		return
	}
	// rename("nonempty_dir1", "nonempty_dir2") = ENOTEMPTY
	rename1 := "renametest/a/"
	rename2 := "renametest/b/"
	err = fs.Mkdir(rename1, 0)
	assert.Equal(t, nil, err)
	err = fs.Mkdir(rename2, 0)
	assert.Equal(t, nil, err)
	err = createS3TestFile(fs, rename1+"a", "test")
	assert.Equal(t, nil, err)
	err = createS3TestFile(fs, rename2+"b", "test")
	assert.Equal(t, nil, err)

	err = fs.Rename(rename1, rename2)
	assert.Equal(t, syscall.ENOTEMPTY, err)

	// rename("file", "dir") = EISDIR
	rename3 := "renametest/c/"
	err = fs.Mkdir(rename3, 0)
	assert.Equal(t, nil, err)
	err = fs.Rename(rename1+"a", rename3)
	assert.Equal(t, syscall.EISDIR, err)

	// rename("dir", "file") = ENOTDIR
	err = fs.Rename(rename3, rename1+"a")
	assert.Equal(t, syscall.ENOTDIR, err)

	cleanS3TestDir(fs, rename1)
	cleanS3TestDir(fs, rename2)
	cleanS3TestDir(fs, rename3)
}

func TestS3ListV2(t *testing.T) {
	fs, err := NewS3FSForTest()
	assert.Equal(t, nil, err)
	if fs == nil {
		return
	}

	// create list test dir
	subDir := "testListDir/"
	err = fs.Mkdir(subDir, 0)
	assert.Equal(t, nil, err)

	// test list current empty dir
	fileInfo, continuationToken, err := fs.(*s3FileSystem).list(subDir, "", 1000, false)
	assert.NoError(t, err)
	assert.Equal(t, "", continuationToken)
	assert.Equal(t, 1, len(fileInfo))

	filePrefix := subDir + "file-"
	sz := 10
	for i := 0; i < sz; i++ {
		err = createS3TestFile(fs, filePrefix+strconv.Itoa(i), strconv.Itoa(i*i))
		assert.Equal(t, nil, err)
	}

	// test list all
	fileInfo, continuationToken, err = fs.(*s3FileSystem).list(subDir, "", 1000, true)
	assert.NoError(t, err)
	assert.Equal(t, "", continuationToken)
	assert.Equal(t, sz+1, len(fileInfo)) // +1 including itself .

	// test truncated
	limit := 2
	fileInfo, continuationToken, err = fs.(*s3FileSystem).list(subDir, "", limit, false)
	assert.NoError(t, err)
	assert.NotEqual(t, "", continuationToken)
	assert.Equal(t, limit, len(fileInfo))

	fileInfo, continuationToken, err = fs.(*s3FileSystem).list(subDir, continuationToken, 100000, false)
	assert.NoError(t, err)
	assert.Equal(t, "", continuationToken)
	assert.Equal(t, sz-limit+1, len(fileInfo))

	cleanS3TestDir(fs, subDir)
}

func TestS3Rename_ReNameOk(t *testing.T) {
	fs, err := NewS3FSForTest()
	assert.Equal(t, nil, err)
	if fs == nil {
		return
	}

	// rename("any", "not_exists") = ok
	rename1 := "renametest/a/"
	rename2 := "renametest/b/"
	err = fs.Mkdir(rename1, 0)
	assert.Equal(t, nil, err)

	n := 10
	for i := 0; i < n; i++ {
		err = createS3TestFile(fs, rename1+strconv.Itoa(i), strconv.Itoa(i*i))
		assert.Equal(t, nil, err)
	}
	err = fs.Rename(rename1, rename2)
	assert.Equal(t, nil, err)

	list, err := fs.ReadDir(rename2)
	assert.Equal(t, nil, err)
	assert.Equal(t, 10, len(list))

	list, err = fs.ReadDir(rename1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(list))

	// rename("file1", "file2") = ok
	fileRename := "fRname"
	err = fs.Rename(rename2+strconv.Itoa(n-1), rename2+fileRename)
	assert.Equal(t, nil, err)
	ioR, err := fs.Get(rename2+fileRename, uint32(os.O_RDONLY), 0, 10)
	assert.Equal(t, nil, err)
	d := make([]byte, 2)
	ioR.Read(d)
	assert.Equal(t, string(d), strconv.Itoa((n-1)*(n-1)))

	cleanS3TestDir(fs, rename2)

	// rename("empty_dir1", "empty_dir2") = ok
	rename3 := "renametest/c/"
	rename4 := "renametest/d/"
	err = fs.Mkdir(rename3, 0)
	assert.Equal(t, nil, err)
	err = fs.Mkdir(rename4, 0)
	assert.Equal(t, nil, err)
	err = fs.Rename(rename3, rename4)
	assert.Equal(t, nil, err)
	cleanS3TestDir(fs, rename4)

	// rename("nonempty_dir1", "empty_dir2") = ok
	rename5 := "renametest/e/"
	rename6 := "renametest/f/"
	err = fs.Mkdir(rename5, 0)
	assert.Equal(t, nil, err)
	err = createS3TestFile(fs, rename5+"testfile", "test for rename")
	assert.Equal(t, nil, err)
	err = fs.Mkdir(rename6, 0)
	assert.Equal(t, nil, err)
	err = fs.Rename(rename5, rename6)
	assert.Equal(t, nil, err)
	list, err = fs.ReadDir(rename6)
	assert.Equal(t, nil, err)
	assert.Equal(t, "testfile", list[0].Name)
	cleanS3TestDir(fs, rename6)
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
