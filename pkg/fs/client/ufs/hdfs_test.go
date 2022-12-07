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
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/colinmarc/hdfs/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

func testFsOp(t *testing.T, fs UnderFileStorage) {
	assert.NotNil(t, fs)
	if _, err := fs.GetAttr("test"); err == nil {
		fs.Rmdir("test")
	}
	assert.NoError(t, fs.Mkdir("test", 0755))
	finfo, err := fs.GetAttr("test")
	assert.NoError(t, err)
	assert.Equal(t, int64(4096), finfo.Size)
	if _, err := fs.GetAttr("hello"); err == nil {
		assert.NoError(t, fs.Unlink("hello"))
	}
	fh, err := fs.Create("hello", uint32(os.O_WRONLY|os.O_CREATE), 0755)
	assert.NoError(t, err)
	content := []byte("hello world")
	fh.Write(content, 0)
	fh.Flush()
	fh.Release()
	fh, err = fs.Open("hello", uint32(os.O_RDONLY), 11)
	assert.NoError(t, err)
	buf := make([]byte, 20)
	n, e := fh.Read(buf, 0)
	assert.Nil(t, e)
	assert.Equal(t, len(content), n)
	fh.Release()
	entries, err := fs.ReadDir("/")
	assert.NoError(t, err)
	assert.LessOrEqual(t, 1, len(entries))
}
func TestHdfs(t *testing.T) {
	if os.Getenv("HDFS_USER") == "" {
		log.Errorf("HDFS Client Init Fail")
		return
	}
	properties := make(map[string]interface{})
	properties[common.NameNodeAddress] = os.Getenv("HDFS_ADDR")
	properties[common.UserKey] = os.Getenv("HDFS_USER")
	root := "/ufs/hdfs"
	properties[common.SubPath] = root
	fs, err := NewHdfsFileSystem(properties)
	assert.NoError(t, err)
	testFsOp(t, fs)
}

func TestHdfsWithKerberos(t *testing.T) {
	if os.Getenv("KRB5_USER") == "" {
		log.Errorf("HDFS Client Init Fail")
		return
	}
	properties := make(map[string]interface{})
	properties[common.NameNodeAddress] = os.Getenv("KRB5_ADDR")
	properties[common.UserKey] = os.Getenv("KRB5_USER")
	keyTabPath := os.Getenv("KRB5_KEYTABPATH")
	keyTabData, err := os.ReadFile(keyTabPath)
	properties[common.KeyTabData] = base64.StdEncoding.EncodeToString(keyTabData)
	properties[common.Kdc] = os.Getenv("KRB5_KDC")
	properties[common.Realm] = os.Getenv("KRB_REALM")
	properties[common.Principal] = os.Getenv("KRB5_PRINCIPAL")
	properties[common.NameNodePrincipal] = os.Getenv("KRB5_NAMENODEPRINCIPAL")
	properties[common.DataTransferProtection] = os.Getenv("KRB5_DATATRANSFERPROTECTION")
	properties[common.SubPath] = "/ufs/hdfs"
	fs, err := NewHdfsWithKerberosFileSystem(properties)
	assert.NoError(t, err)
	testFsOp(t, fs)
}

func TestHdfsTurncate(t *testing.T) {

	var p1 = gomonkey.ApplyMethod(reflect.TypeOf(&hdfsFileSystem{}), "Unlink", func(_ *hdfsFileSystem, name string) error {
		return nil
	})
	defer p1.Reset()
	var p2 = gomonkey.ApplyMethod(reflect.TypeOf(&hdfsFileSystem{}), "Create", func(_ *hdfsFileSystem, name string, flags, mode uint32) (fd FileHandle, err error) {
		return &hdfsFileHandle{}, nil
	})
	defer p2.Reset()
	fs := &hdfsFileSystem{}
	err := fs.Truncate("test", 0)
	assert.Nil(t, err)
	p1 = gomonkey.ApplyMethod(reflect.TypeOf(&hdfsFileSystem{}), "Unlink", func(_ *hdfsFileSystem, name string) error {
		return errors.New("Unlink failed")
	})
	defer p1.Reset()
	err = fs.Truncate("test", 0)
	assert.NotNil(t, err)

}

func Test_hdfsFileSystem_shouldRetry(t *testing.T) {
	type fields struct {
		client      *hdfs.Client
		subpath     string
		blockSize   int64
		replication int
		Mutex       sync.Mutex
	}
	type args struct {
		err error
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name:   "has err",
			fields: fields{},
			args: args{
				err: errors.New("append call failed with ERROR_APPLICATION (org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException"),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &hdfsFileSystem{
				client:      tt.fields.client,
				subpath:     tt.fields.subpath,
				blockSize:   tt.fields.blockSize,
				replication: tt.fields.replication,
				Mutex:       tt.fields.Mutex,
			}
			assert.Equalf(t, tt.want, fs.shouldRetry(tt.args.err), "shouldRetry(%v)", tt.args.err)
		})
	}
}

func Test_hdfsFileHandle_Read(t *testing.T) {
	type fields struct {
		name   string
		fs     *hdfsFileSystem
		writer *hdfs.FileWriter
		reader *hdfs.FileReader
	}
	type args struct {
		buf []byte
		off uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "reader fail",
			fields: fields{
				name: "1",
				fs:   &hdfsFileSystem{client: &hdfs.Client{}},
			},
			args: args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
	}
	var p2 = gomonkey.ApplyMethod(reflect.TypeOf(&hdfs.Client{}), "Open", func(_ *hdfs.Client, name string) (*hdfs.FileReader, error) {
		return nil, fmt.Errorf("oepn fail")
	})
	defer p2.Reset()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fh := &hdfsFileHandle{
				name:   tt.fields.name,
				fs:     tt.fields.fs,
				writer: tt.fields.writer,
				reader: tt.fields.reader,
			}
			got, err := fh.Read(tt.args.buf, tt.args.off)
			if !tt.wantErr(t, err, fmt.Sprintf("Read(%v, %v)", tt.args.buf, tt.args.off)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Read(%v, %v)", tt.args.buf, tt.args.off)
		})
	}
}

func Test_hdfsFileHandle_Write(t *testing.T) {
	type fields struct {
		name   string
		fs     *hdfsFileSystem
		writer *hdfs.FileWriter
		reader *hdfs.FileReader
	}
	type args struct {
		data []byte
		off  uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    uint32
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "write fail",
			fields: fields{
				name: "1",
				fs:   &hdfsFileSystem{client: &hdfs.Client{}},
			},
			args: args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
	}
	var p2 = gomonkey.ApplyMethod(reflect.TypeOf(&hdfs.Client{}), "Append", func(_ *hdfs.Client, name string) (*hdfs.FileWriter, error) {
		return nil, fmt.Errorf("org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException")
	})
	defer p2.Reset()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fh := &hdfsFileHandle{
				name:   tt.fields.name,
				fs:     tt.fields.fs,
				writer: tt.fields.writer,
				reader: tt.fields.reader,
			}
			got, err := fh.Write(tt.args.data, tt.args.off)
			if !tt.wantErr(t, err, fmt.Sprintf("Write(%v, %v)", tt.args.data, tt.args.off)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Write(%v, %v)", tt.args.data, tt.args.off)
		})
	}
}
