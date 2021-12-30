/*
 * Copyright (c) 2021 Baidu.com, Inc. All Rights Reserved
 * Author: xuxiaoping01@baidu.com
 * File: ufs_test.go
 * LastModified: 2021/08/1 16:01:25
 */
package ufs

import (
	"encoding/base64"
	"os"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/fs/client/base"
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
	fh, err = fs.Open("hello", uint32(os.O_RDONLY))
	assert.NoError(t, err)
	buf := make([]byte, 20)
	r, e := fh.Read(buf, 0)
	assert.Equal(t, fuse.OK, e)
	data, code := r.Bytes(buf)
	assert.Equal(t, fuse.OK, code)
	assert.Equal(t, len(content), len(data))
	fh.Release()
	entries, err := fs.ReadDir("/")
	assert.NoError(t, err)
	assert.LessOrEqual(t, 1, len(entries))
}
func TestHdfs(t *testing.T) {
	properties := make(map[string]interface{})
	properties[base.NameNodeAddress] = os.Getenv("HDFS_ADDR")
	properties[base.UserKey] = os.Getenv("HDFS_USER")
	root := "/ufs/hdfs"
	properties[base.SubPath] = root
	fs, err := NewHdfsFileSystem(properties)
	assert.NoError(t, err)
	testFsOp(t, fs)
}

func TestHdfsWithKerberos(t *testing.T) {
	properties := make(map[string]interface{})
	properties[base.NameNodeAddress] = os.Getenv("KRB5_ADDR")
	properties[base.UserKey] = os.Getenv("KRB5_USER")
	keyTabPath := os.Getenv("KRB5_KEYTABPATH")
	keyTabData, err := os.ReadFile(keyTabPath)
	properties[base.KeyTabData] = base64.StdEncoding.EncodeToString(keyTabData)
	properties[base.Kdc] = os.Getenv("KRB5_KDC")
	properties[base.Realm] = os.Getenv("KRB_REALM")
	properties[base.Principal] = os.Getenv("KRB5_PRINCIPAL")
	properties[base.NameNodePrincipal] = os.Getenv("KRB5_NAMENODEPRINCIPAL")
	properties[base.DataTransferProtection] = os.Getenv("KRB5_DATATRANSFERPROTECTION")
	properties[base.SubPath] = "/ufs/hdfs"
	fs, err := NewHdfsWithKerberosFileSystem(properties)
	assert.NoError(t, err)
	testFsOp(t, fs)
}
