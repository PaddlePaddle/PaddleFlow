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
	"os"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

func TestSFTP(t *testing.T) {
	properties := make(map[string]interface{})
	if os.Getenv("SFTP_ADDRESS") == "" {
		log.Errorf("SFTP Client Init Fail")
		return
	}
	properties[common.Address] = os.Getenv("SFTP_ADDRESS")
	properties[common.SubPath] = os.Getenv("SFTP_ROOT")
	properties[common.UserKey] = os.Getenv("SFTP_USER")
	properties[common.Password] = os.Getenv("SFTP_PASSWORD")

	fs, err := NewSftpFileSystem(properties)

	assert.NoError(t, err)
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
	assert.Equal(t, fuse.OK, e)
	assert.Equal(t, len(content), n)
	fh.Release()

	entries, err := fs.ReadDir("/")
	assert.NoError(t, err)
	assert.LessOrEqual(t, 1, len(entries))

}
