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
	"syscall"
	"testing"

	"paddleflow/pkg/fs/client/base"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
)

func TestS3(t *testing.T) {
	properties := make(map[string]interface{})
	properties[base.Endpoint] = os.Getenv("S3_ENDPOINT")
	properties[base.AccessKey] = os.Getenv("S3_AK")
	properties[base.SecretKey] = os.Getenv("S3_SK")
	properties[base.Bucket] = os.Getenv("S3_BUCKET")
	properties[base.Region] = os.Getenv("S3_REGION")
	properties[base.SubPath] = "/ufs/test"

	fs, err := NewS3FileSystem(properties)
	assert.NoError(t, err)

	if _, err := fs.GetAttr("hello"); err == nil {
		fs.Rmdir("hello")
	}

	if _, err := fs.GetAttr("my"); err != nil {
		assert.NoError(t, fs.Mkdir("my", 0))
	}

	finfo, err := fs.GetAttr("my") // exists
	assert.NotNil(t, finfo)
	assert.True(t, finfo.IsDir)

	fh, err := fs.Create("hello", uint32(os.O_WRONLY|os.O_CREATE), 0755)
	if err == syscall.EEXIST {
		fs.Unlink("hello")
		fh, err = fs.Create("hello", uint32(os.O_WRONLY|os.O_CREATE), 0755)
		assert.NoError(t, err)
	}

	content := []byte("hello world")
	fh.Write(content, 0)
	fh.Flush()
	fh.Release()

	fh, err = fs.Open("/hello", uint32(os.O_RDONLY))
	assert.NoError(t, err)
	buf := make([]byte, 4)
	r, e := fh.Read(buf, 0)
	assert.Equal(t, fuse.OK, e)
	data, code := r.Bytes(buf)
	assert.Equal(t, fuse.OK, code)
	assert.Equal(t, 4, len(data))
	fh.Release()

	_, err = fs.Open("hello", uint32(os.O_WRONLY))
	assert.Error(t, err)

	entries, err := fs.ReadDir("")
	assert.NoError(t, err)
	assert.Less(t, 0, len(entries))

}
