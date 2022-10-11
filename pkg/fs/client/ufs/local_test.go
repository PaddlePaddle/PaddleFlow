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
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

func TestFile(t *testing.T) {
	properties := make(map[string]interface{})

	root := "/tmp/ufs/local"
	if _, err := os.Stat(root); err == nil {
		os.RemoveAll(root)
	}
	os.MkdirAll(root, 0755)

	properties[common.SubPath] = root
	fs, err := NewLocalFileSystem(properties)
	assert.NoError(t, err)
	assert.NotNil(t, fs)
	assert.NoError(t, fs.Mkdir("test", 0755))

	finfo, err := fs.GetAttr("test")
	assert.NoError(t, err)
	sysType := runtime.GOOS
	if sysType == "darwin" {
		assert.Equal(t, int64(64), finfo.Size)
	} else if sysType == "linux" {
		assert.Equal(t, int64(4096), finfo.Size)
	}
	assert.Equal(t, true, finfo.IsDir)

	fh, err := fs.Create("hello", uint32(os.O_WRONLY|os.O_CREATE), 0755)
	assert.NoError(t, err)
	content := []byte("hello world")
	fh.Write(content, 0)
	fh.Flush()
	fh.Release()

	finfo, err = fs.GetAttr("hello")
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), finfo.Size)
	assert.Equal(t, false, finfo.IsDir)

	fh, err = fs.Open("hello", uint32(os.O_RDONLY), 11)
	assert.NoError(t, err)
	buf := make([]byte, 20)
	n, e := fh.Read(buf, 0)
	assert.Nil(t, e)
	assert.Equal(t, len(content), n)
	fh.Release()

	fh2, err := fs.Open("hello", uint32(os.O_WRONLY), 11)
	assert.NoError(t, err)
	content = []byte("headfsallo world")
	nWritten, eWritten := fh2.Write(content, 11)
	assert.Nil(t, eWritten)
	assert.Equal(t, len(content), int(nWritten))
	fh2.Flush()
	fh2.Release()

	entries, err := fs.ReadDir("/")
	assert.NoError(t, err)
	assert.LessOrEqual(t, 1, len(entries))
}
