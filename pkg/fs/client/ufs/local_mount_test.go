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

	"github.com/stretchr/testify/assert"
)

func TestLocalMount(t *testing.T) {
	os.MkdirAll("./mock", 0755)
	fs := &localMount{
		localPath: "./mock",
	}
	fs.Mkdir("data1", 0755)
	fs.Mkdir("data1/data2", 0755)

	finfo, err := fs.GetAttr("data1/data2")
	assert.NoError(t, err)
	assert.Equal(t, true, finfo.IsDir)

	fh, err := fs.Create("data1/hello", uint32(os.O_WRONLY|os.O_CREATE), 0755)
	assert.NoError(t, err)
	content := []byte("hello world")
	fh.Write(content, 0)
	fh.Flush()
	fh.Release()

	fh, err = fs.Open("data1/hello", uint32(os.O_RDONLY), 11)
	assert.NoError(t, err)
	buf := make([]byte, 20)
	n, e := fh.Read(buf, 0)
	assert.Nil(t, e)
	assert.Equal(t, len(content), n)
	fh.Release()

	entries, err := fs.ReadDir("data1")
	assert.NoError(t, err)
	assert.LessOrEqual(t, 1, len(entries))

	out := fs.StatFs("data1/hello")
	assert.NotNil(t, out)

	err = fs.Rmdir("data1/data2")
	assert.NoError(t, err)

	os.RemoveAll("./mock")
}
