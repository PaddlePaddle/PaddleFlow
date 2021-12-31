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

package fuse

import (
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/vfs"
)

func TestNewFileSystem(t *testing.T) {
	debug := true
	fs := NewFileSystem(debug)
	assert.NotNil(t, fs)
	assert.Equal(t, fs.debug, debug)
}

func TestFileSystem_String(t *testing.T) {
	fs := NewFileSystem(true)
	assert.NotNil(t, fs)
	assert.Equal(t, fs.String(), fsName)
}

func TestFileSystem_SetDebug(t *testing.T) {
	fs := NewFileSystem(true)
	fs.SetDebug(false)
	assert.NotNil(t, fs)
	assert.Equal(t, fs.debug, false)
}

func TestFileSystem_GetAttr(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock", 0755)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	_, status := fs.GetAttr("", nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_Chmod(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock", 0755)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	status := fs.Chmod("", 0, nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_Chown(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	status := fs.Chown("/abcd", 601, 601, nil)
	assert.NotNil(t, fs)
	if runtime.GOOS == "darwin" {
		// mac下没有权限
		assert.Equal(t, status, fuse.Status(45))
	} else {
		assert.Equal(t, status, fuse.Status(0))
	}
	os.RemoveAll("./mock")
}

func TestFileSystem_Utimens(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	testTime := time.Now()
	status := fs.Utimens("/abcd", &testTime, &testTime, nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_Truncate(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd", 0777)
	file, _ := os.Create("./mock/abcd/file")
	defer file.Close()
	io.WriteString(file, "tesabcd")
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	status := fs.Truncate("abcd/file", 2, nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_Access(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	status := fs.Access("abcd", 502, nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_Link(t *testing.T) {
	fs := NewFileSystem(true)
	status := fs.Link("", "", nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.ENOSYS)
}

func TestFileSystem_Mkdir(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	status := fs.Mkdir("abcd/test", 0777, nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_Mknod(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	status := fs.Mknod("abcd/test1", 0777, 0, nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(1))
	os.RemoveAll("./mock")
}

func TestFileSystem_Rename(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	status := fs.Rename("abcd", "acg", nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_Rmdir(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd1", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	status := fs.Rmdir("abcd1", nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_Unlink(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd1", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	status := fs.Unlink("abcd1", nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(1))
	os.RemoveAll("./mock")
}

func TestFileSystem_GetXAttr(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd1", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	_, status := fs.GetXAttr("abcd1", "", nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_ListXAttr(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd1", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	_, status := fs.ListXAttr("", nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_RemoveXAttr(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd1", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	status := fs.RemoveXAttr("abcd1", "", nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(22))
	os.RemoveAll("./mock")
}

func TestFileSystem_SetXAttr(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd1", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	status := fs.SetXAttr("abcd1", "key", []byte("value"), 0, nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_Open(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd", 0777)
	file, _ := os.Create("./mock/abcd/file")
	defer file.Close()
	io.WriteString(file, "tesabcd")
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	_, status := fs.Open("abcd/file", 0, nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_Create(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd1", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	_, status := fs.Create("abcd1/file", 0, 0755, nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_OpenDir(t *testing.T) {
	fs := NewFileSystem(true)
	os.MkdirAll("./mock/abcd1", 0777)
	vfs.InitOldVFS(base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./mock",
		},
		SubPath: "./mock",
	}, nil, true)
	_, status := fs.OpenDir("abcd1", nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.Status(0))
	os.RemoveAll("./mock")
}

func TestFileSystem_Symlink(t *testing.T) {
	fs := NewFileSystem(true)
	status := fs.Symlink("", "", nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.ENOSYS)
}

func TestFileSystem_Readlink(t *testing.T) {
	fs := NewFileSystem(true)
	_, status := fs.Readlink("", nil)
	assert.NotNil(t, fs)
	assert.Equal(t, status, fuse.ENOSYS)
}
