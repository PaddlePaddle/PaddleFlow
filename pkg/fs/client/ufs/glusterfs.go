/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/common"
	"paddleflow/pkg/fs/utils/mount"
)

const (
	glusterfsMountPath = ".tmp_gfs"
)

type gfsFileSystem struct {
	addr      string // host:
	subpath   string
	localPath string
}

func (fs *gfsFileSystem) String() string {
	return common.GlusterfsType
}

func (fs *gfsFileSystem) GetPath(relPath string) string {
	return filepath.Join(fs.localPath, relPath)
}

func (fs *gfsFileSystem) Chmod(name string, mode uint32) error {
	return os.Chmod(fs.GetPath(name), os.FileMode(mode))
}

func (fs *gfsFileSystem) Chown(name string, uid uint32, gid uint32) error {
	return os.Chown(fs.GetPath(name), int(uid), int(gid))
}

func (fs *gfsFileSystem) Utimens(name string, atime *time.Time, mtime *time.Time) error {
	return os.Chtimes(fs.GetPath(name), *atime, *mtime)
}

func (fs *gfsFileSystem) Truncate(name string, size uint64) error {
	return os.Truncate(fs.GetPath(name), int64(size))
}

func (fs *gfsFileSystem) Access(name string, mode, callerUid, callerGid uint32) error {
	return syscall.ENOSYS
}

func (fs *gfsFileSystem) Link(oldName string, newName string) error {
	return os.Link(fs.GetPath(oldName), fs.GetPath(newName))
}

func (fs *gfsFileSystem) Mkdir(name string, mode uint32) error {
	return os.Mkdir(fs.GetPath(name), os.FileMode(mode))
}

func (fs *gfsFileSystem) Mknod(name string, mode uint32, dev uint32) error {
	return syscall.ENOSYS
}

func (fs *gfsFileSystem) Rename(oldName string, newName string) error {
	return os.Rename(fs.GetPath(oldName), fs.GetPath(newName))
}

func (fs *gfsFileSystem) Rmdir(name string) error {
	return syscall.Rmdir(fs.GetPath(name))
}

func (fs *gfsFileSystem) Unlink(name string) error {
	return syscall.Unlink(fs.GetPath(name))
}

func (fs *gfsFileSystem) GetXAttr(name string, attribute string) (data []byte, err error) {
	return nil, syscall.ENOSYS
}

func (fs *gfsFileSystem) ListXAttr(name string) (attributes []string, err error) {
	return nil, syscall.ENOSYS
}

func (fs *gfsFileSystem) RemoveXAttr(name string, attr string) error {
	return syscall.ENOSYS
}

func (fs *gfsFileSystem) SetXAttr(name string, attr string, data []byte, flags int) error {
	return syscall.ENOSYS
}

func (fs *gfsFileSystem) Open(name string, flags uint32) (fd base.FileHandle, err error) {
	flags = flags &^ syscall.O_APPEND
	f, err := os.OpenFile(fs.GetPath(name), int(flags), 0)
	if err != nil {
		return nil, err
	}
	return nodefs.NewLoopbackFile(f), nil
}

func (fs *gfsFileSystem) Create(name string, flags uint32, mode uint32) (fd base.FileHandle, err error) {
	flags = flags &^ syscall.O_APPEND
	f, err := os.OpenFile(fs.GetPath(name), int(flags)|os.O_CREATE, os.FileMode(mode))
	return nodefs.NewLoopbackFile(f), err
}

// Directory handling
func (fs *gfsFileSystem) ReadDir(name string) (stream []base.DirEntry, err error) {
	entries, err := os.ReadDir(fs.GetPath(name))
	if err != nil {
		return nil, err
	}
	output := make([]base.DirEntry, 0, len(entries))
	for _, entry := range entries {
		fInfo, err := entry.Info()
		if err != nil {
			log.Debugf("ReadDir name[%s] entry[%v] Info() failed: %v", name, entry, err)
			continue
		}
		d := base.DirEntry{
			Name: entry.Name(),
		}
		if s := fuse.ToStatT(fInfo); s != nil {
			d.Mode = uint32(s.Mode)
			d.Ino = s.Ino
		} else {
			log.Debugf("ReadDir name[%s], fInfo[%v] ToStatT is nil", name, fInfo)
		}
		output = append(output, d)
	}
	return output, nil
}

// Symlinks.
func (fs *gfsFileSystem) Symlink(value string, linkName string) error {
	return os.Symlink(value, fs.GetPath(linkName))
}

func (fs *gfsFileSystem) Readlink(name string) (string, error) {
	f, err := os.Readlink(fs.GetPath(name))
	return f, err
}

func (fs *gfsFileSystem) StatFs(name string) *base.StatfsOut {
	s := syscall.Statfs_t{}
	err := syscall.Statfs(fs.GetPath(name), &s)
	if err != nil {
		log.Debugf("StatFs path[%s] failed: %v", name, err)
		return nil
	}
	out := &base.StatfsOut{}
	out.FromStatfsT(&s)
	return out
}

func (fs *gfsFileSystem) Get(name string, flags uint32, off, limit int64) (io.ReadCloser, error) {
	flags = flags &^ syscall.O_APPEND
	reader, err := os.OpenFile(fs.GetPath(name), int(flags), 0)
	if err != nil {
		return nil, err
	}
	if off > 0 {
		if _, err := reader.Seek(off, io.SeekStart); err != nil {
			reader.Close()
			return nil, err
		}
	}
	if limit > 0 {
		return withCloser{io.LimitReader(reader, limit), reader}, nil
	}
	return reader, err
}

func (fs *gfsFileSystem) Put(name string, reader io.Reader) error {
	return nil
}

func NewGFSFileSystem(properties map[string]interface{}) (UnderFileStorage, error) {
	addr := properties[common.Address].(string)
	subpath := properties[common.SubPath].(string)
	os.MkdirAll(glusterfsMountPath, 0755)
	localPath, err := ioutil.TempDir(glusterfsMountPath, "*")
	if err != nil {
		log.Errorf("create temp dir all path[%s] failed: %v", localPath, err)
		return nil, err
	}

	// todo use cfs client
	sourcePath := addr + ":" + subpath
	args := []string{"-t", "glusterfs"}
	output, err := mount.ExecMount(sourcePath, localPath, args)
	if err != nil {
		log.Errorf("exec glusterfs mount cmd failed: %v, output[%s]", err, string(output))
		os.Remove(localPath)
		return nil, errors.New(string(output))
	}

	fs := &gfsFileSystem{
		addr:      addr,
		subpath:   subpath,
		localPath: localPath,
	}

	runtime.SetFinalizer(fs, func(fs *gfsFileSystem) {
		err = mount.ForceUnmount(localPath)
		if err != nil {
			log.Debugf("force unmount mountPoint[%s] failed: %v", localPath, err)
		}
		err = os.Remove(localPath)
		if err != nil {
			log.Debugf("remove all path[%s] failed: %v", localPath, err)
			return
		}
	})
	return fs, nil
}

func init() {
	RegisterUFS(common.GlusterfsType, NewGFSFileSystem)
}
