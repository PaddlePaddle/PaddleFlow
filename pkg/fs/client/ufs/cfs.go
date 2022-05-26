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

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/mount"
)

const (
	mountPath     = ".tmp_cfs"
	cfsMountParam = "minorversion=1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport"
)

type cfsFileSystem struct {
	addr      string // host:
	subpath   string
	localPath string
}

func (fs *cfsFileSystem) String() string {
	return common.CFSType
}

func (fs *cfsFileSystem) GetPath(relPath string) string {
	return filepath.Join(fs.localPath, relPath)
}

func (fs *cfsFileSystem) Chmod(name string, mode uint32) error {
	return os.Chmod(fs.GetPath(name), os.FileMode(mode))
}

func (fs *cfsFileSystem) Chown(name string, uid uint32, gid uint32) error {
	return os.Chown(fs.GetPath(name), int(uid), int(gid))
}

func (fs *cfsFileSystem) Utimens(name string, atime *time.Time, mtime *time.Time) error {
	return os.Chtimes(fs.GetPath(name), *atime, *mtime)
}

func (fs *cfsFileSystem) Truncate(name string, size uint64) error {
	return os.Truncate(fs.GetPath(name), int64(size))
}

func (fs *cfsFileSystem) Access(name string, mode, callerUid, callerGid uint32) error {
	return syscall.ENOSYS
}

func (fs *cfsFileSystem) Link(oldName string, newName string) error {
	return os.Link(fs.GetPath(oldName), fs.GetPath(newName))
}

func (fs *cfsFileSystem) Mkdir(name string, mode uint32) error {
	return os.Mkdir(fs.GetPath(name), os.FileMode(mode))
}

func (fs *cfsFileSystem) Mknod(name string, mode uint32, dev uint32) error {
	return syscall.ENOSYS
}

func (fs *cfsFileSystem) Rename(oldName string, newName string) error {
	return os.Rename(fs.GetPath(oldName), fs.GetPath(newName))
}

func (fs *cfsFileSystem) Rmdir(name string) error {
	return syscall.Rmdir(fs.GetPath(name))
}

func (fs *cfsFileSystem) Unlink(name string) error {
	return syscall.Unlink(fs.GetPath(name))
}

func (fs *cfsFileSystem) GetXAttr(name string, attribute string) (data []byte, err error) {
	return nil, syscall.ENOSYS
}

func (fs *cfsFileSystem) ListXAttr(name string) (attributes []string, err error) {
	return nil, syscall.ENOSYS
}

func (fs *cfsFileSystem) RemoveXAttr(name string, attr string) error {
	return syscall.ENOSYS
}

func (fs *cfsFileSystem) SetXAttr(name string, attr string, data []byte, flags int) error {
	return syscall.ENOSYS
}

func (fs *cfsFileSystem) Open(name string, flags uint32) (fd base.FileHandle, err error) {
	flags = flags &^ syscall.O_APPEND
	f, err := os.OpenFile(fs.GetPath(name), int(flags), 0)
	if err != nil {
		return nil, err
	}
	return nodefs.NewLoopbackFile(f), nil
}

func (fs *cfsFileSystem) Create(name string, flags uint32, mode uint32) (fd base.FileHandle, err error) {
	flags = flags &^ syscall.O_APPEND
	f, err := os.OpenFile(fs.GetPath(name), int(flags)|os.O_CREATE, os.FileMode(mode))
	return nodefs.NewLoopbackFile(f), err
}

// Directory handling
func (fs *cfsFileSystem) ReadDir(name string) (stream []base.DirEntry, err error) {
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
func (fs *cfsFileSystem) Symlink(value string, linkName string) error {
	return os.Symlink(value, fs.GetPath(linkName))
}

func (fs *cfsFileSystem) Readlink(name string) (string, error) {
	f, err := os.Readlink(fs.GetPath(name))
	return f, err
}

func (fs *cfsFileSystem) StatFs(name string) *base.StatfsOut {
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

func (fs *cfsFileSystem) Get(name string, flags uint32, off, limit int64) (io.ReadCloser, error) {
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

func (fs *cfsFileSystem) Put(name string, reader io.Reader) error {
	return nil
}

func NewCfsFileSystem(properties map[string]interface{}) (UnderFileStorage, error) {
	addr := properties[common.Address].(string)
	subpath := properties[common.SubPath].(string)
	os.MkdirAll(mountPath, 0755)
	localPath, err := ioutil.TempDir(mountPath, "*")
	if err != nil {
		log.Errorf("create temp dir all path[%s] failed: %v", localPath, err)
		return nil, err
	}

	// todo use cfs client
	sourcePath := filepath.Join(addr, subpath) + "/"
	args := []string{"-t", "nfs4", "-o", cfsMountParam}
	output, err := mount.ExecMount(sourcePath, localPath, args)
	if err != nil {
		log.Errorf("exec cfs mount cmd failed: %v, output[%s]", err, string(output))
		os.Remove(localPath)
		return nil, errors.New(string(output))
	}

	fs := &cfsFileSystem{
		addr:      addr,
		subpath:   subpath,
		localPath: localPath,
	}

	runtime.SetFinalizer(fs, func(fs *cfsFileSystem) {
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
	RegisterUFS(common.CFSType, NewCfsFileSystem)
}
