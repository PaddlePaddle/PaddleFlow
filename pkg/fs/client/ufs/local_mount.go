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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

const (
	localMountPath = ".tmp_local_mount"
	cfsMountParam  = "minorversion=1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport"
)

type localMount struct {
	addr      string
	subpath   string
	localPath string
	mountType string
}

func (fs *localMount) String() string {
	return fs.mountType
}

func (fs *localMount) GetPath(relPath string) string {
	return filepath.Join(fs.localPath, relPath)
}

func (fs *localMount) Chmod(name string, mode uint32) error {
	return os.Chmod(fs.GetPath(name), os.FileMode(mode))
}

func (fs *localMount) Chown(name string, uid uint32, gid uint32) error {
	return os.Chown(fs.GetPath(name), int(uid), int(gid))
}

func (fs *localMount) Utimens(name string, atime *time.Time, mtime *time.Time) error {
	return os.Chtimes(fs.GetPath(name), *atime, *mtime)
}

func (fs *localMount) Truncate(name string, size uint64) error {
	return os.Truncate(fs.GetPath(name), int64(size))
}

func (fs *localMount) Access(name string, mode, callerUid, callerGid uint32) error {
	return syscall.ENOSYS
}

func (fs *localMount) Link(oldName string, newName string) error {
	return os.Link(fs.GetPath(oldName), fs.GetPath(newName))
}

func (fs *localMount) Mkdir(name string, mode uint32) error {
	return os.Mkdir(fs.GetPath(name), os.FileMode(mode))
}

func (fs *localMount) Mknod(name string, mode uint32, dev uint32) error {
	return syscall.ENOSYS
}

func (fs *localMount) Rename(oldName string, newName string) error {
	return os.Rename(fs.GetPath(oldName), fs.GetPath(newName))
}

func (fs *localMount) Rmdir(name string) error {
	return syscall.Rmdir(fs.GetPath(name))
}

func (fs *localMount) Unlink(name string) error {
	return syscall.Unlink(fs.GetPath(name))
}

func (fs *localMount) GetXAttr(name string, attribute string) (data []byte, err error) {
	return nil, syscall.ENOSYS
}

func (fs *localMount) ListXAttr(name string) (attributes []string, err error) {
	return nil, syscall.ENOSYS
}

func (fs *localMount) RemoveXAttr(name string, attr string) error {
	return syscall.ENOSYS
}

func (fs *localMount) SetXAttr(name string, attr string, data []byte, flags int) error {
	return syscall.ENOSYS
}

func (fs *localMount) Open(name string, flags uint32) (fd FileHandle, err error) {
	flags = flags &^ syscall.O_APPEND
	f, err := os.OpenFile(fs.GetPath(name), int(flags), 0)
	if err != nil {
		return nil, err
	}
	return nodefs.NewLoopbackFile(f), nil
}

func (fs *localMount) Create(name string, flags uint32, mode uint32) (fd FileHandle, err error) {
	flags = flags &^ syscall.O_APPEND
	f, err := os.OpenFile(fs.GetPath(name), int(flags)|os.O_CREATE, os.FileMode(mode))
	return nodefs.NewLoopbackFile(f), err
}

// Directory handling
func (fs *localMount) ReadDir(name string) (stream []DirEntry, err error) {
	entries, err := os.ReadDir(fs.GetPath(name))
	if err != nil {
		return nil, err
	}
	output := make([]DirEntry, 0, len(entries))
	for _, entry := range entries {
		fInfo, err := entry.Info()
		if err != nil {
			log.Debugf("ReadDir name[%s] entry[%v] Info() failed: %v", name, entry, err)
			continue
		}
		d := DirEntry{
			Name: entry.Name(),
		}
		attr := sysToAttr(fInfo)
		d.Attr = &attr
		output = append(output, d)
	}
	return output, nil
}

// Symlinks.
func (fs *localMount) Symlink(value string, linkName string) error {
	return os.Symlink(value, fs.GetPath(linkName))
}

func (fs *localMount) Readlink(name string) (string, error) {
	f, err := os.Readlink(fs.GetPath(name))
	return f, err
}

func (fs *localMount) StatFs(name string) *base.StatfsOut {
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

func (fs *localMount) Get(name string, flags uint32, off, limit int64) (io.ReadCloser, error) {
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

func (fs *localMount) Put(name string, reader io.Reader) error {
	return nil
}

func NewLocalMountFileSystem(properties map[string]interface{}) (UnderFileStorage, error) {
	mountType := properties[common.Type].(string)
	if mountType == "" {
		return nil, errors.New("mount type empty")
	}
	var args []string
	var sourcePath string
	err := os.MkdirAll(localMountPath, 0755)
	if err != nil {
		return nil, err
	}
	localPath, err := ioutil.TempDir(localMountPath, "*")
	if err != nil {
		log.Errorf("create temp dir all path[%s] failed: %v", localPath, err)
		return nil, err
	}
	addr := properties[common.Address].(string)
	subpath := properties[common.SubPath].(string)

	switch mountType {
	case common.GlusterFSType:
		sourcePath = addr + ":" + subpath
		args = []string{"-t", "glusterfs"}
	case common.CFSType:
		sourcePath = filepath.Join(addr, subpath) + "/"
		args = []string{"-t", "nfs4", "-o", cfsMountParam}
	default:
		return nil, fmt.Errorf("type[%s] is not exist", mountType)
	}

	output, err := utils.ExecMount(sourcePath, localPath, args)
	if err != nil {
		log.Errorf("exec %s mount cmd failed: %v, output[%s]", mountType, err, string(output))
		os.Remove(localPath)
		return nil, errors.New(string(output))
	}

	fs := &localMount{
		addr:      addr,
		subpath:   subpath,
		localPath: localPath,
		mountType: mountType,
	}

	runtime.SetFinalizer(fs, func(fs *localMount) {
		err = utils.ForceUnmount(localPath)
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
	RegisterUFS(common.GlusterFSType, NewLocalMountFileSystem)
	RegisterUFS(common.CFSType, NewLocalFileSystem)
}
