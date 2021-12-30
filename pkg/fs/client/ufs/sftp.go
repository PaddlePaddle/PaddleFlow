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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/utils"
)

const (
	dirSuffix = "/"
)

type sshConn struct {
	sshClient *ssh.Client
	// a Client may be called concurrently from multiple Goroutines.
	// 如需要多实例，可考虑sync.Pool实现
	sftpClient *sftp.Client
	err        chan error
}

func (sc *sshConn) wait() {
	sc.err <- sc.sshClient.Conn.Wait()
}

// Closes the connection
func (sc *sshConn) close() error {
	sftpErr := sc.sftpClient.Close()
	sshErr := sc.sshClient.Close()
	if sftpErr != nil {
		return sftpErr
	}
	return sshErr
}

// Returns an error if closed
func (sc *sshConn) closed() error {
	select {
	case err := <-sc.err:
		return err
	default:
	}
	return nil
}

type sftpFileSystem struct {
	addr    string // host:port
	subpath string
	config  *ssh.ClientConfig
	sc      *sshConn
}

// Open a new connection to the SFTP server.
func (fs *sftpFileSystem) NewSSHConn() (sc *sshConn, err error) {
	sc = &sshConn{
		err: make(chan error, 1),
	}

	conn, err := ssh.Dial("tcp", fs.addr, fs.config)
	if err != nil {
		return nil, err
	}
	sc.sshClient = conn
	sc.sftpClient, err = sftp.NewClient(conn)
	if err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "couldn't initialise SFTP")
	}
	go sc.wait()
	return sc, nil
}

// Used for pretty printing.
func (fs *sftpFileSystem) String() string {
	return fmt.Sprintf("%s@%s", fs.config.User, fs.addr)
}

func (fs *sftpFileSystem) GetPath(relPath string) string {
	if relPath == "" {
		return fs.subpath
	}
	var absPath string
	if strings.HasSuffix(relPath, dirSuffix) {
		absPath = filepath.Join(fs.subpath, relPath) + dirSuffix
	} else {
		absPath = filepath.Join(fs.subpath, relPath)
	}
	if runtime.GOOS == "windows" {
		absPath = strings.Replace(absPath, "\\", "/", -1)
	}
	return absPath
}

func (fs *sftpFileSystem) statFromFileInfo(finfo os.FileInfo) *syscall.Stat_t {
	st, ok := finfo.Sys().(*sftp.FileStat)
	if !ok {
		return nil
	}

	modificationTime := time.Unix(int64(st.Mtime), 0)
	mTime := fuse.UtimeToTimespec(&modificationTime)

	accessTime := time.Unix(int64(st.Atime), 0)
	aTime := fuse.UtimeToTimespec(&accessTime)

	fst := fillStat(0, st.Mode, st.UID, st.GID, int64(st.Size), 4096, int64(st.Size)/512, aTime, mTime, mTime)

	return &fst
}

// Attributes.  This function is the main entry point, through
// which FUSE discovers which files and directories exist.
//
// If the filesystem wants to implement hard-links, it should
// return consistent non-zero FileInfo.Ino data.  Using
// hardlinks incurs a performance hit.
func (fs *sftpFileSystem) GetAttr(name string) (*base.FileInfo, error) {
	log.Debugf("the path is %v", fs.GetPath(name))
	info, err := fs.sc.sftpClient.Stat(fs.GetPath(name))

	if err != nil {
		return nil, err
	}
	owner, group := utils.GetOwnerGroup(info)

	return &base.FileInfo{
		Name:  name,
		Path:  fs.GetPath(name),
		Size:  info.Size(),
		Mtime: uint64(info.ModTime().Unix()),
		IsDir: info.IsDir(),
		Owner: owner,
		Group: group,
		Sys:   *fs.statFromFileInfo(info),
	}, nil
}

// // These should update the file's ctime too.
func (fs *sftpFileSystem) Chmod(name string, mode uint32) error {
	return fs.sc.sftpClient.Chmod(fs.GetPath(name), os.FileMode(mode))
}

func (fs *sftpFileSystem) Chown(name string, uid uint32, gid uint32) error {
	return fs.sc.sftpClient.Chown(fs.GetPath(name), int(uid), int(gid))
}

func (fs *sftpFileSystem) Utimens(name string, atime *time.Time, mtime *time.Time) error {
	return fs.sc.sftpClient.Chtimes(fs.GetPath(name), *atime, *mtime)
}

func (fs *sftpFileSystem) Truncate(name string, size uint64) error {
	return fs.sc.sftpClient.Truncate(fs.GetPath(name), int64(size))
}

func (fs *sftpFileSystem) Access(name string, mode, callerUid, callerGid uint32) error {
	return nil
}

// Tree structure
// TODO: test link
func (fs *sftpFileSystem) Link(oldName string, newName string) error {
	return fs.sc.sftpClient.Link(fs.GetPath(oldName), fs.GetPath(newName))
}

func (fs *sftpFileSystem) Mkdir(name string, mode uint32) error {
	return fs.sc.sftpClient.Mkdir(fs.GetPath(name))
}

func (fs *sftpFileSystem) Mknod(name string, mode uint32, dev uint32) error {
	return syscall.ENOSYS
}

func (fs *sftpFileSystem) Rename(oldName string, newName string) error {
	return fs.sc.sftpClient.Rename(fs.GetPath(oldName), fs.GetPath(newName))
}

func (fs *sftpFileSystem) Rmdir(name string) error {
	return fs.sc.sftpClient.Remove(fs.GetPath(name))
}

func (fs *sftpFileSystem) Unlink(name string) error {
	return fs.sc.sftpClient.Remove(fs.GetPath(name))
}

// // Extended attributes.
func (fs *sftpFileSystem) GetXAttr(name string, attribute string) (data []byte, err error) {
	return nil, syscall.ENOSYS
}

func (fs *sftpFileSystem) ListXAttr(name string) (attributes []string, err error) {
	return nil, syscall.ENOSYS
}

func (fs *sftpFileSystem) RemoveXAttr(name string, attr string) error {
	return syscall.ENOSYS
}

func (fs *sftpFileSystem) SetXAttr(name string, attr string, data []byte, flags int) error {
	return syscall.ENOSYS
}

// File handling.  If opening for writing, the file's mtime
// should be updated too.
func (fs *sftpFileSystem) Open(name string, flags uint32) (fd base.FileHandle, err error) {
	f, err := fs.sc.sftpClient.OpenFile(fs.GetPath(name), int(flags))
	if err != nil {
		return nil, err
	}
	return &sftpFileHandle{
		name: name,
		f:    f,
		fs:   fs,
	}, nil
}

func (fs *sftpFileSystem) Create(name string, flags uint32, mode uint32) (fd base.FileHandle, err error) {
	// os.O_RDWR|os.O_CREATE|os.O_TRUNC
	f, err := fs.sc.sftpClient.Create(fs.GetPath(name))

	if err != nil {
		return nil, err
	}

	return &sftpFileHandle{
		name: name,
		f:    f,
		fs:   fs,
	}, nil
}

// Directory handling
func (fs *sftpFileSystem) ReadDir(name string) (stream []base.DirEntry, err error) {
	files, err := fs.sc.sftpClient.ReadDir(fs.GetPath(name))

	allAttrs := make([]base.DirEntry, len(files))

	for i, fileInfo := range files {
		allAttrs[i] = base.DirEntry{
			Name: fileInfo.Name(),
			Mode: uint32(fileInfo.Mode()),
		}
	}
	return allAttrs, nil
}

// Symlinks.
func (fs *sftpFileSystem) Symlink(value string, linkName string) error {
	return fs.sc.sftpClient.Symlink(fs.GetPath(value), fs.GetPath(linkName))
}

func (fs *sftpFileSystem) Readlink(name string) (string, error) {
	return fs.sc.sftpClient.ReadLink(fs.GetPath(name))
}

func (fs *sftpFileSystem) StatFs(name string) *base.StatfsOut {
	vfs, _ := fs.sc.sftpClient.StatVFS(fs.GetPath(name))
	return &base.StatfsOut{
		Blocks:  vfs.Blocks,
		Bfree:   vfs.Bfree,
		Bavail:  vfs.Bavail,
		Files:   vfs.Files,
		Ffree:   vfs.Ffree,
		Bsize:   uint32(vfs.Bsize),
		NameLen: uint32(vfs.Namemax),
		Frsize:  uint32(vfs.Frsize),
	}

}

type sftpFileHandle struct {
	name string
	f    *sftp.File
	fs   *sftpFileSystem
}

var _ base.FileHandle = &sftpFileHandle{}

func (fh *sftpFileHandle) String() string {
	return fmt.Sprintf("sftpFileHandle(%s)", fh.name)
}

func (fh *sftpFileHandle) SetInode(*nodefs.Inode) {
}
func (fh *sftpFileHandle) InnerFile() nodefs.File {
	return nil
}

func (fh *sftpFileHandle) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	n, err := fh.f.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return nil, fuse.ToStatus(err)
	}
	return fuse.ReadResultData(buf[0:n]), fuse.OK
}

func (fh *sftpFileHandle) Write(data []byte, off int64) (uint32, fuse.Status) {
	n, err := fh.f.WriteAt(data, off)
	return uint32(n), fuse.ToStatus(err)
}

func (fh *sftpFileHandle) Release() {
	fh.f.Close()
}

func (fh *sftpFileHandle) Flush() fuse.Status {
	return fuse.ToStatus(fh.f.Sync())
}

func (fh *sftpFileHandle) Fsync(flags int) (code fuse.Status) {
	return fuse.ToStatus(fh.f.Sync())
}

// not support
func (fh *sftpFileHandle) GetLk(owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fh *sftpFileHandle) SetLk(owner uint64, lk *fuse.FileLock, flags uint32) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fh *sftpFileHandle) SetLkw(owner uint64, lk *fuse.FileLock, flags uint32) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fh *sftpFileHandle) setLock(owner uint64, lk *fuse.FileLock, flags uint32, blocking bool) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fh *sftpFileHandle) Truncate(size uint64) fuse.Status {
	return fuse.ToStatus(fh.fs.Truncate(fh.name, size))
}

func (fh *sftpFileHandle) Chmod(mode uint32) fuse.Status {
	return fuse.ToStatus(fh.fs.Chmod(fh.name, mode))
}

func (fh *sftpFileHandle) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.ToStatus(fh.fs.Chown(fh.name, uid, gid))
}

func (fh *sftpFileHandle) GetAttr(a *fuse.Attr) fuse.Status {
	finfo, err := fh.fs.GetAttr(fh.name)
	if err != nil {
		return fuse.ToStatus(err)
	}

	stat_t := finfo.Sys.(syscall.Stat_t)
	a.FromStat(&stat_t)
	return fuse.OK
}
func (fh *sftpFileHandle) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	return fuse.ToStatus(fh.fs.Utimens(fh.name, atime, mtime))
}

func (fh *sftpFileHandle) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	return fuse.ENOSYS
}

func NewSftpFileSystem(properties map[string]interface{}) (UnderFileStorage, error) {
	addr := properties[base.Address].(string)
	subpath := properties[base.SubPath].(string)
	user := properties[base.UserKey].(string)
	password := properties[base.Password].(string)

	if runtime.GOOS == "windows" {
		subpath = strings.Replace(subpath, "\\", "/", -1)
	}

	if !strings.HasSuffix(subpath, dirSuffix) {
		subpath = subpath + dirSuffix
	}

	config := &ssh.ClientConfig{
		User:            user,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Second * 3,
	}

	if password != "" {
		password, err := common.AesDecrypt(password, common.AESEncryptKey)
		if err != nil {
			return nil, err
		}
		config.Auth = append(config.Auth, ssh.Password(password))
	}

	fs := &sftpFileSystem{
		addr:    addr,
		subpath: subpath,
		config:  config,
	}

	sc, err := fs.NewSSHConn()
	if err != nil {
		return nil, err
	}
	fs.sc = sc
	if err := sc.sftpClient.MkdirAll(subpath); err != nil {
		return nil, fmt.Errorf("Creating directory %s failed: %q ", subpath, err)
	}
	runtime.SetFinalizer(fs, func(fs *sftpFileSystem) {
		fs.sc.close()
	})

	return fs, nil
}

func init() {
	RegisterUFS(base.SFTPType, NewSftpFileSystem)
}
