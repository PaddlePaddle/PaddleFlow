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
	"math/rand"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

const (
	DefaultBlockSize   = int64(64 * 1024 * 1024)
	DefaultReplication = 3
	abcException       = "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 32<<10)
		return &buf
	},
}

type hdfsFileSystem struct {
	client      *hdfs.Client
	subpath     string
	blockSize   int64
	replication int
	sync.Mutex
}

// Used for pretty printing.
func (fs *hdfsFileSystem) String() string {
	return common.HDFSType
}

func (fs *hdfsFileSystem) GetPath(relPath string) string {
	return filepath.Join(fs.subpath, relPath)
}

// Returns true if err==nil or err is expected (benign) error which should be propagated directoy to the caller
func IsSuccessOrBenignError(err error) bool {
	if err == nil || err == io.EOF || err == syscall.EEXIST {
		return true
	}
	if pathError, ok := err.(*os.PathError); ok && (pathError.Err == os.ErrNotExist || pathError.Err == os.ErrPermission) {
		return true
	} else {
		return false
	}
}

// Converts os.FileInfo + underlying proto-buf data into Attrs structure
func (fs *hdfsFileSystem) statFromFileInfo(fileInfo os.FileInfo) *syscall.Stat_t {
	log.Tracef("hdfs statFromFileInfo: fileInfo[%+v]", fileInfo)
	protoBufData, ok := fileInfo.Sys().(*hdfs.FileStatus)
	if !ok {
		return nil
	}
	modificationTime := time.Unix(int64(protoBufData.GetModificationTime())/1000, 0)
	mTime := fuse.UtimeToTimespec(&modificationTime)

	accessTime := time.Unix(int64(protoBufData.GetAccessTime())/1000, 0)
	aTime := fuse.UtimeToTimespec(&accessTime)

	size := fileInfo.Size()
	nlink := 0

	blocks := int64(size / 512)

	st := fillStat(uint64(nlink), 0, 0, 0, size, 512, blocks, aTime, mTime, mTime)
	st.Ino = *protoBufData.FileId
	return &st
}

// Attributes.  This function is the main entry point, through
// which FUSE discovers which files and directories exist.
//
// If the filesystem wants to implement hard-links, it should
// return consistent non-zero FileInfo.Ino data.  Using
// hardlinks incurs a performance hit.
func (fs *hdfsFileSystem) GetAttr(name string) (*base.FileInfo, error) {
	log.Tracef("hdfs getattr: name[%s]", name)
	fs.Lock()
	defer fs.Unlock()
	info, err := fs.client.Stat(fs.GetPath(name))
	if err != nil {
		if IsSuccessOrBenignError(err) {
			// benign error (e.g. path not found)
			return &base.FileInfo{}, err
		}
		return nil, err
	}

	hinfo := info.(*hdfs.FileInfo)
	f := &base.FileInfo{
		Name:  name,
		Path:  fs.GetPath(name),
		Size:  hinfo.Size(),
		IsDir: hinfo.IsDir(),
		Mtime: uint64(hinfo.ModTime().Unix()),
		Owner: hinfo.Owner(),
		Group: hinfo.OwnerGroup(),
		Mode:  hinfo.Mode(),
		Sys:   *fs.statFromFileInfo(hinfo),
	}

	// stickybit from HDFS is different than golang
	if f.Mode&01000 != 0 {
		f.Mode &= ^os.FileMode(01000)
		f.Mode |= os.ModeSticky
	}

	if hinfo.IsDir() {
		f.Size = 4096
		if !strings.HasSuffix(f.Path, "/") {
			f.Path += "/"
		}
	}
	return f, nil
}

// These should update the file's ctime too.
func (fs *hdfsFileSystem) Chmod(name string, mode uint32) error {
	// meta support chmod not need hdfs chmod
	return nil
	log.Tracef("hdfs chmod: name[%s], mode[%d]", name, mode)
	fs.Lock()
	defer fs.Unlock()
	return fs.client.Chmod(fs.GetPath(name), os.FileMode(mode))
}

func (fs *hdfsFileSystem) Chown(name string, uid uint32, gid uint32) error {
	// meta support chown not need hdfs chmod
	return nil
	log.Tracef("hdfs chown: name[%s]", name)
	fs.Lock()
	defer fs.Unlock()
	u, err := user.LookupId(fmt.Sprint(uid))
	var owner, groupname string
	if err != nil {
		log.Errorf("Chown: username for uid %v %s", uid, "not found, use uid/gid instead")
		return err
	}
	owner = u.Username
	g, err := user.LookupGroup(fmt.Sprint(gid))
	if err != nil {
		log.Errorf("Chown: username for gid %v %s", uid, "not found, use uid/gid instead")
		groupname = owner
	} else {
		groupname = g.Name
	}

	log.Debugf("chown owner %s group %s", owner, groupname)

	return fs.client.Chown(fs.GetPath(name), owner, groupname)
}

func (fs *hdfsFileSystem) Utimens(name string, atime, mtime *time.Time) error {
	log.Tracef("hdfs utimens: name[%s], atime[%v] mtime[%v]", name, atime, mtime)
	fs.Lock()
	defer fs.Unlock()
	return fs.client.Chtimes(fs.GetPath(name), *atime, *mtime)
}

func (fs *hdfsFileSystem) Truncate(name string, size uint64) error {
	log.Tracef("hdfs truncate: name[%s], size[%d]", name, size)
	if size == 0 {
		if err := fs.Unlink(name); err != nil {
			log.Errorf("hdfsFileSystem name[%s] Truncate Unlink error: %v", name, err)
			return err
		}
		flags := syscall.O_CREAT | syscall.O_EXCL
		fh, err := fs.Create(name, uint32(flags), 0644)
		if err != nil {
			return err
		}
		fh.Release()
		return nil
	}
	return nil

}

func (fs *hdfsFileSystem) Access(name string, mode, callerUid, callerGid uint32) error {
	return nil
}

// Tree structure
func (fs *hdfsFileSystem) Link(oldName string, newName string) error {
	return syscall.ENOSYS
}

func (fs *hdfsFileSystem) Mkdir(name string, mode uint32) error {
	log.Tracef("hdfs mkdir: name[%s], mode[%d]", name, mode)
	fs.Lock()
	defer fs.Unlock()
	err := fs.client.Mkdir(fs.GetPath(name), os.FileMode(mode))
	if err != nil {
		if strings.HasSuffix(err.Error(), "file already exists") {
			err = syscall.EEXIST
		}
	}
	return err
}

func (fs *hdfsFileSystem) Mknod(name string, mode uint32, dev uint32) error {
	return syscall.ENOSYS
}

func (fs *hdfsFileSystem) Rename(oldName, newName string) error {
	log.Tracef("hdfs rename: oldName[%s], newName[%s]", oldName, newName)
	fs.Lock()
	defer fs.Unlock()
	oldPath := fs.GetPath(oldName)
	newPath := fs.GetPath(newName)
	return fs.client.Rename(oldPath, newPath)
}

func (fs *hdfsFileSystem) Rmdir(name string) error {
	log.Tracef("hdfs rmdir: name[%s]", name)
	fs.Lock()
	defer fs.Unlock()
	return fs.client.RemoveAll(fs.GetPath(name))
}

func (fs *hdfsFileSystem) Unlink(name string) error {
	fs.Lock()
	defer fs.Unlock()
	return fs.client.Remove(fs.GetPath(name))
}

// // Extended attributes.
func (fs *hdfsFileSystem) GetXAttr(name string, attribute string) (data []byte, err error) {
	return nil, syscall.ENOSYS
}

func (fs *hdfsFileSystem) ListXAttr(name string) (attributes []string, err error) {
	return nil, syscall.ENOSYS
}

func (fs *hdfsFileSystem) RemoveXAttr(name string, attr string) error {
	return syscall.ENOSYS
}

func (fs *hdfsFileSystem) SetXAttr(name string, attr string, data []byte, flags int) error {
	return syscall.ENOSYS
}

func (fs *hdfsFileSystem) GetOpenFlags(name string, flags uint32) int {

	if flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		return syscall.O_RDONLY
	}

	if flags&syscall.O_TRUNC != 0 {
		return syscall.O_WRONLY
	}

	info, _ := fs.GetAttr(name)
	if info != nil {
		if info.Size == 0 {
			// If the file has zero length, we shouldn't feel bad about blowing it
			// away.
			return syscall.O_WRONLY | syscall.O_APPEND
		}
		if flags&syscall.O_ACCMODE == syscall.O_RDWR {
			// HACK: translate O_RDWR requests into O_RDONLY if the file already
			// exists and has non-zero length.
			return syscall.O_RDONLY
		}
		// translate O_WRONLY requests into append if the file already exists
		return syscall.O_WRONLY | syscall.O_APPEND
	}

	if flags&syscall.O_CREAT != 0 {
		return syscall.O_WRONLY
	}

	return -1
}

func (fs *hdfsFileSystem) Get(name string, flags uint32, off, limit int64) (io.ReadCloser, error) {
	reader, err := fs.client.Open(fs.GetPath(name))
	if err != nil {
		log.Errorf("hdfs client open err: %v", err)
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

	return reader, nil
}

func (fs *hdfsFileSystem) Put(name string, reader io.Reader) error {
	return nil
}

// File handling.  If opening for writing, the file's mtime
// should be updated too.
func (fs *hdfsFileSystem) Open(name string, flags uint32, size uint64) (FileHandle, error) {
	log.Tracef("hdfs open: name[%s], flags[%d]", name, flags)
	flag := fs.GetOpenFlags(name, flags)

	if flag < 0 {
		log.Debugf("hdfs open flag<0")
		return nil, syscall.ENOSYS
	}

	fs.Lock()
	defer fs.Unlock()

	// read only
	if flag&syscall.O_ACCMODE == syscall.O_RDONLY {
		reader, err := fs.client.Open(fs.GetPath(name))
		if err != nil {
			log.Errorf("hdfs client open err: %v", err)
			return nil, err
		}

		return &hdfsFileHandle{
			name:   name,
			reader: reader,
			fs:     fs,
			writer: nil,
		}, nil
	}

	// append only

	if flag&syscall.O_APPEND != 0 {
		// hdfs nameNode maybe not release fh, has error: org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException
		writer, err := fs.client.Append(fs.GetPath(name))
		if fs.shouldRetry(err) {
			for i := 0; i < 2; i++ {
				time.Sleep(100 * time.Millisecond * time.Duration(i*i))
				writer, err = fs.client.Append(fs.GetPath(name))
				if err == nil {
					break
				}
			}
			if fs.shouldRetry(err) {
				err = fs.ReleaseAlreadyBeingCreateFile(fs.GetPath(name))
				if err != nil {
					log.Errorf("ReleaseAlreadyBeingCreateFile: err[%v]", err)
					return nil, err
				}
				writer, err = fs.client.Append(fs.GetPath(name))
			}
			if err != nil {
				log.Errorf("fs.client.Append: err[%v]", err)
				return nil, err
			}
		}
		if err != nil {
			log.Errorf("Append err[%v]", err)
			return nil, err
		}
		return &hdfsFileHandle{
			name:   name,
			reader: nil,
			fs:     fs,
			writer: writer,
		}, nil
	}

	log.Errorf("flag[%d] not correct", flag)
	return nil, syscall.ENOTSUP
}

func (fs *hdfsFileSystem) Create(name string, flags, mode uint32) (fd FileHandle, err error) {
	fs.Lock()
	defer fs.Unlock()
	log.Tracef("hdfs create: name[%s], flags[%d], mode[%d]", name, flags, mode)
	writer, err := fs.client.CreateFile(fs.GetPath(name),
		fs.replication, fs.blockSize, os.FileMode(mode))
	if err != nil {
		log.Errorf("hdfsFileSystem name[%s] create error: %v", name, err)
		return nil, err
	}
	return &hdfsFileHandle{
		name:   name,
		writer: writer,
		fs:     fs,
		reader: nil,
	}, nil
}

// Directory handling
func (fs *hdfsFileSystem) ReadDir(name string) (stream []DirEntry, err error) {
	fs.Lock()
	defer fs.Unlock()
	log.Tracef("hdfs readdir: name[%s]", name)
	files, err := fs.client.ReadDir(fs.GetPath(name))
	if err != nil {
		log.Errorf("hdfsFileSystem name[%s] ReadDir error: %v", name, err)
		return nil, err
	}

	allAttrs := make([]DirEntry, len(files))

	for i, fileInfo := range files {
		attr := fs.sysHdfsToAttr(fileInfo)
		if fileInfo.IsDir() {
			attr.Size = 4096
		}
		allAttrs[i] = DirEntry{
			Name: fileInfo.Name(),
			Attr: &attr,
		}
	}
	return allAttrs, nil
}

// Symlinks.
func (fs *hdfsFileSystem) Symlink(value string, linkName string) error {
	return syscall.ENOSYS
}

func (fs *hdfsFileSystem) Readlink(name string) (string, error) {
	return "", syscall.ENOSYS
}

func (fs *hdfsFileSystem) StatFs(name string) *base.StatfsOut {
	fs.Lock()
	defer fs.Unlock()
	fsInfo, err := fs.client.StatFs()
	if err != nil {
		return &base.StatfsOut{}
	}

	// reference:
	// https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/fuse-dfs/fuse_impls_statfs.c
	return &base.StatfsOut{
		Blocks:  fsInfo.Capacity / uint64(fs.blockSize),
		Bfree:   fsInfo.Remaining / uint64(fs.blockSize),
		Bavail:  fsInfo.Remaining / uint64(fs.blockSize),
		Files:   1000,
		Ffree:   500,
		Bsize:   uint32(fs.blockSize),
		NameLen: 1023,
		Frsize:  uint32(fs.blockSize),
	}
}

func (fs *hdfsFileSystem) shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "AlreadyBeingCreatedException")
}

type hdfsFileHandle struct {
	name   string
	fs     *hdfsFileSystem
	writer *hdfs.FileWriter
	reader *hdfs.FileReader
}

var _ FileHandle = &hdfsFileHandle{}

func (fh *hdfsFileHandle) Read(buf []byte, off uint64) (int, error) {
	log.Tracef("hdfs read: fh.name[%s], offset[%d]", fh.name, off)
	if fh.reader == nil {
		err := fmt.Errorf("hdfs read: file[%s] bad file descriptor reader==nil", fh.name)
		log.Errorf(err.Error())
		return 0, err
	}
	n, err := fh.reader.ReadAt(buf, int64(off))
	if err != nil && err != io.EOF {
		log.Errorf("hdfsRead: the err is %+v", err)
		if strings.Contains(err.Error(), "invalid checksum") ||
			strings.Contains(err.Error(), "read/write on closed pipe") {
			// todo: 考虑并发，fh.reader需要加速判断
			if fh.reader != nil {
				fh.reader.Close()
				fh.reader = nil
			}
		}
		return 0, err
	}
	return n, nil
}

func (fh *hdfsFileHandle) Write(data []byte, off uint64) (uint32, error) {
	log.Tracef("hdfs write: fh.name[%s], dataLength[%d], offset[%d], fh[%+v]", fh.name, len(data), off, fh)
	var err error
	if fh.writer == nil {
		err = fmt.Errorf("hdfs write: file[%s] bad file descriptor writer==nil", fh.name)
		log.Errorf(err.Error())
		return 0, err
	}

	n, err := fh.writer.Write(data)
	if err != nil {
		fh.writer.Close()
		fh.writer = nil
		log.Errorf("hdfs write: fh.name[%s] fh.writer.Write err:%v", fh.name, err)
		return 0, err
	}
	return uint32(n), nil
}

func (fh *hdfsFileHandle) Release() {
	log.Tracef("hdfs release: fh.name[%s]", fh.name)
	if fh.writer != nil {
		fh.writer.Close()
		fh.writer = nil
	}

	if fh.reader != nil {
		fh.reader.Close()
		fh.reader = nil
	}
}

func (fh *hdfsFileHandle) Flush() error {
	log.Tracef("hdfs flush: fh.name[%s]", fh.name)
	if fh.writer == nil {
		return nil
	}

	return fh.writer.Flush()
}

func (fh *hdfsFileHandle) Fsync(flags int) error {
	if fh.writer == nil {
		return nil
	}
	return fh.writer.Flush()
}

func (fh *hdfsFileHandle) Truncate(size uint64) error {
	log.Tracef("hdfs truncate: fh.name[%s], size[%d]", fh.name, size)
	var err error
	if fh.writer != nil && size == 0 {
		fh.writer.Close()
		err = fh.fs.Truncate(fh.name, size)
		if err != nil {
			log.Debugf("hdfs truncate err:%v", err)
			return err
		}
		fh.writer, err = fh.fs.client.Append(fh.fs.GetPath(fh.name))
		if err != nil {
			log.Debugf("hdfs append err:%v", err)
			return err
		}
	} else {
		err = fh.fs.Truncate(fh.name, size)
		if err != nil {
			log.Debugf("hdfs truncate err:%v", err)
			return err
		}
	}
	return nil
}

func (fh *hdfsFileHandle) Allocate(off uint64, size uint64, mode uint32) error {
	return fmt.Errorf("hdfs allocate: not suported")
}

func (fs *hdfsFileSystem) ReleaseAlreadyBeingCreateFile(path string) error {
	tmp := filepath.Join(filepath.Dir(path), fmt.Sprintf(".%s.tmp.%d", filepath.Base(path), rand.Int()))
	f, err := fs.client.CreateFile(tmp, 3, 128<<20, 0755)
	if err != nil {
		if pe, ok := err.(*os.PathError); ok {
			if remoteErr, ok := pe.Err.(hdfs.Error); ok && remoteErr.Exception() == abcException {
				pe.Err = os.ErrExist
			}
			if pe.Err == os.ErrExist {
				_ = fs.client.Remove(tmp)
				f, err = fs.client.CreateFile(tmp, 3, 128<<20, 0755)
			}
		}
		if err != nil {
			return err
		}
	}
	defer func() { _ = fs.client.RemoveAll(tmp) }()
	in, err := fs.client.Open(path)
	if err != nil {
		return err
	}
	buf := bufPool.Get().(*[]byte)
	defer bufPool.Put(buf)
	_, err = io.CopyBuffer(f, in, *buf)
	if err != nil {
		f.Close()
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	return fs.client.Rename(tmp, path)
}

func NewHdfsFileSystem(properties map[string]interface{}) (UnderFileStorage, error) {
	nameNodeAddress := properties[common.NameNodeAddress].(string)
	options := hdfs.ClientOptions{
		Addresses: strings.Split(nameNodeAddress, ","),
	}
	useDatanodeHostname, ok := properties[common.UseDatanodeHostname].(bool)
	if ok && useDatanodeHostname {
		options.UseDatanodeHostname = true
	}
	options.User = properties[common.UserKey].(string)
	cli, err := hdfs.NewClient(options)
	if err != nil {
		return nil, err
	}

	subpath, ok := properties[common.SubPath]
	if !ok {
		subpath = "/"
	} else {
		subpath := subpath.(string)
		// If dirname is already a directory, MkdirAll does nothing and returns nil.
		if err := cli.MkdirAll(subpath, os.FileMode(0644)); err != nil {
			return nil, err
		}
	}

	blockSize, ok := properties[common.BlockSizeKey].(int64)
	if !ok {
		blockSize = DefaultBlockSize
	}
	replication, ok := properties[common.ReplicationKey].(int)
	if !ok {
		replication = DefaultReplication
	}

	fs := &hdfsFileSystem{
		client:      cli,
		blockSize:   blockSize,
		replication: replication,
		subpath:     subpath.(string),
	}
	runtime.SetFinalizer(fs, func(fs *hdfsFileSystem) {
		fs.client.Close()
	})
	return fs, nil
}

func init() {
	RegisterUFS(common.HDFSType, NewHdfsFileSystem)
}
