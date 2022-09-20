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

package fs

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	meta "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta_new"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/vfs_new"
)

type FileInfo struct {
	path      string
	size      int64
	mtime     uint64
	mtimensec uint64
	isDir     bool
	mode      os.FileMode
	sys       interface{}
}

func NewFileInfo(attr *base.FileInfo) FileInfo {
	if attr == nil {
		return FileInfo{}
	}
	return FileInfo{
		path:  attr.Name,
		size:  attr.Size,
		mtime: attr.Mtime,
		isDir: attr.IsDir,
		mode:  attr.Mode,
		sys:   attr.Sys,
	}
}

func NewFileInfoForCreate(path string, mode os.FileMode) FileInfo {
	return FileInfo{
		path:  path,
		isDir: false,
		mode:  mode,
		mtime: uint64(time.Now().Second()),
	}
}

func (i *FileInfo) Name() string {
	return path.Base(i.path)
}

func (i *FileInfo) Size() int64 {
	return i.size
}

func (i *FileInfo) Mode() os.FileMode {
	return i.mode
}

func (i *FileInfo) ModTime() time.Time {
	return time.Unix(0, int64(i.mtime)*1e9+int64(i.mtimensec))
}

func (i *FileInfo) Sys() interface{} {
	return i.sys
}

func (i *FileInfo) IsDir() bool {
	return i.isDir
}

func (i *FileInfo) String() string {
	return fmt.Sprintf("path:[%s],size:[%d],mode:[%d],modTime:[%v],isDir:[%v]",
		i.path, i.size, i.mode, i.ModTime(), i.isDir)
}

type DirEntry struct {
	mode uint32
	name string
	ino  uint64
	info FileInfo
}

func NewDirEntry(dir base.DirEntry, info FileInfo) DirEntry {
	return DirEntry{
		mode: dir.Mode,
		name: dir.Name,
		ino:  dir.Ino,
		info: info,
	}
}

func (d *DirEntry) Name() string {
	return d.name
}

func (d *DirEntry) IsDir() bool {
	return os.FileMode(d.mode)&os.ModeDir != 0
}

func (d *DirEntry) Type() os.FileMode {
	return os.FileMode(d.mode)
}

func (d *DirEntry) Info() (os.FileInfo, error) {
	return &d.info, nil
}

// 参考os.file对外的接口实现, 部分未实现
type File struct {
	inode       vfs.Ino
	fh          uint64
	attr        FileInfo
	readOffset  int64
	writeOffset int64

	fs *FileSystem
}

func NewPFILE(fh uint64, inode vfs.Ino, attr FileInfo, fs *FileSystem) File {
	return File{
		inode:       inode,
		fh:          fh,
		attr:        attr,
		readOffset:  0,
		writeOffset: attr.size,
		fs:          fs,
	}
}

func (f *File) Name() string {
	return filepath.Base(f.attr.path)
}

func (f *File) Stat() (os.FileInfo, error) {
	return &f.attr, nil
}

func (f *File) Chmod(mode os.FileMode) error {
	ctx := meta.NewEmptyContext()
	entry, err := f.fs.vfs.SetAttr(ctx, f.inode, meta.FATTR_MODE, uint32(mode),
		0, 0, 0, 0, 0, 0, 0)
	if utils.IsError(err) {
		return err
	}

	if f.fs.cache != nil {
		f.fs.cache.SetAttrItem(f.inode, entry.Attr)
	}
	return nil
}

func (f *File) Chown(uid, gid int) error {
	ctx := meta.NewEmptyContext()

	entry, err := f.fs.vfs.SetAttr(ctx, f.inode, meta.FATTR_GID|meta.FATTR_UID, 0,
		uint32(uid), uint32(gid), 0, 0, 0, 0, 0)
	if utils.IsError(err) {
		return err
	}

	if f.fs.cache != nil {
		f.fs.cache.SetAttrItem(f.inode, entry.Attr)
	}
	return nil
}

func (f *File) Close() error {
	if f.fh == 0 {
		return nil
	}
	ctx := meta.NewEmptyContext()
	err := f.fs.vfs.Flush(ctx, f.inode, f.fh, uint64(0))
	if utils.IsError(err) {
		log.Errorf("file close: [%s] vfs.Flush err: %v", f.attr.path, err)
		return err
	}
	f.fs.vfs.Release(ctx, f.inode, f.fh)

	if f.fs.cache != nil {
		// 文件可能有改变，close的时候设置attr缓存失效
		f.fs.cache.RemoveAttrItem(f.inode)
	}
	return nil
}

func (f *File) Fd() uintptr {
	if f.fh == 0 {
		return ^(uintptr(0))
	}
	return uintptr(f.fh)
}

func (f *File) Read(b []byte) (int, error) {
	if f.readOffset >= f.attr.size {
		return 0, io.EOF
	}

	ctx := meta.NewEmptyContext()
	n, err := f.fs.vfs.Read(ctx, f.inode, b, uint64(f.readOffset), f.fh)
	if utils.IsError(err) || err == io.EOF {
		return 0, err
	}
	f.readOffset += int64(n)
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (f *File) ReadAt(b []byte, off int64) (int, error) {
	ctx := meta.NewEmptyContext()
	n, err := f.fs.vfs.Read(ctx, f.inode, b, uint64(off), f.fh)
	if utils.IsError(err) || err == io.EOF {
		return 0, err
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (f *File) ReadDir(n int) ([]os.DirEntry, error) {
	ctx := meta.NewEmptyContext()
	path_ := f.fs.vfs.Meta.InoToPath(f.inode)
	entries, err := f.fs.vfs.ReadDir(ctx, f.inode, f.fh, 0)
	if utils.IsError(err) {
		return []os.DirEntry{}, err
	}
	osDirs := make([]os.DirEntry, len(entries))
	for i, info := range entries {
		// TODO: remove base.entry and base.fileInfo
		entry := base.DirEntry{
			Mode: info.Attr.Mode,
			Name: info.Name,
			Ino:  uint64(info.Ino),
		}
		fileInfo := FileInfo{
			path:      path_,
			size:      int64(info.Attr.Size),
			mtime:     uint64(info.Attr.Mtime),
			mtimensec: uint64(info.Attr.Mtimensec),
			isDir:     info.Attr.IsDir(),
			mode:      utils.StatModeToFileMode(int(info.Attr.Mode)),
		}
		dirEntry := NewDirEntry(entry, fileInfo)
		osDirs[i] = &dirEntry
	}
	return osDirs, nil
}

/** os.File实现
func (f *File) ReadFrom(r io.Reader) (n int64, err error) {
	return -1, syscall.ENOSYS
}**/

func (f *File) Readdir(n int) ([]os.FileInfo, error) {
	ctx := meta.NewEmptyContext()
	entries, err := f.fs.vfs.ReadDir(ctx, f.inode, f.fh, 0)
	if utils.IsError(err) {
		return []os.FileInfo{}, err
	}
	dirInfos := make([]os.FileInfo, len(entries))
	for i, info := range entries {
		fileInfo := FileInfo{
			path:      info.Name,
			size:      int64(info.Attr.Size),
			mtime:     uint64(info.Attr.Mtime),
			mtimensec: uint64(info.Attr.Mtimensec),
			isDir:     info.Attr.IsDir(),
			mode:      utils.StatModeToFileMode(int(info.Attr.Mode)),
		}
		dirInfos[i] = &fileInfo
	}
	return dirInfos, nil
}

func (f *File) Readdirnames(n int) ([]string, error) {
	ctx := meta.NewEmptyContext()
	entries, err := f.fs.vfs.ReadDir(ctx, f.inode, f.fh, 0)
	if utils.IsError(err) {
		return []string{}, err
	}
	dirNames := make([]string, len(entries))
	for i, info := range entries {
		dirNames[i] = info.Name
	}
	return dirNames, nil
}

// seek sets the offset for the next Read or Write on file to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
func (f *File) Seek(offset int64, whence int) (ret int64, err error) {
	return -1, syscall.ENOSYS
}

func (f *File) SetDeadline(t time.Time) error {
	return syscall.ENOSYS
}

func (f *File) SetReadDeadline(t time.Time) error {
	return syscall.ENOSYS
}

func (f *File) SetWriteDeadline(t time.Time) error {
	return syscall.ENOSYS
}

func (f *File) Sync() error {
	ctx := meta.NewEmptyContext()
	err := f.fs.vfs.Fsync(ctx, f.inode, 0, f.fh)
	if utils.IsError(err) {
		return err
	}
	return nil
}

func (f *File) SyscallConn() (syscall.RawConn, error) {
	return nil, syscall.ENOSYS
}

func (f *File) Truncate(size int64) error {
	ctx := meta.NewEmptyContext()
	err := f.fs.vfs.Truncate(ctx, f.inode, uint64(size), f.fh)
	if utils.IsError(err) {
		return err
	}

	if f.fs.cache != nil {
		// 文件可能有改变，close的时候设置attr缓存失效
		f.fs.cache.RemoveAttrItem(f.inode)
	}
	return nil
}

func (f *File) Write(b []byte) (int, error) {
	// 默认为追加写
	ctx := meta.NewEmptyContext()
	err := f.fs.vfs.Write(ctx, f.inode, b, uint64(f.writeOffset), f.fh)
	if utils.IsError(err) {
		return 0, err
	}
	size := len(b)
	f.writeOffset = f.writeOffset + int64(size)
	return size, nil
}

func (f *File) WriteAt(b []byte, off int64) (int, error) {
	ctx := meta.NewEmptyContext()
	err := f.fs.vfs.Write(ctx, f.inode, b, uint64(off), f.fh)
	if utils.IsError(err) {
		return 0, err
	}
	size := len(b)
	return size, nil
}

func (f *File) WriteString(s string) (n int, err error) {
	return f.Write([]byte(s))
}
