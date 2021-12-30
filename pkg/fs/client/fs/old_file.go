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
	"bytes"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/utils"
	"paddleflow/pkg/fs/client/vfs"
)

// 参考os.file对外的接口实现, 部分未实现
type OldFile struct {
	// fd的值需要填
	fd          int
	file        base.FileHandle
	attr        FileInfo
	vfs         *vfs.OldVFS
	readOffset  int64
	writeOffset int64
}

func NewOldPFILE(fileHanle base.FileHandle, attr FileInfo, vfs *vfs.OldVFS) OldFile {
	return OldFile{
		// TODO: 替换为文件的fd
		fd:          int(0),
		file:        fileHanle,
		attr:        attr,
		vfs:         vfs,
		readOffset:  0,
		writeOffset: attr.size,
	}
}

func (f *OldFile) Name() string {
	return filepath.Base(f.attr.path)
}

func (f *OldFile) Stat() (os.FileInfo, error) {
	return &f.attr, nil
}

func (f *OldFile) Chmod(mode os.FileMode) error {
	err := f.file.Chmod(uint32(mode))
	if utils.IsError(syscall.Errno(err)) {
		return syscall.Errno(err)
	}
	return nil
}

func (f *OldFile) Chown(uid, gid int) error {
	err := f.file.Chown(uint32(uid), uint32(gid))
	if utils.IsError(syscall.Errno(err)) {
		return syscall.Errno(err)
	}
	return nil
}

func (f *OldFile) Close() error {
	if f.file == nil {
		return nil
	}
	err := f.file.Flush()
	// 返回操作不支持的跳过，Release会调用底层存储文件的close
	if utils.IsError(syscall.Errno(err)) && syscall.Errno(err) != syscall.ENOSYS {
		return syscall.Errno(err)
	}
	f.file.Release()
	return nil
}

func (f *OldFile) Fd() uintptr {
	if f.file == nil {
		return ^(uintptr(0))
	}
	return uintptr(f.fd)
}

func (f *OldFile) Read(b []byte) (int, error) {
	if f.readOffset >= f.attr.size {
		return 0, io.EOF
	}
	n, err := f.file.Read(b, f.readOffset)
	if utils.IsError(syscall.Errno(err)) {
		return 0, syscall.Errno(err)
	}
	buffer, err := n.Bytes(b)
	if utils.IsError(syscall.Errno(err)) {
		return 0, syscall.Errno(err)
	}
	log.Debugf("read buffer.size[%d], buffer[%s]", len(buffer), string(buffer))
	// s3存储没有填充b，因此需要判断填充b。local模式需要依赖len(b)填充读取字节长度。
	size := len(buffer)
	if size > len(b) {
		size = len(b)
	} else {
		b = b[0:size]
	}
	if bytes.Compare(buffer[0:size], b) != 0 {
		b = append(b[:0], buffer[0:size]...)
	}
	f.readOffset += int64(size)
	if size == 0 {
		return 0, io.EOF
	}
	return size, nil
}

func (f *OldFile) ReadAt(b []byte, off int64) (int, error) {
	n, err := f.file.Read(b, off)
	if utils.IsError(syscall.Errno(err)) {
		return 0, syscall.Errno(err)
	}
	buffer, err := n.Bytes(b)
	if utils.IsError(syscall.Errno(err)) {
		return 0, syscall.Errno(err)
	}
	log.Debugf("read buffer.size[%d], buffer[%s], b[%s]", len(buffer), string(buffer), string(b))
	return len(buffer), nil
}

func (f *OldFile) ReadDir(n int) ([]os.DirEntry, error) {
	ctx := base.NewDefaultContext()
	stream, err := f.vfs.OpenDir(f.attr.path, ctx)
	if utils.IsError(syscall.Errno(err)) {
		return []os.DirEntry{}, syscall.Errno(err)
	}
	osDirs := make([]os.DirEntry, len(stream))
	for i, _ := range stream {
		info, err := f.vfs.GetAttr(filepath.Join(f.attr.path, stream[i].Name), ctx)
		if utils.IsError(err) {
			return []os.DirEntry{}, err
		}
		dirEntry := NewDirEntry(stream[i], NewFileInfo(info))
		osDirs[i] = &dirEntry
	}
	return osDirs, nil
}

/** os.File实现
func (f *File) ReadFrom(r io.Reader) (n int64, err error) {
	return -1, syscall.ENOSYS
}**/

func (f *OldFile) Readdir(n int) ([]os.FileInfo, error) {
	ctx := base.NewDefaultContext()
	stream, err := f.vfs.OpenDir(f.attr.path, ctx)
	if utils.IsError(syscall.Errno(err)) {
		return []os.FileInfo{}, syscall.Errno(err)
	}
	osDirs := make([]os.FileInfo, len(stream))
	for i, _ := range stream {
		info, err := f.vfs.GetAttr(filepath.Join(f.attr.path, stream[i].Name), ctx)
		if utils.IsError(err) {
			return []os.FileInfo{}, err
		}
		dirInfo := NewFileInfo(info)
		osDirs[i] = &dirInfo
	}
	return osDirs, nil
}

func (f *OldFile) Readdirnames(n int) ([]string, error) {
	ctx := base.NewDefaultContext()
	dirs, err := f.vfs.OpenDir(f.attr.path, ctx)
	if utils.IsError(err) {
		log.Errorf("readdirnames for path[%s] failed: %v", f.attr.path, err)
		return []string{}, err
	}

	if n < 0 {
		n = len(dirs)
	}
	i := 0
	names := make([]string, 0, n)
	for _, dir := range dirs {
		names = append(names, dir.Name)
		i++
		if i == n {
			break
		}
	}
	return names, nil
}

// seek sets the offset for the next Read or Write on file to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
func (f *OldFile) Seek(offset int64, whence int) (ret int64, err error) {
	return -1, syscall.ENOSYS
}

func (f *OldFile) SetDeadline(t time.Time) error {
	return syscall.ENOSYS
}

func (f *OldFile) SetReadDeadline(t time.Time) error {
	return syscall.ENOSYS
}

func (f *OldFile) SetWriteDeadline(t time.Time) error {
	return syscall.ENOSYS
}

func (f *OldFile) Sync() error {
	// 说明：f.fd均为0，参考go-fuse/nodes/files.go的实现，会替换为实际文件f.fd
	err := f.file.Fsync(int(f.fd))
	if utils.IsError(syscall.Errno(err)) {
		return syscall.Errno(err)
	}
	return nil
}

func (f *OldFile) SyscallConn() (syscall.RawConn, error) {
	return nil, syscall.ENOSYS
}

func (f *OldFile) Truncate(size int64) error {
	err := f.file.Truncate(uint64(size))
	if utils.IsError(syscall.Errno(err)) {
		return syscall.Errno(err)
	}
	return nil
}

func (f *OldFile) Write(b []byte) (int, error) {
	// 默认为追加写
	offset := f.writeOffset
	n, status := f.file.Write(b, offset)
	log.Debugf("write data[%s] offset[%d]", string(b), offset)
	log.Debugf("return n[%d], err[%s]", n, status)
	if utils.IsError(syscall.Errno(status)) {
		return 0, syscall.Errno(status)
	}
	f.writeOffset = f.writeOffset + int64(n)
	return int(n), nil
}

func (f *OldFile) WriteAt(b []byte, off int64) (int, error) {
	n, status := f.file.Write(b, off)
	if utils.IsError(syscall.Errno(status)) {
		return 0, syscall.Errno(status)
	}
	return int(n), nil
}

func (f *OldFile) WriteString(s string) (n int, err error) {
	return f.Write([]byte(s))
}
