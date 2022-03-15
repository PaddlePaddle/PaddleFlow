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

package vfs

import (
	"os"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/cache"
	"paddleflow/pkg/fs/client/meta"
	ufslib "paddleflow/pkg/fs/client/ufs"
	"paddleflow/pkg/fs/client/utils"
	"paddleflow/pkg/fs/common"
)

type VFS struct {
	fsMeta     common.FSMeta
	links      []common.FSMeta
	reader     DataReader
	writer     DataWriter
	handleMap  map[Ino][]*handle
	handleLock sync.RWMutex
	nextfh     uint64
	Meta       meta.Meta
	Store      cache.Store
}

type Config struct {
	Cache *cache.Config
	owner *Owner
	Meta  *meta.Config
}

type Owner struct {
	uid uint32
	gid uint32
}

type Ino = meta.Ino
type Attr = meta.Attr
type ufsMap = sync.Map

var (
	vfsop *VFS
)

type Option func(*Config)

func InitConfig(options ...Option) *Config {
	config := &Config{
		Cache: &cache.Config{
			Mem: &cache.MemConfig{},
			Disk: &cache.DiskConfig{
				Expire: 60 * time.Second,
			},
		},
	}
	for _, f := range options {
		f(config)
	}
	return config
}

func WithMemorySize(cacheSize int) Option {
	return func(config *Config) {
		config.Cache.Mem.CacheSize = cacheSize
	}
}

func WithMemoryExpire(expire time.Duration) Option {
	return func(config *Config) {
		config.Cache.Mem.Expire = expire
	}
}

func WithDiskExpire(expire time.Duration) Option {
	return func(config *Config) {
		config.Cache.Disk.Expire = expire
	}
}

func WithBlockSize(size int) Option {
	return func(config *Config) {
		config.Cache.BlockSize = size
	}
}

func WithDiskCachePath(path string) Option {
	return func(config *Config) {
		config.Cache.Disk.Dir = path
	}
}

func WithOwner(uid, gid uint32) Option {
	return func(config *Config) {
		config.owner = &Owner{
			uid: uid,
			gid: gid,
		}
	}
}

func WithMetaConfig(m meta.Config) Option {
	return func(config *Config) {
		config.Meta = &m
	}
}

func InitVFS(fsMeta common.FSMeta, links map[string]common.FSMeta, global bool, config *Config) (*VFS, error) {
	vfs := &VFS{
		fsMeta: fsMeta,
	}
	inodeHandle := meta.NewInodeHandle()
	inodeHandle.InitRootNode()
	if config == nil {
		config = &Config{}
	}
	vfsMeta, err := meta.NewMeta(fsMeta, links, inodeHandle, config.Meta)

	if err != nil {
		log.Errorf("new default meta failed: %v", err)
		return nil, err
	}
	if config.owner != nil {
		vfsMeta.SetOwner(config.owner.uid, config.owner.gid)
	}
	vfs.Meta = vfsMeta
	var store cache.Store
	var blockSize int
	if config.Cache != nil {
		store = cache.NewCacheStore(config.Cache)
		blockSize = config.Cache.BlockSize
	}
	vfs.Store = store
	vfs.reader = NewDataReader(vfs.Meta, blockSize, store)
	vfs.writer = NewDataWriter(vfs.Meta, blockSize, store)
	vfs.handleMap = make(map[Ino][]*handle)
	vfs.nextfh = 1

	if global {
		vfsop = vfs
	}
	log.Debugf("Init VFS: %+v", vfs)
	return vfs, nil
}

func GetVFS() *VFS {
	if vfsop == nil {
		log.Errorf("vfs is not initialized")
		os.Exit(-1)
	}
	return vfsop
}

func (v *VFS) getUFS(name string) (ufslib.UnderFileStorage, bool, string, string) {
	return v.Meta.GetUFS(name)
}

// Lookup is called by the kernel when the VFS wants to know
// about a file inside a directory. Many lookup calls can
// occur in parallel, but only one call happens for each (dir,
// name) pair.
func (v *VFS) Lookup(ctx *meta.Context, parent Ino, name string) (entry *meta.Entry, err syscall.Errno) {
	var attr = &Attr{}
	var inode Ino
	inode, attr, err = v.Meta.Lookup(ctx, parent, name)
	log.Debugf("Lookup from meta %+v inode %v", attr, inode)
	if utils.IsError(err) {
		return nil, err
	}
	entry = &meta.Entry{Ino: inode, Attr: attr}
	return entry, err
}

// Attributes.
func (v *VFS) GetAttr(ctx *meta.Context, ino Ino) (entry *meta.Entry, err syscall.Errno) {
	var attr = &Attr{}
	err = v.Meta.GetAttr(ctx, ino, attr)
	log.Debugf("GetAttr attr %+v", *attr)
	if utils.IsError(err) {
		return nil, err
	}
	entry = &meta.Entry{Ino: ino, Attr: attr}
	return entry, err
}

func (v *VFS) SetAttr(ctx *meta.Context, ino Ino, set, mode, uid, gid uint32, atime, mtime int64, atimensec, mtimensec uint32, size uint64) (entry *meta.Entry, err syscall.Errno) {
	log.Tracef("vfs setAttr: ino[%d], set[%d], mode[%d], uid[%d], gid[%d], size[%d]", ino, set, mode, uid, gid, size)
	attr := &Attr{
		Mode:      mode,
		Uid:       uid,
		Gid:       gid,
		Atime:     atime,
		Mtime:     mtime,
		Atimensec: atimensec,
		Mtimensec: mtimensec,
		Size:      size,
	}
	err = v.Meta.SetAttr(ctx, ino, set, attr)
	if utils.IsError(err) {
		return entry, err
	}

	// only truncate opened files
	if set&meta.FATTR_SIZE != 0 {
		fhs := v.findAllHandle(ino)
		if fhs != nil {
			for _, h := range fhs {
				if h.writer != nil {
					err = h.writer.Truncate(size)
					if utils.IsError(err) {
						return entry, err
					}
					if v.Store != nil {
						delCacheErr := v.Store.InvalidateCache(v.Meta.InoToPath(ino), int(attr.Size))
						if delCacheErr != nil {
							// todo:: 先忽略删除缓存的错误，需要保证一致性
							log.Errorf("vfs setAttr: truncate delete cache error %v:", delCacheErr)
						}
					}
				}
			}
		}
	}
	attr.Size = size
	entry = &meta.Entry{Ino: ino, Attr: attr}
	return
}

// Modifying structure.
func (v *VFS) Mknod(ctx *meta.Context, parent Ino, name string, mode uint32, rdev uint32) (entry *meta.Entry, err syscall.Errno) {
	var ino Ino
	attr := &Attr{}
	err = v.Meta.Mknod(ctx, parent, name, mode, rdev, &ino, attr)
	entry = &meta.Entry{Ino: ino, Attr: attr}
	return
}

func (v *VFS) Mkdir(ctx *meta.Context, parent Ino, name string, mode uint32) (entry *meta.Entry, err syscall.Errno) {
	var ino Ino
	attr := &Attr{}
	err = v.Meta.Mkdir(ctx, parent, name, mode, &ino, attr)
	entry = &meta.Entry{Ino: ino, Attr: attr}
	return
}

func (v *VFS) Unlink(ctx *meta.Context, parent Ino, name string) (err syscall.Errno) {
	err = v.Meta.Unlink(ctx, parent, name)
	return err
}

func (v *VFS) Rmdir(ctx *meta.Context, parent Ino, name string) (err syscall.Errno) {
	err = v.Meta.Rmdir(ctx, parent, name)
	return err
}

// semantic of rename:
// rename("any", "not_exists") = ok
// rename("file1", "file2") = ok
// rename("empty_dir1", "empty_dir2") = ok
// rename("nonempty_dir1", "empty_dir2") = ok
// rename("nonempty_dir1", "nonempty_dir2") = ENOTEMPTY
// rename("file", "dir") = EISDIR
// rename("dir", "file") = ENOTDIR
func (v *VFS) Rename(ctx *meta.Context, parent Ino, name string, newparent Ino, newname string, flags uint32) (err syscall.Errno) {
	var ino Ino
	attr := &Attr{}
	err = v.Meta.Rename(ctx, parent, name, newparent, newname, flags, &ino, attr)
	if utils.IsError(err) {
		return err
	}
	if v.Store != nil {
		delCacheErr := v.Store.InvalidateCache(v.Meta.InoToPath(parent)+"/"+name, int(attr.Size))
		if delCacheErr != nil {
			// todo:: 先忽略删除缓存的错误，需要保证一致性
			log.Errorf("rename delete cache error %v:", delCacheErr)
		}
		delCacheErr = v.Store.InvalidateCache(v.Meta.InoToPath(ino), int(attr.Size))
		if delCacheErr != nil {
			// todo:: 先忽略删除缓存的错误，需要保证一致性
			log.Errorf("rename delete cache error %v:", delCacheErr)
		}
	}
	return
}

func (v *VFS) Link(ctx *meta.Context, ino Ino, newparent Ino, newname string) (entry *meta.Entry, err syscall.Errno) {
	return nil, syscall.ENOSYS
}

func (v *VFS) Symlink(ctx *meta.Context, path string, parent Ino, name string) (entry *meta.Entry, err syscall.Errno) {
	return nil, syscall.ENOSYS
}

func (v *VFS) Readlink(ctx *meta.Context, ino Ino) (path []byte, err syscall.Errno) {
	return nil, syscall.ENOSYS
}

func (v *VFS) Access(ctx *meta.Context, ino Ino, mask uint32) (err syscall.Errno) {
	err = v.Meta.Access(ctx, ino, mask, nil)
	return err
}

// Extended attributes.

// GetXAttr reads an extended attribute, and should return the
// number of bytes. If the buffer is too small, return ERANGE,
// with the required buffer size.
func (v *VFS) GetXAttr(ctx *meta.Context, ino Ino, name string, size uint32) (data []byte, err syscall.Errno) {
	err = v.Meta.GetXattr(ctx, ino, name, &data)
	if size > 0 && len(data) > int(size) {
		err = syscall.ERANGE
	}
	return
}

// ListXAttr lists extended attributes as '\0' delimited byte
// slice, and return the number of bytes. If the buffer is too
// small, return ERANGE, with the required buffer size.
func (v *VFS) ListXAttr(ctx *meta.Context, ino Ino, size uint32) (data []byte, err syscall.Errno) {
	var attrs []string
	err = v.Meta.ListXattr(ctx, ino, &attrs)
	for _, value := range attrs {
		data = append(data, value...)
		data = append(data, 0)
	}
	if size > 0 && len(data) > int(size) {
		err = syscall.ERANGE
	}

	return
}

// SetAttr writes an extended attribute.
func (v *VFS) SetXAttr(ctx *meta.Context, ino Ino, name string, value []byte, flags uint32) (err syscall.Errno) {
	err = v.Meta.SetXattr(ctx, ino, name, value, flags)
	return
}

// RemoveXAttr removes an extended attribute.
func (v *VFS) RemoveXAttr(ctx *meta.Context, ino Ino, name string) (err syscall.Errno) {
	err = v.Meta.RemoveXattr(ctx, ino, name)
	return
}

// File handling.
func (v *VFS) Create(ctx *meta.Context, parent Ino, name string, mode uint32, cumask uint16, flags uint32) (entry *meta.Entry, fh uint64, err syscall.Errno) {
	var ino Ino
	attr := &Attr{}
	ufs, path, err := v.Meta.Create(ctx, parent, name, mode, cumask, flags, &ino, attr)
	if utils.IsError(err) {
		return
	}
	entry = &meta.Entry{Ino: ino, Attr: attr}
	fh, errHandle := v.newFileHandle(ino, attr.Size, flags, ufs, path)
	if errHandle != nil {
		log.Errorf("new file handle err:%v", err)
		return nil, 0, utils.ToSyscallErrno(errHandle)
	}
	if v.Store != nil {
		delCacheErr := v.Store.InvalidateCache(v.Meta.InoToPath(ino), int(attr.Size))
		if delCacheErr != nil {
			// todo:: 先忽略删除缓存的错误，需要保证一致性
			log.Errorf("create delete cache error %v:", delCacheErr)
		}
	}
	return entry, fh, syscall.F_OK
}

func (v *VFS) Open(ctx *meta.Context, ino Ino, flags uint32) (entry *meta.Entry, fh uint64, err syscall.Errno) {
	var attr = &Attr{}
	ufs, path, err := v.Meta.Open(ctx, ino, flags, attr)
	if utils.IsError(err) {
		return
	}
	var errOpen error
	fh, errOpen = v.newFileHandle(ino, attr.Size, flags, ufs, path)
	if errOpen != nil {
		return entry, fh, utils.ToSyscallErrno(errOpen)
	}
	return entry, fh, syscall.F_OK
}

func (v *VFS) Read(ctx *meta.Context, ino Ino, buf []byte, off uint64, fh uint64) (n int, err syscall.Errno) {
	h := v.findHandle(ino, fh)
	if h == nil {
		err = syscall.EBADF
		return
	}
	if h.reader == nil {
		err = syscall.EACCES
		return
	}
	// todo:: 对读入的文件大小加上限制
	n, err = h.reader.Read(buf, off)
	for err == syscall.EAGAIN {
		n, err = h.reader.Read(buf, off)
	}
	return
}

// File locking
func (v *VFS) GetLk(ctx *meta.Context, ino Ino, fh uint64, owner uint64, start, len *uint64, typ *uint32, pid *uint32) (err syscall.Errno) {
	return syscall.ENOSYS
}

func (v *VFS) SetLk(ctx *meta.Context, ino Ino, fh uint64, owner uint64, start, end uint64, typ uint32, pid uint32, block bool) (err syscall.Errno) {
	return syscall.ENOSYS
}

func (v *VFS) SetLkw(ctx *meta.Context, ino Ino, fh uint64, owner uint64, start, end uint64, typ uint32, pid uint32, block bool) (err syscall.Errno) {
	return syscall.ENOSYS
}

func (v *VFS) Write(ctx *meta.Context, ino Ino, buf []byte, off, fh uint64) (err syscall.Errno) {
	h := v.findHandle(ino, fh)
	if h == nil {
		err = syscall.EBADF
		return
	}
	if h.writer == nil {
		err = syscall.EACCES
		return
	}
	// todo:: 对写入的文件大小加上限制
	// todo:: 限制并发写的情况
	err = h.writer.Write(buf, off)
	if utils.IsError(err) {
		return err
	}
	err = v.Meta.Write(ctx, ino, uint32(off), len(buf))
	return err
}

func (v *VFS) CopyFileRange(ctx *meta.Context, nodeIn Ino, fhIn, offIn uint64, nodeOut Ino, fhOut, offOut, size uint64, flags uint32) (copied uint64, err syscall.Errno) {
	return 0, syscall.ENOSYS
}

func (v *VFS) Flush(ctx *meta.Context, ino Ino, fh uint64, lockOwner uint64) (err syscall.Errno) {
	h := v.findHandle(ino, fh)
	if h == nil {
		err = syscall.EBADF
		return
	}
	if h.writer != nil {
		err = h.writer.Flush()
	}
	return err
}

func (v *VFS) Fsync(ctx *meta.Context, ino Ino, datasync int, fh uint64) (err syscall.Errno) {
	h := v.findHandle(ino, fh)
	if h == nil {
		err = syscall.EBADF
		return
	}
	if h.writer != nil {
		err = h.writer.Fsync(int(fh))
	}
	return err
}

func (v *VFS) Fallocate(ctx *meta.Context, ino Ino, mode uint8, off, length int64, fh uint64) (err syscall.Errno) {
	h := v.findHandle(ino, fh)
	if h == nil {
		err = syscall.EBADF
		return
	}
	if h.writer != nil {
		err = h.writer.Fallocate(length, off, uint32(mode))
	}
	err = v.Meta.Write(ctx, ino, uint32(off), int(length))
	return err
}

// Directory handling
func (v *VFS) OpenDir(ctx *meta.Context, ino Ino) (fh uint64, err syscall.Errno) {
	return v.newHandle(ino).fh, syscall.F_OK
}

func (v *VFS) ReadDir(ctx *meta.Context, ino Ino, fh uint64, offset uint64) (entries []*meta.Entry, err syscall.Errno) {
	h := v.findHandle(ino, fh)
	if h == nil {
		return nil, syscall.EBADF
	}
	if h.children == nil || offset == 0 {
		err = v.Meta.Readdir(ctx, ino, &entries)
		if utils.IsError(err) {
			log.Errorf("Readdir Err %v", err)
			return nil, err
		}
		h.children = entries
	}
	if int(offset) < len(h.children) {
		entries = h.children[offset:]
	}

	return entries, syscall.F_OK
}

func (v *VFS) ReleaseDir(ctx *meta.Context, ino Ino, fh uint64) {
	v.releaseFileHandle(ino, fh)
	log.Debugf("release dir inode %v", ino)
	return
}

func (v *VFS) Release(ctx *meta.Context, ino Ino, fh uint64) {
	if fh > 0 {
		v.releaseFileHandle(ino, fh)
		log.Debugf("release inode %v", ino)
		return
	}
}

func (v *VFS) StatFs(ctx *meta.Context) (*base.StatfsOut, syscall.Errno) {
	statFs, err := v.Meta.StatFS(ctx)
	if utils.IsError(err) {
		return &base.StatfsOut{}, err
	}
	return statFs, syscall.F_OK
}

func (v *VFS) Truncate(ctx *meta.Context, ino Ino, size, fh uint64) (err syscall.Errno) {
	log.Tracef("vfs truncate: ino[%d], size[%d], fh[%d]", ino, size, fh)
	h := v.findHandle(ino, fh)
	if h == nil {
		err = syscall.EBADF
		log.Errorf("vfs truncate: no file handle")
		return
	}
	if h.writer == nil {
		err = syscall.EACCES
		log.Errorf("vfs truncate: no file writer")
		return
	}

	err = h.writer.Truncate(size)
	if utils.IsError(err) {
		log.Debugf("vfs truncate: h.writer.Truncate err")
		return err
	}
	return v.Meta.Truncate(ctx, ino, size)
}
