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
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	log "github.com/sirupsen/logrus"

	meta "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
	vfs "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/vfs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

type FileSystem struct {
	fsMeta common.FSMeta
	vfs    *vfs.VFS
	stop   chan struct{}
	cache  *metaCache
}

var collectorOnce sync.Once

func collectorRegister() {
	prometheus.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	prometheus.MustRegister(collectors.NewGoCollector())
}

func wrapRegister(fsMeta common.FSMeta) *prometheus.Registry {
	registry := prometheus.NewRegistry() // replace default so only pfs-fuse metrics are exposed
	prometheus.DefaultGatherer = registry
	metricLabels := prometheus.Labels{"fsname": fsMeta.Name, "fsID": fsMeta.ID}
	prometheus.DefaultRegisterer = prometheus.WrapRegistererWithPrefix("pfs_",
		prometheus.WrapRegistererWith(metricLabels, registry))
	collectorOnce.Do(collectorRegister)
	return registry
}

// entryExpire 记录path对应的ino信息，一般不会修改，因此可以不设置过期时间（entryExpire = 0）。
// attrExpire 记录ino对应节点的attr属性，包括mode、uid、gid和mtime等信息，文件修改时会改变。
func NewFileSystem(fsMeta common.FSMeta, links map[string]common.FSMeta, skipSub bool, hasCache bool,
	linkMetaDirPrefix string, config *vfs.Config) (*FileSystem, error) {
	fs := FileSystem{fsMeta: fsMeta, stop: make(chan struct{})}
	// todo:: 客户端增加配置，填充到这里
	// config := &vfs.Config{Cache: cache}
	registry := wrapRegister(fsMeta)
	vfs, err := vfs.InitVFS(fsMeta, links, false, config, registry)
	if err != nil {
		log.Errorf("init vfs failed: %v", err)
		return nil, err
	}
	fs.vfs = vfs
	// 释放fs的时候会结束协程
	runtime.SetFinalizer(&fs, func(fs *FileSystem) {
		close(fs.stop)
	})
	if !skipSub {
		go func() {
			_ = vfs.Meta.LinksMetaUpdateHandler(fs.stop, meta.DefaultLinkUpdateInterval, linkMetaDirPrefix)
		}()
	}

	if hasCache {
		fs.setCache(defaultEntryCacheSize, defaultAttrCacheSize, defaultEntryExpire, defaultAttrExpire)
	}
	return &fs, nil
}

func (fs *FileSystem) setCache(entrySize, attrSize, entryExpire, attrExpire int) {
	fs.cache = NewMetaCache(entrySize, attrSize, entryExpire, attrExpire)
}

func (fs *FileSystem) Open(path_ string) (*File, error) {
	path_ = path.Clean(path_)
	// 参考的os.path， 默认flag=O_RDONLY
	var fh uint64
	flags := uint32(syscall.O_RDONLY)
	ctx := meta.NewEmptyContext()
	attr, ino, sysErr := fs.lookup(ctx, path_, true)
	log.Debugf("fs client looup inode[%v] path[%s]", ino, path_)
	if utils.IsError(sysErr) {
		return nil, sysErr
	}

	var openErr syscall.Errno
	if attr.IsDir() {
		fh, openErr = fs.vfs.OpenDir(ctx, ino)
	} else {
		_, fh, openErr = fs.vfs.Open(ctx, ino, flags)
	}
	if utils.IsError(openErr) {
		return nil, openErr
	}

	info := FileInfo{
		path:      path_,
		size:      int64(attr.Size),
		mtime:     uint64(attr.Mtime),
		mtimensec: uint64(attr.Mtimensec),
		isDir:     attr.IsDir(),
		mode:      utils.StatModeToFileMode(int(attr.Mode)),
	}

	file := NewPFILE(fh, ino, info, fs)
	return &file, nil
}

func (fs *FileSystem) Chmod(name string, mode os.FileMode) error {
	name = path.Clean(name)
	ctx := meta.NewEmptyContext()
	_, ino, sysErr := fs.lookup(ctx, name, true)
	if utils.IsError(sysErr) {
		return sysErr
	}

	entry, err := fs.vfs.SetAttr(ctx, ino, meta.FATTR_MODE, uint32(mode),
		0, 0, 0, 0, 0, 0, 0)
	if utils.IsError(err) {
		return err
	}

	if fs.cache != nil {
		fs.cache.SetAttrItem(ino, entry.Attr)
	}
	return nil
}

func (fs *FileSystem) Chown(name string, uid, gid int) error {
	name = path.Clean(name)
	ctx := meta.NewEmptyContext()
	_, ino, sysErr := fs.lookup(ctx, name, true)
	if utils.IsError(sysErr) {
		return sysErr
	}

	entry, err := fs.vfs.SetAttr(ctx, ino, meta.FATTR_GID|meta.FATTR_UID, 0,
		uint32(uid), uint32(gid), 0, 0, 0, 0, 0)
	if utils.IsError(err) {
		return err
	}

	if fs.cache != nil {
		fs.cache.SetAttrItem(ino, entry.Attr)
	}
	return nil
}

func (fs *FileSystem) Stat(path_ string) (os.FileInfo, error) {
	path_ = path.Clean(path_)
	ctx := meta.NewEmptyContext()
	attr, _, err := fs.lookup(ctx, path_, true)
	if utils.IsError(err) {
		return nil, err
	}

	info := FileInfo{
		path:      path_,
		size:      int64(attr.Size),
		mtime:     uint64(attr.Mtime),
		mtimensec: uint64(attr.Mtimensec),
		isDir:     attr.IsDir(),
		mode:      utils.StatModeToFileMode(int(attr.Mode)),
	}
	return &info, nil
}

func (fs *FileSystem) Mkdir(name string, mode os.FileMode) error {
	name = path.Clean(name)
	ctx := meta.NewEmptyContext()
	_, ino, err := fs.lookup(ctx, path.Dir(name), true)
	if utils.IsError(err) {
		return err
	}

	_, err = fs.vfs.Mkdir(ctx, ino, path.Base(name), uint32(mode), 0)
	if utils.IsError(err) {
		log.Errorf("mkdir path[%s] with mode[%d] failed: %v", name, mode, err)
		return err
	}
	return nil
}

func (fs *FileSystem) Rmdir(name string) error {
	name = path.Clean(name)
	ctx := meta.NewEmptyContext()
	_, ino, err := fs.lookup(ctx, path.Dir(name), true)
	if utils.IsError(err) {
		return err
	}

	err = fs.vfs.Rmdir(ctx, ino, path.Base(name))
	if utils.IsError(err) {
		log.Errorf("rmdir path[%s] failed: %v", name, err)
		return err
	}

	if fs.cache != nil {
		fs.cache.RemoveEntryItem(name)
	}
	return nil
}

func (fs *FileSystem) Rename(oldPath string, newPath string) error {
	oldPath = path.Clean(oldPath)
	newPath = path.Clean(newPath)
	ctx := meta.NewEmptyContext()
	_, oldIno, err := fs.lookup(ctx, path.Dir(oldPath), true)
	if utils.IsError(err) {
		return err
	}

	_, newIno, err := fs.lookup(ctx, path.Dir(newPath), true)
	if utils.IsError(err) {
		return err
	}

	err = fs.vfs.Rename(ctx, oldIno, path.Base(oldPath), newIno, path.Base(newPath), 0)
	if utils.IsError(err) {
		log.Errorf("Rename: oldName[%s], newName[%s], failed: [%v]",
			oldPath, newPath, err)
		return err
	}

	if fs.cache != nil {
		// 因为inode不会复用,只需要删除entry cache信息，attr cache等待过期。
		fs.cache.RemoveEntryItem(oldPath)
	}
	return nil
}

func (fs *FileSystem) Unlink(name string) error {
	name = path.Clean(name)
	ctx := meta.NewEmptyContext()
	_, ino, err := fs.lookup(ctx, path.Dir(name), true)
	if utils.IsError(err) {
		return err
	}
	err = fs.vfs.Unlink(ctx, ino, path.Base(name))
	if utils.IsError(err) {
		log.Errorf("delete path[%s] failed: %v", name, err)
		return err
	}
	if fs.cache != nil {
		fs.cache.RemoveEntryItem(name)
	}
	return nil
}

func (fs *FileSystem) Create(name string, flags uint32, mode uint32) (*File, error) {
	name = path.Clean(name)
	ctx := meta.NewEmptyContext()
	_, ino, err := fs.lookup(ctx, path.Dir(name), true)
	if utils.IsError(err) {
		return nil, err
	}

	entry, fh, err := fs.vfs.Create(ctx, ino, path.Base(name), mode&07777, 0, flags)
	if utils.IsError(err) {
		log.Errorf("create path[%s] failed: %v", name, err)
		return nil, err
	}

	attr := NewFileInfoForCreate(name, os.FileMode(mode))
	file := NewPFILE(fh, entry.Ino, attr, fs)
	return &file, nil
}

func (fs *FileSystem) lookup(ctx *meta.Context, path_ string, canCache bool) (*meta.Attr, vfs.Ino, syscall.Errno) {
	path_ = path.Clean(path_)
	if canCache && fs.cache != nil {
		key, inodeOk := fs.cache.GetEntryItem(path_)

		if inodeOk {
			inode := key.(vfs.Ino)
			attr, attrOk := fs.cache.GetAttrItem(inode)
			if attrOk {
				return attr.(*meta.Attr), inode, syscall.F_OK
			} else {
				entry, err := fs.vfs.GetAttr(ctx, inode)
				if utils.IsError(err) {
					return nil, 0, err
				}
				fs.cache.SetAttrItem(inode, entry.Attr)
				return entry.Attr, inode, syscall.F_OK
			}
		}
	}

	// filepath.Dir(path)不会返回"", 当path为空或者父目录为空均返回"."
	if path_ == "" || path_ == "/" || path_ == "." {
		entry, err := fs.vfs.GetAttr(ctx, vfs.Ino(1))
		if utils.IsError(err) {
			return nil, 1, err
		}
		if fs.cache != nil {
			fs.cache.SetEntryItem("/", entry.Ino)
			fs.cache.SetAttrItem(entry.Ino, entry.Attr)
		}
		return entry.Attr, entry.Ino, syscall.F_OK
	}

	parent := filepath.Dir(path_)
	_, pInode, err := fs.lookup(ctx, parent, canCache)
	if utils.IsError(err) {
		return nil, 0, err
	}
	entry, err := fs.vfs.Lookup(ctx, pInode, filepath.Base(path_))
	if utils.IsError(err) {
		return nil, 0, err
	}
	log.Debugf("the pInode[%d], name[%s], entry[%+v]", pInode, filepath.Base(path_), entry)
	if fs.cache != nil {
		fs.cache.SetEntryItem(path_, entry.Ino)
		fs.cache.SetAttrItem(entry.Ino, entry.Attr)
	}
	return entry.Attr, entry.Ino, syscall.F_OK
}
