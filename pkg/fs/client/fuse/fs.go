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
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/utils"
	"paddleflow/pkg/fs/client/vfs"
)

const fsName = "PaddleFlowFS"

type FileSystem struct {
	// TODO: 基于Pathfs实现的文件系统，未来基于RawFileSystem自定义文件系统
	pathfs.FileSystem
	fs.InodeEmbedder
	debug  bool
	nodeFs *pathfs.PathNodeFs
}

func NewFileSystem(debug bool) *FileSystem {
	return &FileSystem{
		debug: debug,
	}
}

func (fs *FileSystem) String() string {
	return fsName
}

// If called, provide debug output through the log package.
func (fs *FileSystem) SetDebug(debug bool) {
	fs.debug = debug
}

// Attributes.  This function is the main entry point, through
// which FUSE discovers which files and directories exist.
//
// If the filesystem wants to implement hard-links, it should
// return consistent non-zero FileInfo.Ino data.  Using
// hardlinks incurs a performance hit.
func (fs *FileSystem) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	log.Debugf("GetAttr: name [%s]", name)
	attr, err := vfs.GetOldVFS().GetAttr(name, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("GetAttr name[%s] failed: %v", name, err)
		return nil, fuse.Status(err)
	}
	attrOut := fuse.Attr{}
	if attr.Sys != nil {
		stat_t := attr.Sys.(syscall.Stat_t)
		attrOut.FromStat(&stat_t)
	} else {
		// TODO: 由非sys部分填充fuse.Attr
		return nil, fuse.ENOSYS
	}
	return &attrOut, fuse.F_OK
}

// These should update the file's ctime too.
func (fs *FileSystem) Chmod(name string, mode uint32, context *fuse.Context) fuse.Status {
	log.Debugf("Chmod: name [%s], mode [%d]", name, mode)
	err := vfs.GetOldVFS().Chmod(name, mode, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("Chmod name[%s] mode[%d] failed: %v", name, mode, err)
		return fuse.Status(err)
	}
	return fuse.F_OK
}

func (fs *FileSystem) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	log.Debugf("Chown: name [%s], uid[%d], gid[%d]", name, uid, gid)
	err := vfs.GetOldVFS().Chown(name, uid, gid, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("Chown: name[%s] with uid[%d] and gid[%d] failed: %v", name, uid, gid, err)
		return fuse.Status(err)
	}
	return fuse.F_OK
}

func (fs *FileSystem) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (
	code fuse.Status) {
	log.Debugf("Utimens: name[%s] Atime [%v], Mtime[%v]", name, Atime, Mtime)
	err := vfs.GetOldVFS().Utimens(name, Atime, Mtime, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("Utimens: name[%s] Atime [%v], Mtime[%v] failed: [%v]", name, Atime, Mtime, err)
		return fuse.Status(err)
	}
	return fuse.F_OK
}

func (fs *FileSystem) Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status) {
	log.Debugf("Truncate: name[%s], size[%d]", name, size)
	err := vfs.GetOldVFS().Truncate(name, size, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("Truncate: name[%s], size[%d] failed: [%v]", name, size, err)
		return fuse.Status(err)
	}
	return fuse.F_OK
}

func (fs *FileSystem) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	log.Debugf("Access: name[%s], mode[%d]", name, mode)
	err := vfs.GetOldVFS().Access(name, mode, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("Access: name[%s], mode[%d] failed: [%v]", name, mode, err)
		return fuse.Status(err)
	}
	return fuse.F_OK
}

// Tree structure
func (fs *FileSystem) Link(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	return fuse.ENOSYS
}
func (fs *FileSystem) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	log.Debugf("Mkdir: name[%s], mode[%d]", name, mode)
	err := vfs.GetOldVFS().Mkdir(name, mode, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("Mkdir: name[%s], mode[%d] failed: [%v]", name, mode, err)
		return fuse.Status(err)
	}
	return fuse.F_OK
}
func (fs *FileSystem) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
	log.Debugf("Mknod: name[%s], mode[%d], dev[%d]", name, mode, dev)
	err := vfs.GetOldVFS().Mknod(name, mode, dev, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("Mkdir: name[%s], mode[%d], dev[%d] failed: [%v]", name, mode, dev, err)
		return fuse.Status(err)
	}
	return fuse.F_OK
}
func (fs *FileSystem) Rename(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	log.Debugf("Rename: oldName[%v], newName[%v]", oldName, newName)
	err := vfs.GetOldVFS().Rename(oldName, newName, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("Rename: oldName[%v], newName[%v] failed: [%v]", oldName, newName, err)
		return fuse.Status(err)
	}
	return fuse.F_OK
}
func (fs *FileSystem) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	log.Debugf("Rmdir: name[%v]", name)
	err := vfs.GetOldVFS().Rmdir(name, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("Rmdir: name[%v] failed: [%v]", name, err)
		return fuse.Status(err)
	}
	return fuse.F_OK
}
func (fs *FileSystem) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	log.Debugf("Unlink: name[%v]", name)
	err := vfs.GetOldVFS().Unlink(name, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("Unlink: name[%v] failed: [%v]", name, err)
		return fuse.Status(err)
	}
	return fuse.F_OK
}

// Extended attributes.
func (fs *FileSystem) GetXAttr(name string, attribute string, context *fuse.Context) ([]byte, fuse.Status) {
	log.Debugf("GetXAttr: name[%s], attribute[%v]", name, attribute)
	if len(attribute) == 0 {
		return []byte{}, fuse.F_OK
	}
	data, err := vfs.GetOldVFS().GetXAttr(name, attribute, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("GetXAttr: name[%s], attribute[%v] failed: [%v]", name, attribute, err)
		return []byte{}, fuse.Status(err)
	}
	return data, fuse.F_OK
}
func (fs *FileSystem) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
	log.Debugf("ListXAttr: name[%s]", name)
	attrs, err := vfs.GetOldVFS().ListXAttr(name, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("ListXAttr: name[%s] failed: [%v]", name, err)
		return []string{}, fuse.Status(err)
	}
	return attrs, fuse.F_OK
}
func (fs *FileSystem) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	log.Debugf("RemoveXAttr: name[%s], attr[%s]", name, attr)
	err := vfs.GetOldVFS().RemoveXAttr(name, attr, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("RemoveXAttr: name[%s] failed: [%v]", name, err)
		return fuse.Status(err)
	}
	return fuse.F_OK
}
func (fs *FileSystem) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	log.Debugf("SetXAttr: name[%s], attr[%s], data[%s], flags [%d]", name, attr, data, flags)
	err := vfs.GetOldVFS().SetXAttr(name, attr, data, flags, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("SetXAttr: name[%s], attr[%s], data[%s], flags [%d] failed: [%v]",
			name, attr, data, flags, err)
		return fuse.Status(err)
	}
	return fuse.F_OK
}

// Called after mount.
func (fs *FileSystem) OnMount(nodeFs *pathfs.PathNodeFs) {
	fs.nodeFs = nodeFs
}

func (fs *FileSystem) OnUnmount() {
}

// File handling.  If opening for writing, the file's mtime
// should be updated too.
func (fs *FileSystem) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	log.Debugf("Open: name[%s], flags[%d]", name, flags)
	fileHandle, err := vfs.GetOldVFS().Open(name, flags, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("Open: name[%s], flags[%d], failed: [%v]", name, flags, err)
		return fileHandle, fuse.Status(err)
	}
	return fileHandle, fuse.F_OK
}

func (fs *FileSystem) Create(name string, flags uint32, mode uint32, context *fuse.Context) (
	nodefs.File, fuse.Status) {
	log.Debugf("Create: name[%s], flags[%d], mode[%d]", name, flags, mode)
	fileHandle, err := vfs.GetOldVFS().Create(name, flags, mode, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("Create: name[%s], flags[%d], mode[%d], failed: [%v]", name, flags, mode, err)
		return fileHandle, fuse.Status(err)
	}
	return fileHandle, fuse.F_OK
}

// Directory handling
func (fs *FileSystem) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {
	log.Debugf("OpenDir: name[%s]", name)
	dirs, err := vfs.GetOldVFS().OpenDir(name, base.NewContext(context))
	if utils.IsError(err) {
		log.Errorf("OpenDir: name[%s], failed: [%v]", name, err)
		return stream, fuse.Status(err)
	}

	return base.ToDirEntryList(dirs), fuse.F_OK
}

// Symlinks.
func (fs *FileSystem) Symlink(value string, linkName string, context *fuse.Context) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	return "", fuse.ENOSYS
}

func (fs *FileSystem) StatFs(name string) *fuse.StatfsOut {
	log.Debugf("StatFs: name [%s]", name)
	statfs := vfs.GetOldVFS().StatFs(name)
	statfsOut := fuse.StatfsOut(*statfs)
	return &statfsOut
}
