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

package meta_new

import (
	"syscall"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv_new"
	ufslib "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs_new"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

const (
	FATTR_MODE      = (1 << 0)
	FATTR_UID       = (1 << 1)
	FATTR_GID       = (1 << 2)
	FATTR_SIZE      = (1 << 3)
	FATTR_ATIME     = (1 << 4)
	FATTR_MTIME     = (1 << 5)
	FATTR_FH        = (1 << 6)
	FATTR_ATIME_NOW = (1 << 7)
	FATTR_MTIME_NOW = (1 << 8)
	FATTR_LOCKOWNER = (1 << 9)
	FATTR_CTIME     = (1 << 10)
)

const (
	TypeFile      = 1 // type for regular file
	TypeDirectory = 2 // type for directory
	TypeSymlink   = 3 // type for symlink
	TypeFIFO      = 4 // type for FIFO node
	TypeBlockDev  = 5 // type for block device
	TypeCharDev   = 6 // type for character device
	TypeSocket    = 7 // type for socket
)

type Ino uint64

// Attr represents attributes of a node.
type Attr struct {
	Type      uint8  // type of a node
	Mode      uint32 // permission mode
	Uid       uint32 // owner id
	Gid       uint32 // group id of owner
	Rdev      uint64 // device number
	Atime     int64  // last access time
	Mtime     int64  // last modified time
	Ctime     int64  // last change time for meta
	Atimensec uint32 // nanosecond part of atime
	Mtimensec uint32 // nanosecond part of mtime
	Ctimensec uint32 // nanosecond part of ctime
	Nlink     uint64 // number of links (sub-directories or hardlinks)
	Size      uint64 // size of regular file
	Blksize   int64  // 目录默认4096 文件为0
	Block     int64  // 文件size的大小/512
}

// Entry is an entry inside a directory.
type Entry struct {
	Attr *Attr
	// Name is the basename of the file in the directory.
	Name string
	// Ino is the inode number.
	Ino Ino
}

type Creator func(meta Meta, config Config) (Meta, error)

type Config struct {
	kv_new.Config
	AttrCacheExpire    time.Duration
	EntryCacheExpire   time.Duration
	AttrCacheSize      uint64
	EntryAttrCacheSize uint64
}

// Meta is a interface for a meta service for file system.
type Meta interface {
	// GetUFS returns ufs of link and path
	GetUFS(name string) (ufslib.UnderFileStorage, bool, string, string)

	// Name of database
	Name() string
	InitRootInode() error
	InoToPath(inode Ino) string

	SetOwner(uid, gid uint32)

	// StatFS returns summary statistics of a volume.
	StatFS(ctx *Context) (*base.StatfsOut, syscall.Errno)
	// Access checks the access permission on given inode.
	Access(ctx *Context, inode Ino, mask uint32, attr *Attr) syscall.Errno
	// Lookup returns the inode and attributes for the given entry in a directory.
	Lookup(ctx *Context, parent Ino, name string) (Ino, *Attr, syscall.Errno)
	// Resolve fetches the inode and attributes for an entry identified by the given path.
	// ENOTSUP will be returned if there's no natural implementation for this operation or
	// if there are any symlink following involved.
	Resolve(ctx *Context, parent Ino, path string, inode *Ino, attr *Attr) syscall.Errno
	// GetAttr returns the attributes for given node.
	GetAttr(ctx *Context, inode Ino, attr *Attr) syscall.Errno
	// SetAttr updates the attributes for given node.
	SetAttr(ctx *Context, inode Ino, set uint32, attr *Attr) (string, syscall.Errno)
	// Truncate changes the length for given file.
	Truncate(ctx *Context, inode Ino, size uint64) syscall.Errno
	// Fallocate preallocate given space for given file.
	Fallocate(ctx *Context, inode Ino, mode uint8, off uint64, size uint64) syscall.Errno
	// ReadLink returns the target of a symlink.
	ReadLink(ctx *Context, inode Ino, path *[]byte) syscall.Errno
	// Symlink creates a symlink in a directory with given name.
	Symlink(ctx *Context, parent Ino, name string, path string, inode *Ino, attr *Attr) syscall.Errno
	// Mknod creates a node in a directory with given name, type and permissions.
	Mknod(ctx *Context, parent Ino, name string, _type uint8, mode, cumask uint32, rdev uint32, inode *Ino, attr *Attr) syscall.Errno
	// Mkdir creates a sub-directory with given name and mode.
	Mkdir(ctx *Context, parent Ino, name string, mode uint32, cumask uint16, inode *Ino, attr *Attr) syscall.Errno
	// Unlink removes a file entry from a directory.
	// The file will be deleted if it's not linked by any entries and not open by any sessions.
	Unlink(ctx *Context, parent Ino, name string) syscall.Errno
	// Rmdir removes an empty sub-directory.
	Rmdir(ctx *Context, parent Ino, name string) syscall.Errno
	// Rename move an entry from a source directory to another with given name.
	// The targeted entry will be overwrited if it's a file or empty directory.
	Rename(ctx *Context, parentSrc Ino, nameSrc string, parentDst Ino, nameDst string, flags uint32, inode *Ino, attr *Attr) (string, string, syscall.Errno)
	// Link creates an entry for node.
	Link(ctx *Context, inodeSrc, parent Ino, name string, attr *Attr) syscall.Errno
	// Readdir returns all entries for given directory, which include attributes if plus is true.
	Readdir(ctx *Context, inode Ino, entries *[]*Entry) syscall.Errno
	// Create creates a file in a directory with given name.
	Create(ctx *Context, parent Ino, name string, mode uint32, cumask uint16, flags uint32, inode *Ino, attr *Attr) (ufslib.UnderFileStorage, string, syscall.Errno)
	// Open checks permission on a node and track it as open.
	Open(ctx *Context, inode Ino, flags uint32, attr *Attr) (ufslib.UnderFileStorage, string, syscall.Errno)
	// Close a file.
	Close(ctx *Context, inode Ino) syscall.Errno
	// Read returns the list of blocks
	Read(ctx *Context, inode Ino, indx uint32, buf []byte) syscall.Errno
	// Write put a slice of data on top of the given chunk.
	Write(ctx *Context, inode Ino, off uint32, length int) syscall.Errno

	// CopyFileRange copies part of a file to another one.
	CopyFileRange(ctx *Context, fin Ino, offIn uint64, fout Ino, offOut uint64, size uint64, flags uint32, copied *uint64) syscall.Errno

	// GetXattr returns the value of extended attribute for given name.
	GetXattr(ctx *Context, inode Ino, attribute string, vbuff *[]byte) syscall.Errno
	// ListXattr returns all extended attributes of a node.
	ListXattr(ctx *Context, inode Ino, dbuff *[]string) syscall.Errno
	// SetXattr update the extended attribute of a node.
	SetXattr(ctx *Context, inode Ino, name string, value []byte, flags uint32) syscall.Errno
	// RemoveXattr removes the extended attribute of a node.
	RemoveXattr(ctx *Context, inode Ino, name string) syscall.Errno
	// Flock tries to put a lock on given file.
	Flock(ctx *Context, inode Ino, owner uint64, ltype uint32, block bool) syscall.Errno
	// Getlk returns the current lock owner for a range on a file.
	Getlk(ctx *Context, inode Ino, owner uint64, ltype *uint32, start, end *uint64, pid *uint32) syscall.Errno
	// Setlk sets a file range lock on given file.
	Setlk(ctx *Context, inode Ino, owner uint64, block bool, ltype uint32, start, end uint64, pid uint32) syscall.Errno

	LinksMetaUpdateHandler(stopChan chan struct{}, interval int, linkMetaDirPrefix string) error
}

func (a *Attr) IsDir() bool {
	return utils.StatModeToFileMode(int(a.Mode)).IsDir()
}

func NewMeta(fsMeta common.FSMeta, links map[string]common.FSMeta, config *Config) (Meta, error) {
	if config == nil {
		config = &Config{
			AttrCacheExpire:  2,
			EntryCacheExpire: 2,
			Config: kv_new.Config{
				Driver: kv_new.MemType,
				FsID:   fsMeta.ID,
			},
		}
	}
	return newKvMeta(fsMeta, links, *config)
}
