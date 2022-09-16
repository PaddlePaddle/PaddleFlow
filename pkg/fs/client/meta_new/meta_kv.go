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

package meta_new

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	pathlib "path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	apicommon "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv_new"
	ufslib "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs_new"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

const (
	// inodeSize struct size
	inodeSize = 109
	// entrySize struct size
	entrySize = 21
	entryDone = 1

	rootInodeID = Ino(1)

	MemDriver = "mem"
	// DiskDriver   = "disk"
	nextInodeKey              = "nextInode"
	DefaultLinkUpdateInterval = 15
	DefaultRootPath           = "/"
)

var _ Meta = &kvMeta{}

type freeID struct {
	next  uint64
	maxid uint64
}

// kvMeta
type kvMeta struct {
	client       kv_new.KvClient
	attrTimeOut  time.Duration
	entryTimeOut time.Duration
	defaultUfs   ufslib.UnderFileStorage
	ufsMap       *sync.Map
	ufsMapLock   sync.RWMutex
	ufsMapUT     int64
	setOwner     bool
	uid          uint32
	gid          uint32

	freeMu     sync.Mutex
	freeInodes freeID
}

type entryItem struct {
	ino    Ino
	mode   uint32
	expire int64
	// if done is true, means the parent entry for readdir cache is effective.
	done uint8
}

type inodeItem struct {
	attr        Attr
	parentIno   Ino
	expire      int64
	fileHandles int32
	name        []byte
}

func newKvMeta(fsMeta common.FSMeta, links map[string]common.FSMeta, config Config) (Meta, error) {
	client, err := newClient(config.Config)
	if err != nil {
		return nil, err
	}
	m := &kvMeta{
		client:       client,
		attrTimeOut:  config.AttrCacheExpire,
		entryTimeOut: config.EntryCacheExpire,
	}
	ufs, err := newUFS(fsMeta)
	if err != nil {
		return nil, err
	}
	m.defaultUfs = ufs
	err = m.UpdateUFSMap(links)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *kvMeta) UpdateUFSMap(fsMetas map[string]common.FSMeta) error {
	var ufsMap sync.Map
	for key, value := range fsMetas {
		linkUfs, err := newUFS(value)
		if err != nil {
			log.Errorf("new ufs for fsMeta[%+v] failed: %v", value, err)
			return err
		}
		ufsMap.Store(pathlib.Clean(key), linkUfs)
	}
	m.ufsMapLock.Lock()
	m.ufsMap = &ufsMap
	m.ufsMapLock.Unlock()
	return nil
}

func newUFS(fsMeta common.FSMeta) (ufslib.UnderFileStorage, error) {
	log.Debugf("begin to new UFS: fsMeta[%+v]", fsMeta)
	properties := make(map[string]interface{})
	for k, v := range fsMeta.Properties {
		properties[k] = v
	}
	properties[common.SubPath] = fsMeta.SubPath
	properties[common.Type] = fsMeta.UfsType

	switch fsMeta.UfsType {
	case common.HDFSType:
		properties[common.NameNodeAddress] = fsMeta.ServerAddress
	case common.HDFSWithKerberosType:
		properties[common.NameNodeAddress] = fsMeta.ServerAddress
	case common.SFTPType, common.CFSType, common.GlusterFSType:
		properties[common.Address] = fsMeta.ServerAddress
	}
	return ufslib.NewUFS(fsMeta.UfsType, properties)
}

func newClient(config kv_new.Config) (kv_new.KvClient, error) {
	var client kv_new.KvClient
	var err error
	switch config.Driver {
	case kv_new.DiskType, kv_new.MemType:
		client, err = kv_new.NewBadgerClient(config)
	default:
		return nil, fmt.Errorf("unknown meta client")
	}
	return client, err
}

func (m *kvMeta) fmtKey(args ...interface{}) []byte {
	b := utils.NewBuffer(uint32(m.keyLen(args...)))
	for _, a := range args {
		switch a := a.(type) {
		case byte:
			b.Put8(a)
		case uint32:
			b.Put32(a)
		case uint64:
			b.Put64(a)
		case Ino:
			m.encodeInode(a, b.Get(8))
		case string:
			b.Put([]byte(a))
		default:
			log.Panicf("invalid type %T, value %v", a, a)
		}
	}
	return b.Bytes()
}

func (m *kvMeta) keyLen(args ...interface{}) int {
	var c int
	for _, a := range args {
		switch a := a.(type) {
		case byte:
			c++
		case uint32:
			c += 4
		case uint64:
			c += 8
		case Ino:
			c += 8
		case string:
			c += len(a)
		default:
			log.Panicf("invalid type %T, value %v", a, a)
		}
	}
	return c
}

func (m *kvMeta) encodeInode(ino Ino, buf []byte) {
	binary.LittleEndian.PutUint64(buf, uint64(ino))
}

func (m *kvMeta) counterKey(key string) []byte {
	return m.fmtKey("C", key)
}

func (m *kvMeta) inodeKey(ino Ino) []byte {
	return m.fmtKey("I", ino)
}

func (m *kvMeta) entryKey(parent Ino, name string) []byte {
	return m.fmtKey("E", parent, "N", name)
}

func (m *kvMeta) get(key []byte) ([]byte, error) {
	var value []byte
	err := m.client.Txn(func(tx kv_new.KvTxn) error {
		value = tx.Get(key)
		return nil
	})
	return value, err
}

func (m *kvMeta) set(key []byte, value []byte) error {
	err := m.client.Txn(func(tx kv_new.KvTxn) error {
		errTx := tx.Set(key, value)
		return errTx
	})
	return err
}

func (m *kvMeta) scanValues(prefix []byte) (map[string][]byte, error) {
	var values map[string][]byte
	err := m.client.Txn(func(tx kv_new.KvTxn) error {
		values, _ = tx.ScanValues(prefix)
		return nil
	})
	return values, err
}

func (m *kvMeta) nextInode() (Ino, error) {
	m.freeMu.Lock()
	defer m.freeMu.Unlock()
	if m.freeInodes.next >= m.freeInodes.maxid {
		var v int64
		err := m.txn(func(tx kv_new.KvTxn) error {
			v = tx.IncrBy(m.counterKey(nextInodeKey), 100)
			return nil
		})
		if err != nil {
			return 0, err
		}
		m.freeInodes.next = uint64(v) - 100
		m.freeInodes.maxid = uint64(v)
	}
	n := m.freeInodes.next
	m.freeInodes.next++
	return Ino(n + 2), nil
}

func (m *kvMeta) parseInode(buf []byte, inode *inodeItem) {
	if inode == nil {
		return
	}
	rb := utils.FromBuffer(buf)
	inode.attr.Type = rb.Get8()
	inode.attr.Mode = rb.Get32()
	inode.attr.Uid = rb.Get32()
	inode.attr.Gid = rb.Get32()
	inode.attr.Rdev = rb.Get64()
	inode.attr.Atime = int64(rb.Get64())
	inode.attr.Mtime = int64(rb.Get64())
	inode.attr.Ctime = int64(rb.Get64())
	inode.attr.Atimensec = rb.Get32()
	inode.attr.Mtimensec = rb.Get32()
	inode.attr.Ctimensec = rb.Get32()
	inode.attr.Nlink = rb.Get64()
	inode.attr.Size = rb.Get64()
	inode.attr.Blksize = int64(rb.Get64())
	inode.attr.Block = int64(rb.Get64())
	inode.parentIno = Ino(rb.Get64())
	inode.expire = int64(rb.Get64())
	inode.fileHandles = int32(rb.Get32())
	inode.name = rb.Get(rb.Left())
}

func (m *kvMeta) marshalInode(inode *inodeItem) []byte {
	w := utils.NewBuffer(inodeSize + uint32(len(inode.name)))
	w.Put8(inode.attr.Type)
	w.Put32(inode.attr.Mode)
	w.Put32(inode.attr.Uid)
	w.Put32(inode.attr.Gid)
	w.Put64(inode.attr.Rdev)
	w.Put64(uint64(inode.attr.Atime))
	w.Put64(uint64(inode.attr.Mtime))
	w.Put64(uint64(inode.attr.Ctime))
	w.Put32(inode.attr.Atimensec)
	w.Put32(inode.attr.Mtimensec)
	w.Put32(inode.attr.Ctimensec)
	w.Put64(inode.attr.Nlink)
	w.Put64(inode.attr.Size)
	w.Put64(uint64(inode.attr.Blksize))
	w.Put64(uint64(inode.attr.Block))
	w.Put64(uint64(inode.parentIno))
	w.Put64(uint64(inode.expire))
	w.Put32(uint32(inode.fileHandles))
	w.Put(inode.name)
	return w.Bytes()
}

func (m *kvMeta) parseEntry(buf []byte, entry *entryItem) {
	if entry == nil {
		return
	}
	rb := utils.FromBuffer(buf)
	entry.ino = Ino(rb.Get64())
	entry.mode = rb.Get32()
	entry.expire = int64(rb.Get64())
	entry.done = rb.Get8()
}

func (m *kvMeta) marshalEntry(entry *entryItem) []byte {
	w := utils.NewBuffer(entrySize)
	w.Put64(uint64(entry.ino))
	w.Put32(entry.mode)
	w.Put64(uint64(entry.expire))
	w.Put8(entry.done)
	return w.Bytes()
}

func (m *kvMeta) fullPath(inode Ino) string {
	var builder strings.Builder
	var segments []string
	if inode == rootInodeID {
		return "/"
	}
	for {
		if inode == rootInodeID {
			break
		}
		d, err := m.get(m.inodeKey(inode))
		if err != nil {
			panic(err)
		}
		if d == nil {
			panic(fmt.Sprintf("full path parse fail with inode %v", inode))
		}
		attr := &inodeItem{}
		m.parseInode(d, attr)
		inode = attr.parentIno
		segments = append(segments, string(attr.name))
	}
	for i := len(segments) - 1; i >= 0; i-- {
		builder.WriteString("/")
		builder.WriteString(segments[i])
	}
	return builder.String()
}

func (m *kvMeta) txn(f func(tx kv_new.KvTxn) error) error {
	var err error
	for i := 0; i < 50; i++ {
		if err = m.client.Txn(f); m.shouldRetry(err) {
			time.Sleep(time.Millisecond * time.Duration(i*i))
			continue
		}
		break
	}
	return err
}

func (m *kvMeta) shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(syscall.Errno); ok {
		return false
	}
	// TODO: add other retryable errors here
	return strings.Contains(err.Error(), "Transaction Conflict")
}

func (m *kvMeta) getAttrFromCacheWithNoExpired(ino Ino, inodeItem_ *inodeItem) bool {
	a, err := m.get(m.inodeKey(ino))
	if err != nil || a == nil {
		return false
	}
	m.parseInode(a, inodeItem_)
	return !m.inodeItemExpired(*inodeItem_)
}

func (m *kvMeta) getAttrFromCache(ino Ino, inodeItem_ *inodeItem) bool {
	a, err := m.get(m.inodeKey(ino))
	if err != nil || a == nil {
		return false
	}
	return true
}

func (m *kvMeta) inodeItemExpired(item inodeItem) bool {
	if item.expire < time.Now().Unix() && item.fileHandles == 0 {
		return true
	}
	return false
}

func (m *kvMeta) entryItemExpired(item *entryItem) bool {
	return time.Now().Unix() > item.expire
}

func (m *kvMeta) modifyTime(oldAttr, newAttr *Attr) {
	if oldAttr != nil {
		if oldAttr.Mtime > newAttr.Mtime {
			newAttr.Mtime = oldAttr.Mtime
			newAttr.Mtimensec = oldAttr.Mtimensec
		}
		if oldAttr.Ctime > newAttr.Ctime {
			newAttr.Ctime = oldAttr.Ctime
			newAttr.Ctimensec = oldAttr.Ctimensec
		}
		if oldAttr.Atime > newAttr.Atime {
			newAttr.Atime = oldAttr.Atime
			newAttr.Atimensec = oldAttr.Atimensec
		}
	}
}

func (m *kvMeta) GetUFS(name string) (ufslib.UnderFileStorage, bool, string, string) {
	var findUfs ufslib.UnderFileStorage
	findKey := ""
	targetPath := pathlib.Clean(pathlib.Join("/", name))
	newPath := name
	// todo：改为并发安全的container/list实现
	m.ufsMapLock.RLock()
	ufsMap := m.ufsMap
	m.ufsMapLock.RUnlock()
	if ufsMap != nil {
		ufsMap.Range(func(key, value interface{}) bool {
			prefix := key.(string)
			// "/"要加到Clean外面，这样确保匹配的前缀是目录项
			keyPath := pathlib.Clean(pathlib.Join("/", prefix)) + "/"

			if strings.HasPrefix(targetPath, keyPath) || targetPath+"/" == keyPath {
				findUfs = value.(ufslib.UnderFileStorage)
				findKey = keyPath
				if targetPath+"/" == keyPath {
					newPath = ""
				} else {
					newPath = strings.TrimPrefix(targetPath, keyPath)
				}
				return false
			}
			return true
		})
	}
	isLink := true
	if findUfs == nil {
		isLink = false
		findUfs = m.defaultUfs
		findKey = ""
	}

	log.Debugf("source path[%s], findKey[%s], isLink[%t], path[%s]", name, findKey, isLink, newPath)
	return findUfs, isLink, findKey, newPath
}

func (m *kvMeta) Name() string {
	return "meta_kv"
}

func (m *kvMeta) InitRootInode() error {
	attr := &Attr{}
	now := time.Now()
	attr.Type = TypeDirectory
	attr.Mode = syscall.S_IFDIR | 0777
	attr.Nlink = 2
	attr.Size = 4 << 10
	attr.Mtime = now.Unix()
	attr.Mtimensec = uint32(now.Nanosecond())
	attr.Ctime = now.Unix()
	attr.Ctimensec = uint32(now.Nanosecond())
	inodeItem_ := &inodeItem{
		parentIno: 0,
		name:      []byte("/"),
		expire:    now.Add(1000 * time.Hour).Unix(), // root inode not expire
		attr:      *attr,
	}
	err := m.set(m.inodeKey(rootInodeID), m.marshalInode(inodeItem_))
	return err
}

func (m *kvMeta) SetOwner(uid, gid uint32) {
}

func (m *kvMeta) StatFS(ctx *Context) (*base.StatfsOut, syscall.Errno) {
	log.Debugf("defaultMeta, StatFs: name[%s]", DefaultRootPath)
	ufs := m.defaultUfs
	return ufs.StatFs(DefaultRootPath), syscall.F_OK
}

func (m *kvMeta) Access(ctx *Context, inode Ino, mask uint32, attr *Attr) syscall.Errno {
	if ctx.Uid == 0 {
		return 0
	}

	if attr == nil {
		attr = &Attr{}
		err := m.GetAttr(ctx, inode, attr)
		if utils.IsError(err) {
			return err
		}
	}

	uid := attr.Uid
	gid := attr.Gid
	if m.setOwner {
		uid = m.uid
		gid = m.gid
	}
	if !utils.HasAccess(ctx.Uid, ctx.Gid, uid, gid, attr.Mode, mask) {
		return syscall.EACCES
	}
	return syscall.F_OK
}

func (m *kvMeta) Lookup(ctx *Context, parent Ino, name string) (Ino, *Attr, syscall.Errno) {
	log.Debugf("kv meta  parent Ino[%v] name [%s]", parent, name)
	// todo:: add "." and ".."
	entry, err := m.get(m.entryKey(parent, name))
	if err != nil {
		return 0, nil, syscall.EIO
	}
	var inode Ino
	attr := &Attr{}
	inodeItem_ := &inodeItem{}
	if entry != nil {
		entryItem_ := &entryItem{}
		m.parseEntry(entry, entryItem_)
		inode = entryItem_.ino
		ok := m.getAttrFromCacheWithNoExpired(inode, inodeItem_)
		log.Debugf("kv meta look up cache inode[%v] item[%+v]", inode, inodeItem_)
		if ok {
			*attr = inodeItem_.attr
			return inode, attr, syscall.F_OK
		}
	}

	absolutePath := filepath.Join(m.fullPath(parent), name)
	ufs, isLink, prefix, path := m.GetUFS(absolutePath)

	err = m.txn(func(tx kv_new.KvTxn) error {
		info, err := ufs.GetAttr(path)
		now := time.Now()
		if err != nil {
			log.Debugf("[vfs-lookup] GetAttr failed: %v with path[%s] name[%s] and absolutePath[%s]", err, path, name, absolutePath)
			return err
		}
		if isLink {
			info.FixLinkPrefix(prefix)
		}
		attr.FromFileInfo(info)
		if entry == nil {
			number, err := m.nextInode()
			if err != nil {
				return err
			}
			inode = number
			entryItem_ := &entryItem{
				ino:  inode,
				mode: attr.Mode,
			}
			err = tx.Set(m.entryKey(parent, name), m.marshalEntry(entryItem_))
			if err != nil {
				return err
			}
		}
		inodeItem_.expire = now.Add(m.attrTimeOut).Unix()
		inodeItem_.parentIno = parent
		inodeItem_.name = []byte(name)
		m.modifyTime(&(inodeItem_.attr), attr)
		inodeItem_.attr = *attr
		log.Debugf("lookup set inode[%v] item[%v]", inode, inodeItem_)

		err = tx.Set(m.inodeKey(inode), m.marshalInode(inodeItem_))
		return err
	})
	return inode, attr, utils.ToSyscallErrno(err)
}

func (m *kvMeta) Resolve(ctx *Context, parent Ino, path string, inode *Ino, attr *Attr) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) GetAttr(ctx *Context, inode Ino, attr *Attr) syscall.Errno {
	log.Debugf("kv GetAttr inode[%v]", inode)
	inodeItem_ := &inodeItem{}
	if inode == rootInodeID {
		now := time.Now()
		attr.Type = TypeDirectory
		attr.Mode = syscall.S_IFDIR | 0777
		attr.Nlink = 2
		attr.Size = 4 << 10
		attr.Mtime = now.Unix()
		attr.Mtimensec = uint32(now.Nanosecond())
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())
		return syscall.F_OK
	}
	has := m.getAttrFromCacheWithNoExpired(inode, inodeItem_)
	if has {
		*attr = inodeItem_.attr
		return syscall.F_OK
	}
	absolutePath := m.fullPath(inode)
	ufs, isLink, prefix, path := m.GetUFS(absolutePath)
	err := m.txn(func(tx kv_new.KvTxn) error {
		now := time.Now()
		info, err := ufs.GetAttr(path)
		if err != nil {
			log.Debugf("[vfs] GetAttr failed: %v with path[%s] and absolutePath[%s]", err, path, absolutePath)
			if utils.IfNotExist(err) {
				_ = tx.Dels(m.inodeKey(inode), m.entryKey(inodeItem_.parentIno, string(inodeItem_.name)))
			}
			return err
		}
		log.Debugf("before fix: the attr mode is [%d]", attr.Mode)
		if isLink {
			info.FixLinkPrefix(prefix)
		}
		attr.FromFileInfo(info)
		log.Debugf("set attr get is inode %v attr %+v", inode, attr)
		m.modifyTime(&(inodeItem_.attr), attr)
		inodeItem_.attr = *attr
		inodeItem_.expire = now.Add(m.attrTimeOut).Unix()
		_ = tx.Set(m.inodeKey(inode), m.marshalInode(inodeItem_))
		return nil
	})
	return utils.ToSyscallErrno(err)
}

func (m *kvMeta) SetAttr(ctx *Context, inode Ino, set uint32, attr *Attr) (string, syscall.Errno) {
	var absolutePath string
	err := m.txn(func(tx kv_new.KvTxn) error {
		var cur inodeItem
		now := time.Now()
		a := tx.Get(m.inodeKey(inode))
		if a == nil {
			return syscall.ENOENT
		}
		m.parseInode(a, &cur)
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())
		cur.attr = *attr
		cur.expire = now.Add(m.attrTimeOut).Unix()
		err := tx.Set(m.inodeKey(inode), m.marshalInode(&cur))
		if err != nil {
			return err
		}
		absolutePath = m.fullPath(inode)
		ufs_, isLink, prefix, path := m.GetUFS(absolutePath)
		ufsAttr, err := ufs_.GetAttr(path)
		if err != nil {
			return err
		}
		attr.FromFileInfo(ufsAttr)
		if isLink {
			ufsAttr.FixLinkPrefix(prefix)
		}
		if set&FATTR_UID != 0 || set&FATTR_GID != 0 {
			if err = ufs_.Chown(path, attr.Uid, attr.Gid); err != nil {
				return err
			}
		}

		if set&FATTR_MODE != 0 {
			if err = ufs_.Chmod(path, attr.Mode); err != nil {
				return err
			}
		}
		// s3未实现utimes函数，创建文件时存在报错：setting times of ‘xx’: Function not implemented。因此这里忽略enosys报错
		if set&FATTR_ATIME != 0 || set&FATTR_MTIME != 0 || set&FATTR_CTIME != 0 {
			atime := time.Unix(attr.Atime, int64(attr.Atimensec))
			ctime := time.Unix(attr.Ctime, int64(attr.Ctimensec))
			if err = ufs_.Utimens(path, &atime, &ctime); err != nil {
				return utils.ToSyscallErrno(err)
			}
		}
		return nil
	})
	if err != nil {
		return "", utils.ToSyscallErrno(err)
	}
	return absolutePath, syscall.F_OK
}

func (m *kvMeta) Truncate(ctx *Context, inode Ino, size uint64) syscall.Errno {
	return syscall.F_OK
}

func (m *kvMeta) Fallocate(ctx *Context, inode Ino, mode uint8, off uint64, size uint64) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) ReadLink(ctx *Context, inode Ino, path *[]byte) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) Symlink(ctx *Context, parent Ino, name string, path string, inode *Ino, attr *Attr) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) Mknod(ctx *Context, parent Ino, name string, _type uint8, mode, cumask uint32, rdev uint32, inode *Ino, attr *Attr) syscall.Errno {
	ino, err := m.nextInode()
	*inode = ino
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	insertInodeItem_ := &inodeItem{}
	if attr == nil {
		attr = &Attr{}
	}
	now := time.Now()
	attr.Type = _type
	attr.Uid = ctx.Uid
	attr.Gid = ctx.Gid
	// todo:: file mode including type and unix permission, add smode to transe
	attr.Mode = mode & ^uint32(cumask)
	attr.Nlink = 1
	attr.Size = 4 << 10
	insertInodeItem_.attr = *attr
	insertInodeItem_.parentIno = parent
	insertInodeItem_.fileHandles = 1
	insertInodeItem_.name = []byte(name)
	insertInodeItem_.expire = now.Add(m.attrTimeOut).Unix()

	absolutePath := filepath.Join(m.fullPath(parent), name)
	ufs, _, _, newPath := m.GetUFS(absolutePath)
	if err := ufs.Mknod(newPath, mode, rdev); err != nil {
		return utils.ToSyscallErrno(err)
	}
	// todo:: adjust s3 mode
	err = m.txn(func(tx kv_new.KvTxn) error {
		a := tx.Get(m.inodeKey(parent))
		if a == nil {
			return syscall.ENOENT
		}
		var pInodeItem inodeItem
		m.parseInode(a, &pInodeItem)
		if pInodeItem.attr.Type != TypeDirectory {
			return syscall.ENOTDIR
		}
		buf := tx.Get(m.entryKey(parent, name))
		if buf != nil {
			return syscall.EEXIST
		}
		now = time.Now()
		pInodeItem.attr.Mtime = now.Unix()
		pInodeItem.attr.Mtimensec = uint32(now.Nanosecond())
		pInodeItem.attr.Ctime = now.Unix()
		pInodeItem.attr.Ctimensec = uint32(now.Nanosecond())
		pInodeItem.attr.Nlink++
		attr.Atime = now.Unix()
		attr.Atimensec = uint32(now.Nanosecond())
		attr.Mtime = now.Unix()
		attr.Mtimensec = uint32(now.Nanosecond())
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())
		insertEntryItem_ := &entryItem{
			ino:  Ino(ino),
			mode: mode,
		}
		err = tx.Set(m.entryKey(parent, name), m.marshalEntry(insertEntryItem_))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(parent), m.marshalInode(&pInodeItem))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(Ino(ino)), m.marshalInode(insertInodeItem_))
		if err != nil {
			return err
		}

		return nil
	})
	return utils.ToSyscallErrno(err)
}

func (m *kvMeta) Mkdir(ctx *Context, parent Ino, name string, mode uint32, cumask uint16, inode *Ino, attr *Attr) syscall.Errno {
	ino, err := m.nextInode()
	log.Debugf("kv meta mkdir parent_ino[%v] inode[%v] name[%s]", parent, ino, name)
	*inode = Ino(ino)
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	insertInodeItem_ := &inodeItem{}
	if attr == nil {
		attr = &Attr{}
	}
	now := time.Now()
	attr.Type = TypeDirectory
	attr.Uid = ctx.Uid
	attr.Gid = ctx.Gid
	// todo:: file mode including type and unix permission, add smode to transe
	attr.Mode = syscall.S_IFDIR | mode & ^uint32(cumask)
	attr.Nlink = 1
	attr.Size = 4 << 10
	insertInodeItem_.parentIno = parent
	insertInodeItem_.fileHandles = 1
	insertInodeItem_.name = []byte(name)
	insertInodeItem_.expire = now.Add(m.attrTimeOut).Unix()

	absolutePath := filepath.Join(m.fullPath(parent), name)
	ufs, _, _, newPath := m.GetUFS(absolutePath)
	if err := ufs.Mkdir(newPath, mode); err != nil {
		return utils.ToSyscallErrno(err)
	}
	// todo:: adjust s3 mode
	err = m.txn(func(tx kv_new.KvTxn) error {
		a := tx.Get(m.inodeKey(parent))
		if a == nil {
			return syscall.ENOENT
		}
		var pInodeItem inodeItem
		m.parseInode(a, &pInodeItem)
		if pInodeItem.attr.Type != TypeDirectory {
			return syscall.ENOTDIR
		}
		buf := tx.Get(m.entryKey(parent, name))
		if buf != nil {
			return syscall.EEXIST
		}
		now = time.Now()
		pInodeItem.attr.Mtime = now.Unix()
		pInodeItem.attr.Mtimensec = uint32(now.Nanosecond())
		pInodeItem.attr.Ctime = now.Unix()
		pInodeItem.attr.Ctimensec = uint32(now.Nanosecond())
		pInodeItem.attr.Nlink++
		attr.Atime = now.Unix()
		attr.Atimensec = uint32(now.Nanosecond())
		attr.Mtime = now.Unix()
		attr.Mtimensec = uint32(now.Nanosecond())
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())
		insertInodeItem_.attr = *attr
		insertEntryItem_ := &entryItem{
			ino:  Ino(ino),
			mode: mode,
		}
		err = tx.Set(m.entryKey(parent, name), m.marshalEntry(insertEntryItem_))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(parent), m.marshalInode(&pInodeItem))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(Ino(ino)), m.marshalInode(insertInodeItem_))
		if err != nil {
			return err
		}

		return nil
	})
	return utils.ToSyscallErrno(err)
}

func (m *kvMeta) Unlink(ctx *Context, parent Ino, name string) syscall.Errno {
	log.Debugf("kv meta Unlink parent[%v] name[%s]", parent, name)
	err := m.txn(func(tx kv_new.KvTxn) error {
		entry, err := m.get(m.entryKey(parent, name))
		if err != nil {
			return syscall.ENOENT
		}
		entryItem_ := &entryItem{}
		m.parseEntry(entry, entryItem_)
		pinodebyte, err := m.get(m.inodeKey(parent))
		if pinodebyte == nil || err != nil {
			log.Debugf("[vfs-unlink] failed. file %v's pnode %v is not exist. \n", name, parent)
			return syscall.ENOENT
		}
		pinodeItem := &inodeItem{}
		m.parseInode(pinodebyte, pinodeItem)
		now := time.Now()
		pinodeItem.attr.Mtime = now.Unix()
		pinodeItem.attr.Mtimensec = uint32(now.Nanosecond())
		pinodeItem.attr.Ctime = now.Unix()
		pinodeItem.attr.Ctimensec = uint32(now.Nanosecond())
		if err = tx.Set(m.inodeKey(parent), m.marshalInode(pinodeItem)); err != nil {
			return err
		}
		if err = tx.Dels(m.entryKey(parent, name), m.inodeKey(entryItem_.ino)); err != nil {
			return err
		}
		absolutePath := filepath.Join(m.fullPath(parent), name)
		ufs, _, _, path := m.GetUFS(absolutePath)
		if err = ufs.Unlink(path); err != nil {
			return utils.ToSyscallErrno(err)
		}
		return nil
	})
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK

}

func (m *kvMeta) Rmdir(ctx *Context, parent Ino, name string) syscall.Errno {
	log.Debugf("kv meta Rmdir parent[%v] name[%s]", parent, name)
	err := m.txn(func(tx kv_new.KvTxn) error {
		buf := tx.Get(m.entryKey(parent, name))
		if buf == nil {
			return syscall.ENOENT
		}
		inodeEntry := &entryItem{}
		m.parseEntry(buf, inodeEntry)
		inodeItem_ := &inodeItem{}
		ino := inodeEntry.ino
		a := tx.Get(m.inodeKey(ino))
		if a != nil {
			m.parseInode(a, inodeItem_)
			if !m.inodeItemExpired(*inodeItem_) {
				if inodeItem_.attr.Type != TypeDirectory {
					return syscall.ENOTDIR
				}
			}
		}
		a = tx.Get(m.inodeKey(parent))
		if a == nil {
			return syscall.ENOENT
		}
		parentItem_ := &inodeItem{}
		m.parseInode(a, parentItem_)
		if !m.inodeItemExpired(*parentItem_) {
			if parentItem_.attr.Type != TypeDirectory {
				return syscall.ENOTDIR
			}
		}
		if tx.Exist(m.entryKey(ino, "")) {
			return syscall.ENOTEMPTY
		}
		absolutePath := m.fullPath(ino)
		ufs, _, _, path := m.GetUFS(absolutePath)
		now := time.Now()
		parentItem_.attr.Mtime = now.Unix()
		parentItem_.attr.Mtimensec = uint32(now.Nanosecond())
		parentItem_.attr.Ctime = now.Unix()
		parentItem_.attr.Ctimensec = uint32(now.Nanosecond())
		err := tx.Dels(m.entryKey(parent, name), m.inodeKey(ino))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(parent), m.marshalInode(parentItem_))
		if err != nil {
			return err
		}
		if err = ufs.Rmdir(path); err != nil {
			return err
		}
		return nil
	})
	return utils.ToSyscallErrno(err)
}

func (m *kvMeta) Rename(ctx *Context, parentSrc Ino, nameSrc string, parentDst Ino, nameDst string, flags uint32, inode *Ino, attr *Attr) (string, string, syscall.Errno) {
	var pathDst string
	var pathSrc string
	err := m.txn(func(tx kv_new.KvTxn) error {
		buf := tx.Get(m.entryKey(parentSrc, nameSrc))
		if buf == nil {
			return syscall.ENOENT
		}
		srcEntryItem_ := &entryItem{}
		m.parseEntry(buf, srcEntryItem_)

		now := time.Now()
		pSrcAttr := &inodeItem{}
		pDstAttr := &inodeItem{}
		srcAttr := &inodeItem{}
		has := m.getAttrFromCache(parentSrc, pSrcAttr) && m.getAttrFromCache(parentDst, pDstAttr) && m.getAttrFromCache(srcEntryItem_.ino, srcAttr)
		if !has {
			return syscall.ENOENT
		}
		if parentSrc == parentDst && nameSrc == nameDst {
			if inode != nil {
				*inode = srcEntryItem_.ino
			}
			if attr != nil {
				*attr = srcAttr.attr
			}
			return nil
		}
		pSrcAttr.attr.Mtime = now.Unix()
		pSrcAttr.attr.Mtimensec = uint32(now.Nanosecond())
		pSrcAttr.attr.Ctime = now.Unix()
		pSrcAttr.attr.Ctimensec = uint32(now.Nanosecond())

		if parentSrc != parentDst {

			pDstAttr.attr.Mtime = now.Unix()
			pDstAttr.attr.Mtimensec = uint32(now.Nanosecond())
			pDstAttr.attr.Ctime = now.Unix()
			pDstAttr.attr.Ctimensec = uint32(now.Nanosecond())

		}
		srcAttr.parentIno = parentDst
		// 更新srcPath
		srcAttr.attr.Ctime = now.Unix()
		srcAttr.attr.Ctimensec = uint32(now.Nanosecond())
		if inode != nil {
			*inode = srcEntryItem_.ino
		}
		if attr != nil {
			*attr = srcAttr.attr
		}
		log.Debugf("rename set new inode %v parentDst %v nameDst %s", *inode, parentDst, nameDst)
		_ = tx.Set(m.entryKey(parentDst, nameDst), m.marshalEntry(srcEntryItem_))
		_ = tx.Set(m.inodeKey(*inode), m.marshalInode(srcAttr))
		_ = tx.Dels(m.entryKey(parentSrc, nameSrc))

		// ufs rename
		pathDst = filepath.Join(m.fullPath(parentDst), nameDst)
		pathSrc = filepath.Join(m.fullPath(parentSrc), nameSrc)
		ufsSrc, _, _, pathOld := m.GetUFS(pathSrc)
		ufsDst, _, _, pathNew := m.GetUFS(pathDst)
		if ufsDst != ufsSrc {
			log.Errorf("Rename between two ufs is not supported")
			return syscall.ENOSYS
		}
		err := ufsSrc.Rename(pathOld, pathNew)
		return err
	})

	if err != nil {
		return "", "", utils.ToSyscallErrno(err)
	}

	return pathSrc, pathDst, syscall.F_OK
}

func (m *kvMeta) Link(ctx *Context, inodeSrc, parent Ino, name string, attr *Attr) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) Readdir(ctx *Context, inode Ino, entries *[]*Entry) syscall.Errno {
	log.Debugf("kv meta readdir inode[%v]", inode)
	*entries = []*Entry{}
	dirInodeItem := &inodeItem{}
	buf, err := m.get(m.inodeKey(inode))
	if err != nil {
		return syscall.EIO
	}
	if buf == nil {
		return syscall.ENOENT
	}
	m.parseInode(buf, dirInodeItem)
	parentIno := dirInodeItem.parentIno

	entry, err := m.get(m.entryKey(parentIno, string(dirInodeItem.name)))
	if err != nil {
		return utils.ToSyscallErrno(err)
	}

	if entry != nil {
		dirEntryItem := &entryItem{}
		m.parseEntry(entry, dirEntryItem)
		if dirEntryItem.done == entryDone && !m.entryItemExpired(dirEntryItem) {
			ens, err := m.scanValues(m.entryKey(inode, ""))
			if err != nil {
				return utils.ToSyscallErrno(err)
			}
			prefix := len(m.entryKey(inode, ""))
			var childEntryItem *entryItem
			for name, childEntryBuf := range ens {
				childEntryItem = &entryItem{}
				m.parseEntry(childEntryBuf, childEntryItem)
				en := &Entry{
					Ino:  childEntryItem.ino,
					Name: name[prefix:],
					Attr: &Attr{Mode: childEntryItem.mode},
				}
				*entries = append(*entries, en)
			}
			return syscall.F_OK
		}
	}
	absolutePath := m.fullPath(inode)
	ufs, isLink, _, path := m.GetUFS(absolutePath)

	err = m.txn(func(tx kv_new.KvTxn) error {
		dirs, err := ufs.ReadDir(path)
		if err != nil {
			return utils.ToSyscallErrno(err)
		}
		now := time.Now()

		// 不存在link嵌套，因此只有非link情况下才需要填充link文件
		if !isLink {
			m.ufsMap.Range(func(key, value interface{}) bool {
				dir := pathlib.Dir(key.(string))
				linkUfs := value.(ufslib.UnderFileStorage)
				if pathlib.Clean("/"+absolutePath) == pathlib.Clean("/"+dir) {
					entry := ufslib.DirEntry{
						Name: pathlib.Base(key.(string)),
					}
					attr, err := linkUfs.GetAttr("")
					if err != nil {
						// TODO: 获取link文件报错，暂不往上抛出
						log.Errorf("getAttr : name[%s] failed: [%v]", key.(string), err)
					}
					entry.Attr.Mode = uint32(os.ModeSymlink | attr.Mode)
					// 如果原位置已有文件，默认展示Link文件覆盖原来的文件，unlink后可见
					exist := false
					for i, dirEntry := range dirs {
						log.Debugf("dirEntry.Name[%s] and key[%s]", dirEntry.Name, key.(string))
						if pathlib.Clean(dirEntry.Name) == pathlib.Clean(pathlib.Base(key.(string))) {
							exist = true
							dirs[i] = entry
							break
						}
					}
					if !exist {
						dirs = append(dirs, entry)
					}
				}
				return true
			})
		}
		log.Debugf("dirs is [%+v]", dirs)

		var childEntryItem *Entry
		var expire int64
		var childEntryBuf []byte
		var insertChildInode *inodeItem
		for _, dir := range dirs {
			childEntryItem = &Entry{
				Name: dir.Name,
				Attr: &Attr{
					Type:      dir.Attr.Type,
					Mode:      dir.Attr.Mode,
					Uid:       dir.Attr.Uid,
					Gid:       dir.Attr.Gid,
					Rdev:      dir.Attr.Rdev,
					Atime:     dir.Attr.Atime,
					Mtime:     dir.Attr.Mtime,
					Ctime:     dir.Attr.Ctime,
					Atimensec: dir.Attr.Atimensec,
					Mtimensec: dir.Attr.Mtimensec,
					Ctimensec: dir.Attr.Ctimensec,
					Nlink:     dir.Attr.Nlink,
					Size:      dir.Attr.Size,
					Blksize:   dir.Attr.Blksize,
					Block:     dir.Attr.Block,
				},
			}
			var newInode Ino

			childEntryBuf, err = m.get(m.entryKey(inode, dir.Name))
			if err != nil {
				return err
			}
			var childEntryItemFromCache *entryItem
			if childEntryBuf != nil {
				childEntryItemFromCache = &entryItem{}
				m.parseEntry(childEntryBuf, childEntryItemFromCache)
				newInode = childEntryItemFromCache.ino
			} else {
				newInodeNumber, err := m.nextInode()
				if err != nil {
					return err
				}
				newInode = Ino(newInodeNumber)
				insertChildEntry := &entryItem{
					ino:  newInode,
					mode: dir.Attr.Mode,
				}
				err = tx.Set(m.entryKey(inode, dir.Name), m.marshalEntry(insertChildEntry))
				if err != nil {
					return err
				}
			}
			expire = now.Add(m.attrTimeOut).Unix()
			insertChildInode = &inodeItem{
				attr:      *childEntryItem.Attr,
				parentIno: inode,
				expire:    expire,
				name:      []byte(dir.Name),
			}
			err = tx.Set(m.inodeKey(newInode), m.marshalInode(insertChildInode))
			if err != nil {
				return err
			}
			childEntryItem.Ino = newInode
			*entries = append(*entries, childEntryItem)
		}
		insertDirEntry := &entryItem{
			ino:    inode,
			mode:   dirInodeItem.attr.Mode,
			expire: now.Add(m.entryTimeOut).Unix(),
			done:   entryDone,
		}
		err = tx.Set(m.entryKey(parentIno, string(dirInodeItem.name)), m.marshalEntry(insertDirEntry))
		return err
	})
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

func (m *kvMeta) Create(ctx *Context, parent Ino, name string, mode uint32, cumask uint16, flags uint32, inode *Ino, attr *Attr) (ufslib.UnderFileStorage, string, syscall.Errno) {
	ino, err := m.nextInode()
	*inode = ino
	if err != nil {
		return nil, "", utils.ToSyscallErrno(err)
	}
	insertInodeItem_ := &inodeItem{}
	if attr == nil {
		attr = &Attr{}
	}
	now := time.Now()
	attr.Type = TypeFile
	attr.Mode = uint32(uint16(mode) & ^cumask)
	attr.Uid = ctx.Uid
	attr.Gid = ctx.Gid
	attr.Nlink = 1
	attr.Size = 0
	attr.Rdev = 0
	insertInodeItem_.parentIno = parent
	insertInodeItem_.fileHandles = 1
	insertInodeItem_.name = []byte(name)
	insertInodeItem_.expire = now.Add(m.attrTimeOut).Unix()

	absolutePath := filepath.Join(m.fullPath(parent), name)
	ufs, _, _, newPath := m.GetUFS(absolutePath)
	fh, err := ufs.Create(newPath, flags, mode)
	if err != nil {
		log.Errorf("Create: name[%s], flags[%d], mode[%d], failed: [%v]",
			name, flags, mode, err)
		return nil, newPath, utils.ToSyscallErrno(err)
	}
	defer fh.Release()

	err = m.txn(func(tx kv_new.KvTxn) error {
		a := tx.Get(m.inodeKey(parent))
		if a == nil {
			return syscall.ENOENT
		}
		var pInodeItem inodeItem
		m.parseInode(a, &pInodeItem)
		if pInodeItem.attr.Type != TypeDirectory {
			return syscall.ENOTDIR
		}
		buf := tx.Get(m.entryKey(parent, name))
		if buf != nil {
			return syscall.EEXIST
		}
		now = time.Now()
		pInodeItem.attr.Mtime = now.Unix()
		pInodeItem.attr.Mtimensec = uint32(now.Nanosecond())
		pInodeItem.attr.Ctime = now.Unix()
		pInodeItem.attr.Ctimensec = uint32(now.Nanosecond())
		attr.Atime = now.Unix()
		attr.Atimensec = uint32(now.Nanosecond())
		attr.Mtime = now.Unix()
		attr.Mtimensec = uint32(now.Nanosecond())
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())
		insertInodeItem_.attr = *attr
		insertEntryItem_ := &entryItem{
			ino:  Ino(ino),
			mode: mode,
		}
		err = tx.Set(m.entryKey(parent, name), m.marshalEntry(insertEntryItem_))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(parent), m.marshalInode(&pInodeItem))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(Ino(ino)), m.marshalInode(insertInodeItem_))
		if err != nil {
			return err
		}

		return nil
	})
	return ufs, newPath, utils.ToSyscallErrno(err)
}

func (m *kvMeta) Open(ctx *Context, inode Ino, flags uint32, attr *Attr) (ufslib.UnderFileStorage, string, syscall.Errno) {
	err := m.GetAttr(ctx, inode, attr)
	if err != 0 {
		return nil, "", err
	}
	absolutePath := m.fullPath(inode)
	ufs, _, _, newPath := m.GetUFS(absolutePath)
	return ufs, newPath, syscall.F_OK
}

func (m *kvMeta) Close(ctx *Context, inode Ino) syscall.Errno {
	err := m.txn(func(tx kv_new.KvTxn) error {
		a := tx.Get(m.inodeKey(inode))
		updateInodeItem := &inodeItem{}
		m.parseInode(a, updateInodeItem)
		if atomic.AddInt32(&updateInodeItem.fileHandles, -1) == -1 {
			panic(updateInodeItem.fileHandles)
		}
		return tx.Set(m.inodeKey(inode), m.marshalInode(updateInodeItem))
	})
	return utils.ToSyscallErrno(err)
}

func (m *kvMeta) Read(ctx *Context, inode Ino, indx uint32, buf []byte) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) Write(ctx *Context, inode Ino, off uint32, length int) syscall.Errno {
	updateInodeItem := &inodeItem{}
	if ok := m.getAttrFromCacheWithNoExpired(inode, updateInodeItem); ok {
		now := time.Now()
		newLength := uint64(int(off) + length)
		if newLength > updateInodeItem.attr.Size {
			updateInodeItem.attr.Size = newLength
		}
		updateInodeItem.attr.Ctime = now.Unix()
		updateInodeItem.attr.Ctimensec = uint32(now.Nanosecond())
		updateInodeItem.attr.Mtime = now.Unix()
		updateInodeItem.attr.Mtimensec = uint32(now.Nanosecond())
		err := m.txn(func(tx kv_new.KvTxn) error {
			return tx.Set(m.inodeKey(inode), m.marshalInode(updateInodeItem))
		})
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

func (m *kvMeta) CopyFileRange(ctx *Context, fin Ino, offIn uint64, fout Ino, offOut uint64, size uint64, flags uint32, copied *uint64) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) GetXattr(ctx *Context, inode Ino, attribute string, vbuff *[]byte) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) ListXattr(ctx *Context, inode Ino, dbuff *[]string) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) SetXattr(ctx *Context, inode Ino, name string, value []byte, flags uint32) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) RemoveXattr(ctx *Context, inode Ino, name string) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) Flock(ctx *Context, inode Ino, owner uint64, ltype uint32, block bool) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) Getlk(ctx *Context, inode Ino, owner uint64, ltype *uint32, start, end *uint64, pid *uint32) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) Setlk(ctx *Context, inode Ino, owner uint64, block bool, ltype uint32, start, end uint64, pid uint32) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) LinksMetaUpdateHandler(stopChan chan struct{}, interval int, linkMetaDirPrefix string) error {
	for {
		err := m.linksMetaUpdate(linkMetaDirPrefix)
		if err != nil {
			log.Debugf("links meta update failed, err[%v]", err)
		}
		select {
		case <-stopChan:
			log.Info("links meta update handler stopped")
			return nil
		default:
			time.Sleep(time.Duration(interval) * time.Second)
		}
	}
}

func (m *kvMeta) InoToPath(inode Ino) string {
	return m.fullPath(inode)
}

func (m *kvMeta) getAttr(name string, attr *Attr) syscall.Errno {
	ufs, isLink, prefix, path := m.GetUFS(name)
	info, err := ufs.GetAttr(path)
	if err != nil {
		log.Debugf("[vfs] GetAttr failed: %v with path[%s] and name[%s]", err, path, name)
		return utils.ToSyscallErrno(err)
	}
	log.Debugf("before fix: the attr mode is [%d]", attr.Mode)
	if isLink {
		info.FixLinkPrefix(prefix)
	}
	attr.FromFileInfo(info)
	return syscall.F_OK
}

func (m *kvMeta) linksMetaUpdate(linkMetaDirPrefix string) error {
	filePath := pathlib.Join(linkMetaDirPrefix, common.LinkMetaDir, common.LinkMetaFile)
	attr := &Attr{}
	errno := m.getAttr(filePath, attr)
	if utils.IsError(errno) {
		log.Debugf("GetAttr file[%s] failed: %v", filePath, errno)
		return errno
	}

	if attr.Mtime <= m.ufsMapUT {
		return nil
	}

	flags := uint32(syscall.O_RDONLY)
	fileHandle, err := m.defaultUfs.Open(filePath, flags, attr.Size)
	if err != nil {
		log.Errorf("open file[%s] failed: %v", filePath, err)
		return err
	}
	buf := make([]byte, attr.Size)
	_, err = fileHandle.Read(buf, 0)
	if err != nil {
		log.Errorf("fileHandle Read err[%v]", err)
		return err
	}
	content := buf

	var result map[string]common.FSMeta
	if len(content) != 0 {
		decodedLinksMeta, err := apicommon.AesDecrypt(string(content), apicommon.AESEncryptKey)
		if err != nil {
			log.Errorf("aes decrypt links meta json string err[%v]", err)
			return err
		}

		if err := json.Unmarshal([]byte(decodedLinksMeta), &result); err != nil {
			log.Errorf("json unmarshal links meta err[%v]", err)
			return err
		}
	}

	if err := m.UpdateUFSMap(result); err != nil {
		log.Errorf("update ufs map err[%v]", err)
		return err
	}
	m.ufsMapUT = attr.Mtime
	return nil
}
