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

package meta

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	pathlib "path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dgraph-io/ristretto"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	apicommon "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv"
	ufslib "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs"
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

var MetaCachePath string

type freeID struct {
	next  uint64
	maxid uint64
}

type FuseConfig struct {
	Uid          int
	Gid          int
	EntryTimeout time.Duration
	AttrTimeout  time.Duration
	DirMode      int
	FileMode     int
}

var FuseConf = &FuseConfig{
	EntryTimeout: 1 * time.Second,
	AttrTimeout:  1 * time.Second,
	DirMode:      ufslib.DefaultDirMode,
	FileMode:     ufslib.DefaultFileMode,
	Uid:          os.Getuid(),
	Gid:          os.Getgid(),
}

// kvMeta
type kvMeta struct {
	client       kv.KvClient
	attrTimeOut  time.Duration
	entryTimeOut time.Duration
	defaultUfs   ufslib.UnderFileStorage
	ufsMap       *sync.Map
	ufsMapLock   sync.RWMutex
	symlinks     *sync.Map
	ufsMapUT     int64
	setOwner     bool
	uid          uint32
	gid          uint32

	freeMu     sync.Mutex
	freeInodes freeID

	pathCache   *ristretto.Cache
	pathTimeOut time.Duration
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

type pathItem struct {
	name      []byte
	parentIno Ino
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
	if config.PathCacheExpire > 0 {
		pathCache, err := ristretto.NewCache(&ristretto.Config{
			NumCounters: 1e7,
			MaxCost:     1 << 30,
			BufferItems: 64,
		})
		if err != nil {
			return nil, err
		}
		m.pathCache = pathCache
		m.pathTimeOut = config.PathCacheExpire
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
	case common.SFTPType, common.CFSType, common.GlusterFSType, common.AFSType:
		properties[common.Address] = fsMeta.ServerAddress
	}
	return ufslib.NewUFS(fsMeta.UfsType, properties)
}

func newClient(config kv.Config) (kv.KvClient, error) {
	var client kv.KvClient
	var err error
	switch config.Driver {
	case kv.DiskType, kv.MemType:
		if config.CachePath != "" {
			config.CachePath = filepath.Join(config.CachePath, config.FsID,
				strconv.Itoa(int(time.Now().Unix()))+"_"+utils.GetRandID(5))
			MetaCachePath = config.CachePath
		}
		client, err = kv.NewBadgerClient(config)
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

func (m *kvMeta) symKey(ino Ino) []byte {
	return m.fmtKey("SYM", ino)
}

func (m *kvMeta) pathKey(ino Ino) []byte {
	return m.fmtKey("P", ino)
}

func (m *kvMeta) entryKey(parent Ino, name string) []byte {
	return m.fmtKey("E", parent, "N", name)
}

func (m *kvMeta) get(key []byte) ([]byte, error) {
	var value []byte
	err := m.client.Txn(func(tx kv.KvTxn) error {
		value = tx.Get(key)
		return nil
	})
	return value, err
}

func (m *kvMeta) set(key []byte, value []byte) error {
	err := m.client.Txn(func(tx kv.KvTxn) error {
		errTx := tx.Set(key, value)
		return errTx
	})
	return err
}

func (m *kvMeta) scanValues(prefix []byte) (map[string][]byte, error) {
	var values map[string][]byte
	err := m.client.Txn(func(tx kv.KvTxn) error {
		values, _ = tx.ScanValues(prefix)
		return nil
	})
	return values, err
}

func (m *kvMeta) nextInode() (Ino, error) {
	m.freeMu.Lock()
	defer m.freeMu.Unlock()
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

func (m *kvMeta) txn(f func(tx kv.KvTxn) error) error {
	var err error
	for i := 0; i < 100; i++ {
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
	m.parseInode(a, inodeItem_)
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

// pathCache为空的判断是为了测试方便，可以更方便的走之前的逻辑
func (m *kvMeta) setPathCache(inode Ino, inodeItem *inodeItem) {
	if m.pathCache == nil {
		return
	}
	pathItem_ := &pathItem{
		name:      inodeItem.name,
		parentIno: inodeItem.parentIno,
	}
	m.pathCache.SetWithTTL(m.pathKey(inode), pathItem_, 1, m.pathTimeOut)
}

func (m *kvMeta) delsPathCache(inodes ...Ino) {
	if m.pathCache == nil {
		return
	}
	for _, inode := range inodes {
		m.pathCache.Del(m.pathKey(inode))
	}
}

func (m *kvMeta) getPathCache(inode Ino) (interface{}, bool) {
	if m.pathCache == nil {
		return nil, false
	}
	return m.pathCache.Get(m.pathKey(inode))
}

// absolutePath by inode, and ensure that the absolute path is obtained in the same transaction
func (m *kvMeta) absolutePath(inode Ino, tx kv.KvTxn) string {
	var builder strings.Builder
	var segments []string
	if inode == rootInodeID {
		return "/"
	}
	var attr *inodeItem
	for {
		if inode == rootInodeID {
			break
		}
		if buf, exist := m.getPathCache(inode); exist {
			pathItem_ := buf.(*pathItem)
			inode = pathItem_.parentIno
			segments = append(segments, string(pathItem_.name))
		} else {
			d := tx.Get(m.inodeKey(inode))
			if d == nil {
				log.Errorf("full path parse fail with inode %v %+v", inode, attr)
				return ""
			}

			attr = &inodeItem{}
			m.parseInode(d, attr)
			inode = attr.parentIno
			segments = append(segments, string(attr.name))

		}

	}
	for i := len(segments) - 1; i >= 0; i-- {
		builder.WriteString("/")
		builder.WriteString(segments[i])
	}
	return builder.String()
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

	return findUfs, isLink, findKey, newPath
}

func (m *kvMeta) Name() string {
	return "meta_kv"
}

func (m *kvMeta) SetOwner(uid, gid uint32) {
}

func (m *kvMeta) StatFS(ctx *Context) (*base.StatfsOut, syscall.Errno) {
	log.Debugf("defaultMeta, StatFs: name[%s]", DefaultRootPath)
	ufs_ := m.defaultUfs
	return ufs_.StatFs(DefaultRootPath), syscall.F_OK
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

func (m *kvMeta) Lookup(ctx *Context, parent Ino, name string) (inode Ino, attr *Attr, errNo syscall.Errno) {
	defer func() {
		log.Debugf("kv meta Lookup parent Ino[%v] name [%s] attr[%+v] errNo[%v] ", parent, name, attr, errNo)
	}()
	attr = &Attr{}
	// todo:: add "." and ".."
	entry, err := m.get(m.entryKey(parent, name))
	if err != nil {
		log.Errorf("m get error %v", err)
		return 0, nil, syscall.EIO
	}
	inodeItem_ := &inodeItem{}
	if entry != nil {
		entryItem_ := &entryItem{}
		m.parseEntry(entry, entryItem_)
		inode = entryItem_.ino
		ok := m.getAttrFromCacheWithNoExpired(inode, inodeItem_)
		if ok {
			*attr = inodeItem_.attr
			m.setPathCache(inode, inodeItem_)
			return inode, attr, syscall.F_OK
		}
	}

	err = m.txn(func(tx kv.KvTxn) error {
		absolutePath := filepath.Join(m.absolutePath(parent, tx), name)
		ufs_, isLink, prefix, path := m.GetUFS(absolutePath)
		info, err := ufs_.GetAttr(path)
		now := time.Now()
		if err != nil {
			log.Debugf("[vfs-lookup] Lookup GetAttr failed: %v with path[%s] name[%s] and absolutePath[%s]", err, path, name, absolutePath)
			return err
		}
		if isLink {
			info.FixLinkPrefix(prefix)
		}
		attr.FromFileInfo(info)
		// uid gid mode not expire, use default config or user setattr and not use ufs model's uid, gid or mode
		if inodeItem_.attr.Type != 0 {
			attr.Uid = inodeItem_.attr.Uid
			attr.Gid = inodeItem_.attr.Gid
			if inodeItem_.attr.Mode != 0 {
				attr.Mode = inodeItem_.attr.Mode
			} else {
				if attr.Type == TypeDirectory {
					attr.Mode = syscall.S_IFDIR | uint32(FuseConf.DirMode)
				} else {
					attr.Mode = syscall.S_IFREG | uint32(FuseConf.FileMode)
				}
			}
		} else {
			attr.Uid = uint32(FuseConf.Uid)
			attr.Gid = uint32(FuseConf.Gid)
			if attr.Type == TypeDirectory {
				attr.Mode = syscall.S_IFDIR | uint32(FuseConf.DirMode)
			} else {
				attr.Mode = syscall.S_IFREG | uint32(FuseConf.FileMode)
			}
		}

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
				log.Debugf("tx set error %v", err)
				return err
			}
		}
		inodeItem_.expire = now.Add(m.attrTimeOut).Unix()
		inodeItem_.parentIno = parent
		inodeItem_.name = []byte(name)
		m.modifyTime(&(inodeItem_.attr), attr)
		inodeItem_.attr = *attr
		err = tx.Set(m.inodeKey(inode), m.marshalInode(inodeItem_))
		if err != nil {
			log.Errorf("set error %v", err)
		}
		return err
	})
	if err == nil {
		m.setPathCache(inode, inodeItem_)
	}
	if err != nil {
		log.Errorf("look up err %v", err)
	}
	return inode, attr, utils.ToSyscallErrno(err)
}

func (m *kvMeta) Resolve(ctx *Context, parent Ino, path string, inode *Ino, attr *Attr) syscall.Errno {
	return syscall.ENOSYS
}

var rootTime = time.Now()

func (m *kvMeta) GetAttr(ctx *Context, inode Ino, attr *Attr) (errNo syscall.Errno) {
	defer func() {
		log.Debugf("kv GetAttr inode[%v] attr[%+v] errNo[%v]", inode, attr, errNo)
	}()
	inodeItem_ := &inodeItem{}

	has := m.getAttrFromCacheWithNoExpired(inode, inodeItem_)
	if has {
		*attr = inodeItem_.attr
		if attr.Mode == 0 {
			log.Errorf("get cache mode is 0 arr[%+v]", attr)
		}
		m.setPathCache(inode, inodeItem_)
		return syscall.F_OK
	}

	if inode == rootInodeID {
		attr.Type = TypeDirectory
		attr.Mode = syscall.S_IFDIR | uint32(FuseConf.DirMode)
		attr.Nlink = 2
		attr.Size = 4 << 10
		attr.Uid = uint32(FuseConf.Uid)
		attr.Gid = uint32(FuseConf.Gid)
		attr.Mtime = rootTime.Unix()
		attr.Mtimensec = uint32(rootTime.Nanosecond())
		attr.Ctime = rootTime.Unix()
		attr.Ctimensec = uint32(rootTime.Nanosecond())

		if inodeItem_.attr.Type != 0 {
			attr.Uid = inodeItem_.attr.Uid
			attr.Gid = inodeItem_.attr.Gid
			if inodeItem_.attr.Mode != 0 {
				attr.Mode = inodeItem_.attr.Mode
			} else {
				if attr.Type == TypeDirectory {
					attr.Mode = syscall.S_IFDIR | uint32(FuseConf.DirMode)
				} else {
					attr.Mode = syscall.S_IFREG | uint32(FuseConf.FileMode)
				}
			}
		}

		now := time.Now()
		inodeItem_.attr = *attr
		inodeItem_.expire = now.Add(m.attrTimeOut + time.Hour*100).Unix()
		err := m.set(m.inodeKey(inode), m.marshalInode(inodeItem_))
		if err != nil {
			log.Errorf("set error %v", err)
			errNo = syscall.EBADF
			return errNo
		}

		return syscall.F_OK
	}

	err := m.txn(func(tx kv.KvTxn) error {
		absolutePath := m.absolutePath(inode, tx)
		ufs_, isLink, prefix, path := m.GetUFS(absolutePath)
		info, err := ufs_.GetAttr(path)
		if err != nil {
			log.Errorf("[vfs] GetAttr failed: %v with path[%s] and absolutePath[%s]", err, path, absolutePath)
			return err
		}
		if isLink {
			info.FixLinkPrefix(prefix)
		}
		now := time.Now()
		attr.FromFileInfo(info)

		// uid gid mode not expire, use default config or user setattr and not use ufs model's uid, gid or mode
		if inodeItem_.attr.Type != 0 {
			attr.Uid = inodeItem_.attr.Uid
			attr.Gid = inodeItem_.attr.Gid
			if inodeItem_.attr.Mode != 0 {
				attr.Mode = inodeItem_.attr.Mode
			} else {
				if attr.Type == TypeDirectory {
					attr.Mode = syscall.S_IFDIR | uint32(FuseConf.DirMode)
				} else {
					attr.Mode = syscall.S_IFREG | uint32(FuseConf.FileMode)
				}
			}
		} else {
			attr.Uid = uint32(FuseConf.Uid)
			attr.Gid = uint32(FuseConf.Gid)
			if attr.Type == TypeDirectory {
				attr.Mode = syscall.S_IFDIR | uint32(FuseConf.DirMode)
			} else {
				attr.Mode = syscall.S_IFREG | uint32(FuseConf.FileMode)
			}
		}

		m.modifyTime(&(inodeItem_.attr), attr)
		inodeItem_.attr = *attr
		inodeItem_.expire = now.Add(m.attrTimeOut).Unix()
		err = tx.Set(m.inodeKey(inode), m.marshalInode(inodeItem_))
		return err
	})
	if err == nil {
		m.setPathCache(inode, inodeItem_)
	}
	return utils.ToSyscallErrno(err)
}

func (m *kvMeta) SetAttr(ctx *Context, inode Ino, set uint32, attr *Attr) (string, syscall.Errno) {
	log.Debugf("kv meta setattr inode[%v] and set [%v] %+v", inode, set, attr)
	var absolutePath string
	var cur inodeItem
	var ufs_ ufslib.UnderFileStorage
	var isLink bool
	var prefix string
	var path string
	uid := attr.Uid
	gid := attr.Gid
	mode := attr.Mode
	atime := attr.Atime
	atimensec := attr.Atimensec
	mtime := attr.Mtime
	mtimesec := attr.Mtimensec
	size := attr.Size
	err := m.txn(func(tx kv.KvTxn) error {
		absolutePath = m.absolutePath(inode, tx)
		ufs_, isLink, prefix, path = m.GetUFS(absolutePath)
		if !m.getAttrFromCacheWithNoExpired(inode, &cur) {
			ufsAttr, err := ufs_.GetAttr(path)
			if err != nil {
				return err
			}
			if isLink {
				ufsAttr.FixLinkPrefix(prefix)
			}
			attr.FromFileInfo(ufsAttr)
			if attr.Type == TypeDirectory {
				attr.Mode = syscall.S_IFDIR | uint32(FuseConf.DirMode)
			} else {
				attr.Mode = syscall.S_IFREG | uint32(FuseConf.FileMode)
			}
			cur.attr = *attr
		} else {
			*attr = cur.attr
		}
		if set&FATTR_UID != 0 || set&FATTR_GID != 0 {
			log.Debugf("set uid %+v", set)
			cur.attr.Uid = uid
			cur.attr.Gid = gid
		}
		if set&FATTR_MODE != 0 {
			log.Debugf("set mode %+v", set)
			if mode == 0 {
				log.Errorf("set mode is 0 set %v", set)
			}
			cur.attr.Mode = mode
		}
		if set&FATTR_ATIME != 0 {
			log.Debugf("set Atime %+v", set)
			cur.attr.Atime = atime
			cur.attr.Atimensec = atimensec
		}

		if set&FATTR_MTIME != 0 {
			log.Debugf("set Mtime %+v", set)
			cur.attr.Mtime = mtime
			cur.attr.Mtimensec = mtimesec
		}

		if set&FATTR_SIZE != 0 {
			log.Debugf("set size %+v size %+v", set, size)
			cur.attr.Size = size
		}
		log.Debugf("set attr info is inode[%v] %+v", inode, cur.attr)
		err := tx.Set(m.inodeKey(inode), m.marshalInode(&cur))
		if err != nil {
			return err
		}
		return nil
	})
	// if set&FATTR_UID != 0 || set&FATTR_GID != 0 {
	// 	if err = ufs_.Chown(path, cur.attr.Uid, cur.attr.Gid); err != nil {
	// 		return "", utils.ToSyscallErrno(err)
	// 	}
	// }
	//
	// if set&FATTR_MODE != 0 {
	// 	if err = ufs_.Chmod(path, cur.attr.Mode); err != nil {
	// 		return "", utils.ToSyscallErrno(err)
	// 	}
	// }
	// s3未实现utimes函数，创建文件时存在报错：setting times of ‘xx’: Function not implemented。因此这里忽略enosys报错
	if set&FATTR_ATIME != 0 || set&FATTR_MTIME != 0 || set&FATTR_CTIME != 0 {
		atime := time.Unix(cur.attr.Atime, int64(cur.attr.Atimensec))
		ctime := time.Unix(cur.attr.Ctime, int64(cur.attr.Ctimensec))
		if err = ufs_.Utimens(path, &atime, &ctime); err != nil {
			return "", utils.ToSyscallErrno(err)
		}
	}

	if err != nil {
		return "", utils.ToSyscallErrno(err)
	}
	m.setPathCache(inode, &cur)
	return absolutePath, syscall.F_OK
}

func (m *kvMeta) Truncate(ctx *Context, inode Ino, size uint64) syscall.Errno {
	return syscall.F_OK
}

func (m *kvMeta) Fallocate(ctx *Context, inode Ino, mode uint8, off uint64, size uint64) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) ReadLink(ctx *Context, inode Ino, path *[]byte) syscall.Errno {
	attr := &inodeItem{}
	now := time.Now()
	err := m.txn(func(tx kv.KvTxn) error {
		inodeAttr := tx.Get(m.inodeKey(inode))
		if inodeAttr == nil {
			log.Errorf("no inode %v", inode)
			return syscall.ENOENT
		}
		linkPath := tx.Get(m.symKey(inode))
		if linkPath == nil {
			log.Errorf("no link %v", inode)
			return syscall.ENOENT
		}
		m.parseInode(inodeAttr, attr)
		*path = linkPath
		attr.attr.Atime = now.Unix()
		attr.attr.Atimensec = uint32(now.Nanosecond())
		err := tx.Set(m.inodeKey(inode), m.marshalInode(attr))
		if err != nil {
			log.Errorf("tx set err %v", err)
			return err
		}
		return nil
	})
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	return 0
}

func (m *kvMeta) Symlink(ctx *Context, parent Ino, name string, path string, inode *Ino, attr *Attr) syscall.Errno {
	defer func() {
		log.Infof("symlink name[%s] path[%s] attr[%+v]", name, path, attr)
	}()
	insertInodeItem_ := &inodeItem{}
	if attr == nil {
		attr = &Attr{}
	}
	attr.Mode = syscall.S_IFLNK | uint32(0777)
	now := time.Now()
	attr.Type = TypeSymlink
	attr.Uid = ctx.Uid
	attr.Gid = ctx.Gid
	// todo:: file mode including type and unix permission, add smode to transe
	attr.Nlink = 1
	attr.Size = uint64(len(path))
	insertInodeItem_.parentIno = parent
	insertInodeItem_.fileHandles = 1
	insertInodeItem_.name = []byte(name)
	// link设置无限大时间，永远不过期
	insertInodeItem_.expire = now.Add(time.Hour * 876000).Unix()

	ino, err := m.nextInode()
	*inode = ino
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	err = m.txn(func(tx kv.KvTxn) error {
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
		// 结构体中是没有指针，默认是深拷贝，等待 attr 赋值完成进行深拷贝
		insertInodeItem_.attr = *attr
		insertEntryItem_ := &entryItem{
			ino:  ino,
			mode: attr.Mode,
		}

		err = tx.Set(m.entryKey(parent, name), m.marshalEntry(insertEntryItem_))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(parent), m.marshalInode(&pInodeItem))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(ino), m.marshalInode(insertInodeItem_))
		if err != nil {
			return err
		}
		err = tx.Set(m.symKey(ino), []byte(path))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	m.setPathCache(ino, insertInodeItem_)
	return syscall.F_OK
}

func (m *kvMeta) Mknod(ctx *Context, parent Ino, name string, _type uint8, mode, cumask uint32, rdev uint32, inode *Ino, attr *Attr) syscall.Errno {
	insertInodeItem_ := &inodeItem{}
	if attr == nil {
		attr = &Attr{}
	}
	now := time.Now()
	attr.Type = _type
	attr.Uid = ctx.Uid
	attr.Gid = ctx.Gid
	// todo:: file mode including type and unix permission, add smode to transe
	attr.Mode = mode & ^cumask
	attr.Nlink = 1
	attr.Size = 4 << 10
	insertInodeItem_.attr = *attr
	insertInodeItem_.parentIno = parent
	insertInodeItem_.fileHandles = 1
	insertInodeItem_.name = []byte(name)
	insertInodeItem_.expire = now.Add(m.attrTimeOut).Unix()

	ino, err := m.nextInode()
	*inode = ino
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	var absolutePath string
	err = m.txn(func(tx kv.KvTxn) error {
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
			ino:  ino,
			mode: mode,
		}
		absolutePath = filepath.Join(m.absolutePath(parent, tx), name)

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
	ufs_, _, _, newPath := m.GetUFS(absolutePath)
	if err = ufs_.Mknod(newPath, mode, rdev); err != nil {
		return utils.ToSyscallErrno(err)
	}
	m.setPathCache(ino, insertInodeItem_)
	return syscall.F_OK
}

func (m *kvMeta) Mkdir(ctx *Context, parent Ino, name string, mode uint32, cumask uint16, inode *Ino, attr *Attr) syscall.Errno {
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

	ino, err := m.nextInode()
	*inode = ino
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	var absolutePath string
	err = m.txn(func(tx kv.KvTxn) error {
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
			ino:  ino,
			mode: mode,
		}
		absolutePath = filepath.Join(m.absolutePath(parent, tx), name)

		err = tx.Set(m.entryKey(parent, name), m.marshalEntry(insertEntryItem_))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(parent), m.marshalInode(&pInodeItem))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(ino), m.marshalInode(insertInodeItem_))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	ufs_, _, _, newPath := m.GetUFS(absolutePath)
	if err = ufs_.Mkdir(newPath, mode); err != nil {
		log.Errorf("kv meta mkdir parent %v name %s err %v", parent, name, err)
		return utils.ToSyscallErrno(err)
	}
	m.setPathCache(ino, insertInodeItem_)
	return syscall.F_OK
}

func (m *kvMeta) Unlink(ctx *Context, parent Ino, name string) syscall.Errno {
	log.Debugf("kv meta Unlink parent[%v] name[%s]", parent, name)
	var absolutePath string
	entryItem_ := &entryItem{}
	var isLink bool
	err := m.txn(func(tx kv.KvTxn) error {
		entry, err := m.get(m.entryKey(parent, name))
		if err != nil {
			return syscall.ENOENT
		}
		if entry == nil || len(entry) == 0 {
			return nil
		}
		m.parseEntry(entry, entryItem_)
		pinodebyte, err := m.get(m.inodeKey(parent))
		if pinodebyte == nil || err != nil {
			log.Errorf("[vfs-unlink] failed. file %v's pnode %v is not exist. \n", name, parent)
			return syscall.ENOENT
		}
		inodeItemBuf, err := m.get(m.inodeKey(entryItem_.ino))
		if inodeItemBuf == nil || err != nil {
			log.Errorf("[vfs-unlink] failed. file %v's pnode %v is not exist. \n", name, parent)
			return syscall.ENOENT
		}

		pinodeItem := &inodeItem{}
		inodeItem_ := &inodeItem{}
		m.parseInode(pinodebyte, pinodeItem)
		m.parseInode(inodeItemBuf, inodeItem_)
		now := time.Now()
		pinodeItem.attr.Mtime = now.Unix()
		pinodeItem.attr.Mtimensec = uint32(now.Nanosecond())
		pinodeItem.attr.Ctime = now.Unix()
		pinodeItem.attr.Ctimensec = uint32(now.Nanosecond())
		absolutePath = filepath.Join(m.absolutePath(parent, tx), name)
		if err = tx.Set(m.inodeKey(parent), m.marshalInode(pinodeItem)); err != nil {
			return err
		}
		if err = tx.Dels(m.entryKey(parent, name), m.inodeKey(entryItem_.ino)); err != nil {
			return err
		}
		if inodeItem_.attr.Type == TypeSymlink {
			isLink = true
			if err = tx.Dels(m.symKey(entryItem_.ino)); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	if !isLink {
		ufs_, _, _, path := m.GetUFS(absolutePath)
		if err = ufs_.Unlink(path); err != nil {
			log.Errorf("kv meta unlink parent %v name %s err %v", parent, name, err)
			return utils.ToSyscallErrno(err)
		}
	}
	m.delsPathCache(entryItem_.ino)
	return syscall.F_OK

}

func (m *kvMeta) Rmdir(ctx *Context, parent Ino, name string) syscall.Errno {
	log.Debugf("kv meta Rmdir parent[%v] name[%s]", parent, name)
	var absolutePath string
	inodeEntry := &entryItem{}
	err := m.txn(func(tx kv.KvTxn) error {
		buf := tx.Get(m.entryKey(parent, name))
		if buf == nil {
			return syscall.ENOENT
		}
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
		absolutePath = m.absolutePath(ino, tx)
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

		return nil
	})
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	ufs_, _, _, path := m.GetUFS(absolutePath)
	if err = ufs_.Rmdir(path); err != nil {
		log.Errorf("kv meta rmdir parent %v name %s err %v", parent, name, err)
		return utils.ToSyscallErrno(err)
	}
	m.delsPathCache(inodeEntry.ino)
	return syscall.F_OK
}

func (m *kvMeta) Rename(ctx *Context, parentSrc Ino, nameSrc string, parentDst Ino, nameDst string, flags uint32, inode *Ino, attr *Attr) (string, string, syscall.Errno) {
	log.Debugf("kv meta rename parentSrc[%v]'s[%s] to parentDst[%v]'s[%s]", parentSrc, nameSrc, parentDst, nameDst)
	var pathDst string
	var pathSrc string
	srcAttr := &inodeItem{}
	srcEntryItem_ := &entryItem{}
	err := m.txn(func(tx kv.KvTxn) error {
		buf := tx.Get(m.entryKey(parentSrc, nameSrc))
		if buf == nil {
			return syscall.ENOENT
		}
		m.parseEntry(buf, srcEntryItem_)

		now := time.Now()
		pSrcAttr := &inodeItem{}
		pDstAttr := &inodeItem{}

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

		pDstAttr.attr.Mtime = now.Unix()
		pDstAttr.attr.Mtimensec = uint32(now.Nanosecond())
		pDstAttr.attr.Ctime = now.Unix()
		pDstAttr.attr.Ctimensec = uint32(now.Nanosecond())

		srcAttr.parentIno = parentDst
		srcAttr.name = []byte(nameDst)
		// 更新srcPath
		srcAttr.attr.Ctime = now.Unix()
		srcAttr.attr.Ctimensec = uint32(now.Nanosecond())
		if inode != nil {
			*inode = srcEntryItem_.ino
		}
		if attr != nil {
			*attr = srcAttr.attr
		}
		if parentDst != parentSrc {
			_ = tx.Set(m.inodeKey(parentSrc), m.marshalInode(pSrcAttr))
		}
		_ = tx.Set(m.entryKey(parentDst, nameDst), m.marshalEntry(srcEntryItem_))
		_ = tx.Set(m.inodeKey(*inode), m.marshalInode(srcAttr))
		_ = tx.Dels(m.entryKey(parentSrc, nameSrc))
		_ = tx.Set(m.inodeKey(parentDst), m.marshalInode(pDstAttr))

		// ufs rename
		pathDst = filepath.Join(m.absolutePath(parentDst, tx), nameDst)
		pathSrc = filepath.Join(m.absolutePath(parentSrc, tx), nameSrc)

		return nil
	})
	if err != nil {
		return "", "", utils.ToSyscallErrno(err)
	}
	ufsSrc, _, _, pathOld := m.GetUFS(pathSrc)
	ufsDst, _, _, pathNew := m.GetUFS(pathDst)
	if ufsDst != ufsSrc {
		log.Errorf("Rename between two ufs is not supported")
		return "", "", syscall.ENOSYS
	}
	err = ufsSrc.Rename(pathOld, pathNew)
	if err != nil {
		log.Errorf("kv meta rename parentSrc %v nameSrc %v parentDst %v nameDst %v err %v",
			parentSrc, nameSrc, parentDst, nameDst, err)
		return "", "", utils.ToSyscallErrno(err)
	}
	m.setPathCache(srcEntryItem_.ino, srcAttr)
	return pathSrc, pathDst, syscall.F_OK
}

func (m *kvMeta) Link(ctx *Context, inodeSrc, parent Ino, name string, attr *Attr) syscall.Errno {
	return syscall.ENOSYS
}

// badger do not allow a Tnx which is too big
type entrySliceItem struct {
	name string
	et   *entryItem
}

type inodeSliceItem struct {
	ino Ino
	it  *inodeItem
}

func (m *kvMeta) updateDirentrys(parent Ino, entrySlice []entrySliceItem, inodeSlice []inodeSliceItem) error {
	g := new(errgroup.Group)
	for begin := 0; begin < len(entrySlice); begin += 1000 {
		begin_ := begin
		g.Go(func() error {
			err := m.txn(func(tx kv.KvTxn) error {
				for i := begin_; i < begin_+1000 && i < len(entrySlice); i++ {
					if badgerErr := tx.Set(m.entryKey(parent, entrySlice[i].name), m.marshalEntry(entrySlice[i].et)); badgerErr != nil {
						return badgerErr
					}
				}
				return nil
			})
			return err
		})
	}
	for begin := 0; begin < len(inodeSlice); begin += 1000 {
		begin_ := begin
		g.Go(func() error {
			err := m.txn(func(tx kv.KvTxn) error {
				for i := begin_; i < begin_+1000 && i < len(inodeSlice); i++ {
					if badgerErr := tx.Set(m.inodeKey(inodeSlice[i].ino), m.marshalInode(inodeSlice[i].it)); badgerErr != nil {
						return badgerErr
					}
				}
				return nil
			})
			return err
		})
	}
	if err := g.Wait(); err != nil {
		log.Errorf("kv-meta update entries, errgroup wait err: %s", err.Error())
		return err
	}
	return nil
}

func (m *kvMeta) Readdir(ctx *Context, inode Ino, entries *[]*Entry) syscall.Errno {
	defer func() {
		log.Debugf("Readdir inode[%v] lenEntries[%v]", inode, len(*entries))
	}()
	dirInodeItem := &inodeItem{}
	entrySlice := make([]entrySliceItem, 0)
	inodeSlice := make([]inodeSliceItem, 0)
	var parentIno Ino
	now := time.Now()
	var fromCache bool
	if inode == rootInodeID {
		log.Infof("root readdir %v", inode)
		attrTmp := &Attr{}
		err := m.GetAttr(ctx, inode, attrTmp)
		if err != 0 {
			log.Errorf("get attr err %v", err)
			return syscall.EBADF
		}
	}
	err := m.txn(func(tx kv.KvTxn) error {
		buf := tx.Get(m.inodeKey(inode))
		if buf == nil {
			log.Errorf("get attr dir inode %v empty", inode)
			return syscall.ENOENT
		}
		m.parseInode(buf, dirInodeItem)
		parentIno = dirInodeItem.parentIno
		entry := tx.Get(m.entryKey(parentIno, string(dirInodeItem.name)))
		if entry != nil {
			dirEntryItem := &entryItem{}
			m.parseEntry(entry, dirEntryItem)
			if dirEntryItem.done == entryDone && !m.entryItemExpired(dirEntryItem) {
				ens, err := tx.ScanValues(m.entryKey(inode, ""))
				if err != nil {
					return err
				}
				prefix := len(m.entryKey(inode, ""))
				// var childEntryItem *entryItem
				for name, childEntryBuf := range ens {
					childEntryItem := &entryItem{}
					m.parseEntry(childEntryBuf, childEntryItem)
					en := &Entry{
						Ino:  childEntryItem.ino,
						Name: name[prefix:],
						Attr: &Attr{Mode: childEntryItem.mode},
					}
					*entries = append(*entries, en)
				}
				fromCache = true
				return nil
			}
		}
		absolutePath := m.absolutePath(inode, tx)
		ufs_, isLink, _, path := m.GetUFS(absolutePath)
		dirs, err := ufs_.ReadDir(path)
		if err != nil {
			log.Errorf("meta-kv read dir from ufs succeed, but update dir err: %s", err.Error())
			return err
		}

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
		fromCache = false
		log.Debugf("meta-kv got [%d] dirEntries from ufs ", len(dirs))

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
			insertChildEntry := &entryItem{}
			if childEntryBuf != nil {
				childEntryItemFromCache = &entryItem{}
				m.parseEntry(childEntryBuf, childEntryItemFromCache)
				newInode = childEntryItemFromCache.ino
				insertChildEntry.done = childEntryItemFromCache.done
				insertChildEntry.expire = childEntryItemFromCache.expire
				insertChildEntry.ino = childEntryItemFromCache.ino
				insertChildEntry.mode = childEntryItemFromCache.mode
			} else {
				newInodeNumber, err := m.nextInode()
				if err != nil {
					return err
				}
				newInode = newInodeNumber
				if dir.Attr.Type == TypeDirectory {
					insertChildEntry.mode = uint32(utils.StatModeToFileMode(int(syscall.S_IFDIR | uint32(FuseConf.DirMode))))
				} else {
					insertChildEntry.mode = uint32(utils.StatModeToFileMode(int(syscall.S_IFREG | uint32(FuseConf.FileMode))))
				}
				insertChildEntry.ino = newInode
			}
			entrySlice = append(entrySlice, entrySliceItem{
				dir.Name,
				insertChildEntry,
			})
			expire = now.Add(m.attrTimeOut).Unix()
			insertChildInode = &inodeItem{
				attr:      *childEntryItem.Attr,
				parentIno: inode,
				expire:    expire,
				name:      []byte(dir.Name),
			}
			newInodeItemBuf := tx.Get(m.inodeKey(newInode))
			if len(newInodeItemBuf) != 0 {
				var newInodeItem inodeItem
				m.parseInode(newInodeItemBuf, &newInodeItem)
				if newInodeItem.fileHandles != 0 {
					insertChildInode.fileHandles = newInodeItem.fileHandles
					insertChildInode.attr.Size = newInodeItem.attr.Size
				}
				// uid gid mode not expire, use default config or user setattr and not use ufs model's uid, gid or mode
				insertChildInode.attr.Uid = newInodeItem.attr.Uid
				insertChildInode.attr.Gid = newInodeItem.attr.Gid
				if newInodeItem.attr.Mode != 0 {
					insertChildInode.attr.Mode = newInodeItem.attr.Mode
				} else {
					if insertChildInode.attr.Type == TypeDirectory {
						insertChildInode.attr.Mode = syscall.S_IFDIR | uint32(FuseConf.DirMode)
					} else {
						insertChildInode.attr.Mode = syscall.S_IFREG | uint32(FuseConf.FileMode)
					}
				}
			} else {
				insertChildInode.attr.Uid = uint32(FuseConf.Uid)
				insertChildInode.attr.Gid = uint32(FuseConf.Gid)
				if insertChildInode.attr.Type == TypeDirectory {
					insertChildInode.attr.Mode = syscall.S_IFDIR | uint32(FuseConf.DirMode)
				} else {
					insertChildInode.attr.Mode = syscall.S_IFREG | uint32(FuseConf.FileMode)
				}
			}

			childEntryItem.Attr.Uid = insertChildInode.attr.Uid
			childEntryItem.Attr.Gid = insertChildInode.attr.Gid
			childEntryItem.Attr.Mode = insertChildInode.attr.Mode

			inodeSlice = append(inodeSlice, inodeSliceItem{
				newInode,
				insertChildInode,
			})
			childEntryItem.Ino = newInode
			*entries = append(*entries, childEntryItem)
		}
		return nil
	})
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	if fromCache {
		return syscall.F_OK
	}

	// 从远端拉取dir的entries时 更新badger（删除badger里存在的dir下的entries）,目的是避免远端删除了dir下的一些entries,但badger里还存在这些entries的情况
	err = m.txn(func(tx kv.KvTxn) error {
		ens, err := tx.ScanValues(m.entryKey(inode, ""))
		if err != nil {
			return err
		}
		for _, childEntryBuf := range ens {
			childEntryItem := &entryItem{}
			m.parseEntry(childEntryBuf, childEntryItem)
			if childEntryItem.mode == syscall.S_IFLNK|uint32(0777) {
				linkEntry := Entry{}
				linkEntry.Ino = childEntryItem.ino
				linkInodeItem := &inodeItem{}
				if linkBuf := tx.Get(m.inodeKey(childEntryItem.ino)); len(linkBuf) != 0 {
					m.parseInode(linkBuf, linkInodeItem)
				} else {
					log.Errorf("link inode get err wit linkBuf empty")
					return syscall.EBADF
				}
				linkEntry.Name = string(linkInodeItem.name)
				linkEntry.Attr = &linkInodeItem.attr
				*entries = append(*entries, &linkEntry)
				continue
			}
			childInoItem := &inodeItem{}
			if childInodeBuf := tx.Get(m.inodeKey(childEntryItem.ino)); len(childInodeBuf) != 0 {
				m.parseInode(childInodeBuf, childInoItem)
				_ = tx.Dels(m.entryKey(inode, string(childInoItem.name)))

			}
			_ = tx.Dels(m.inodeKey(childEntryItem.ino))

		}
		return nil
	})
	if err != nil {
		log.Errorf("meta-kv read dir[%v]. deletes invalid entries err: %s", inode, err.Error())
		return utils.ToSyscallErrno(err)
	}
	err = m.updateDirentrys(inode, entrySlice, inodeSlice)
	if err != nil {
		log.Errorf("meta-kv read dir from ufs succeed, but update entries err: %s", err.Error())
		return utils.ToSyscallErrno(err)
	}
	// update dir
	insertDirEntry := &entryItem{
		ino:    inode,
		mode:   dirInodeItem.attr.Mode,
		expire: now.Add(m.entryTimeOut).Unix(),
		done:   entryDone,
	}
	err = m.txn(func(tx kv.KvTxn) error {
		err = tx.Set(m.entryKey(parentIno, string(dirInodeItem.name)), m.marshalEntry(insertDirEntry))
		return err
	})
	if err != nil {
		return utils.ToSyscallErrno(err)
	}
	m.setPathCache(inode, dirInodeItem)
	return syscall.F_OK
}

func (m *kvMeta) Create(ctx *Context, parent Ino, name string, mode uint32, cumask uint16, flags uint32, inode *Ino, attr *Attr) (ufslib.UnderFileStorage, string, syscall.Errno) {
	log.Debugf("kv meta create parent[%v] name[%s]", parent, name)
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

	var newPath string
	var ufs_ ufslib.UnderFileStorage
	var absolutePath string
	err = m.txn(func(tx kv.KvTxn) error {
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
			ino:  ino,
			mode: mode,
		}
		absolutePath = filepath.Join(m.absolutePath(parent, tx), name)

		err = tx.Set(m.entryKey(parent, name), m.marshalEntry(insertEntryItem_))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(parent), m.marshalInode(&pInodeItem))
		if err != nil {
			return err
		}
		err = tx.Set(m.inodeKey(ino), m.marshalInode(insertInodeItem_))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, "", utils.ToSyscallErrno(err)
	}
	ufs_, _, _, newPath = m.GetUFS(absolutePath)
	fh, err := ufs_.Create(newPath, flags, mode)
	if err != nil {
		log.Errorf("Create: name[%s], flags[%d], mode[%d], failed: [%v]",
			name, flags, mode, err)
		return nil, "", utils.ToSyscallErrno(err)
	} else {
		m.setPathCache(ino, insertInodeItem_)
	}
	defer fh.Release()
	return ufs_, newPath, utils.ToSyscallErrno(err)
}

func (m *kvMeta) Open(ctx *Context, inode Ino, flags uint32, attr *Attr) (ufslib.UnderFileStorage, string, syscall.Errno) {
	log.Debugf("kv meta Open inode[%v]", inode)
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
		return nil, "", syscall.F_OK
	}
	var ufs_ ufslib.UnderFileStorage
	var newPath string
	var isLink bool
	var prefix string
	err := m.txn(func(tx kv.KvTxn) error {
		absolutePath := m.absolutePath(inode, tx)
		ufs_, isLink, prefix, newPath = m.GetUFS(absolutePath)

		a := tx.Get(m.inodeKey(inode))
		if a != nil {
			m.parseInode(a, inodeItem_)
			if !m.inodeItemExpired(*inodeItem_) {
				log.Debugf("open inodeItem cache %+v and attr %+v", *inodeItem_, inodeItem_.attr)
				*attr = inodeItem_.attr
				inodeItem_.fileHandles += 1
				err := tx.Set(m.inodeKey(inode), m.marshalInode(inodeItem_))
				return err
			}
		} else {
			for i := 0; i < 3; i++ {
				log.Errorf("open inode[%v] not exist [%v]", inode, i)
				time.Sleep(time.Duration(i*100) * time.Millisecond)
				a = tx.Get(m.inodeKey(inode))
				if a != nil {
					break
				}
			}
			if a == nil {
				log.Errorf("open emtpy inode")
				return syscall.ENOENT
			}
		}
		info, err := ufs_.GetAttr(newPath)
		if err != nil {
			log.Errorf("[vfs] GetAttr failed: %v with path[%s] and absolutePath[%s]", err, newPath, absolutePath)
			if utils.IfNotExist(err) {
				_ = tx.Dels(m.inodeKey(inode), m.entryKey(inodeItem_.parentIno, string(inodeItem_.name)))
			}
			return err
		}
		if isLink {
			info.FixLinkPrefix(prefix)
		}
		now := time.Now()
		attr.FromFileInfo(info)
		// 复用缓存里面的mode，这个值是固定值
		if inodeItem_.attr.Mode != 0 {
			attr.Mode = inodeItem_.attr.Mode
		} else {
			log.Errorf("open mode is zero inodeItem[%+v]", inodeItem_.attr)
			if attr.Type == TypeDirectory {
				attr.Mode = syscall.S_IFDIR | uint32(FuseConf.DirMode)
			} else {
				attr.Mode = syscall.S_IFREG | uint32(FuseConf.FileMode)
			}
		}
		attr.Uid = inodeItem_.attr.Uid
		attr.Gid = inodeItem_.attr.Gid
		m.modifyTime(&(inodeItem_.attr), attr)
		inodeItem_.attr = *attr
		inodeItem_.expire = now.Add(m.attrTimeOut).Unix()
		inodeItem_.fileHandles += 1
		err = tx.Set(m.inodeKey(inode), m.marshalInode(inodeItem_))
		return err
	})
	if err != nil {
		log.Errorf("open inode[%v] err %v", inode, err)
		return nil, "", utils.ToSyscallErrno(err)
	}
	m.setPathCache(inode, inodeItem_)
	return ufs_, newPath, syscall.F_OK
}

func (m *kvMeta) Close(ctx *Context, inode Ino) syscall.Errno {
	err := m.txn(func(tx kv.KvTxn) error {
		updateInodeItem := &inodeItem{}
		buf := tx.Get(m.inodeKey(inode))
		if buf != nil {
			m.parseInode(buf, updateInodeItem)
			if !m.inodeItemExpired(*updateInodeItem) && updateInodeItem.attr.Type == TypeFile {
				updateInodeItem.fileHandles -= 1
				if updateInodeItem.fileHandles < 0 {
					log.Errorf("inode[%v] close file handles not correct %v and inodeItem %+v", inode, updateInodeItem.fileHandles, updateInodeItem)
					return nil
				}
				log.Tracef("close fileHandles %v", updateInodeItem.fileHandles)
				return tx.Set(m.inodeKey(inode), m.marshalInode(updateInodeItem))

			}
		}
		return nil
	})
	return utils.ToSyscallErrno(err)
}

func (m *kvMeta) Read(ctx *Context, inode Ino, indx uint32, buf []byte) syscall.Errno {
	return syscall.ENOSYS
}

func (m *kvMeta) Write(ctx *Context, inode Ino, off uint64, length int) syscall.Errno {
	updateInodeItem := &inodeItem{}

	err := m.txn(func(tx kv.KvTxn) error {
		tmp := tx.Get(m.inodeKey(inode))
		if tmp == nil || len(tmp) == 0 {
			return nil
		}
		m.parseInode(tmp, updateInodeItem)
		if !m.inodeItemExpired(*updateInodeItem) {
			now := time.Now()
			newLength := uint64(int(off) + length)
			if newLength > updateInodeItem.attr.Size {
				updateInodeItem.attr.Size = newLength
			}
			updateInodeItem.attr.Ctime = now.Unix()
			updateInodeItem.attr.Ctimensec = uint32(now.Nanosecond())
			updateInodeItem.attr.Mtime = now.Unix()
			updateInodeItem.attr.Mtimensec = uint32(now.Nanosecond())
		} else {
			return nil
		}

		return tx.Set(m.inodeKey(inode), m.marshalInode(updateInodeItem))
	})
	return utils.ToSyscallErrno(err)
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

func (m *kvMeta) ClientClose(stopChan chan struct{}) {
	for {
		select {
		case <-stopChan:
			m.client.Close()
			m.pathCache.Close()
			return
		}
	}
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
	if attr.Type == TypeDirectory {
		attr.Mode = syscall.S_IFDIR | uint32(FuseConf.DirMode)
	} else {
		attr.Mode = syscall.S_IFREG | uint32(FuseConf.FileMode)
	}
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
		decodedLinksMeta, err := apicommon.AesDecrypt(string(content), apicommon.GetAESEncryptKey())
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
