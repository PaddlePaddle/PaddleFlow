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
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv"
	ufslib "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
)

const (
	AttrKey  = "A"
	EntryKey = "P"
	// attrCacheSize struct size
	attrCacheSize = 97
	// entryCacheSize struct size
	entryCacheSize = 21
	entryDone      = 1
)

var _ Meta = &kvMeta{}

// kvMeta
type kvMeta struct {
	client       kv.Client
	defaultMeta  Meta
	attrTimeOut  time.Duration
	entryTimeOut time.Duration
}

type entryCacheItem struct {
	ino    Ino
	mode   uint32
	expire int64
	// if done is true, means the parent entry for readdir cache is effective.
	done uint8
}

type attrCacheItem struct {
	attr   Attr
	expire int64
}

// SliceByte is definition of slice([]byte), use unsafe.Pointer complete the conversion of structure and []byte
type SliceByte struct {
	addr uintptr
	len  int
	cap  int
}

func newKvMeta(defaultMeta Meta, config Config) (Meta, error) {
	if config.Driver == DefaultName {
		// default meta has no cache. query from remote each time
		return defaultMeta, nil
	}

	client, err := newClient(config.Config)
	if err != nil {
		return nil, err
	}
	m := &kvMeta{
		client:       client,
		defaultMeta:  defaultMeta,
		attrTimeOut:  config.AttrCacheExpire,
		entryTimeOut: config.EntryCacheExpire,
	}
	return m, nil
}

func newClient(config kv.Config) (kv.Client, error) {
	var client kv.Client
	var err error
	switch config.Driver {
	case kv.LevelDB:
		client, err = kv.NewLevelDBClient(config)
	case kv.NutsDB:
		client, err = kv.NewNutsClient(config)
	case kv.Mem:
		client, err = kv.NewMemClient(config)
	default:
		return nil, fmt.Errorf("unknown meta client")
	}
	return client, err
}

func (m *kvMeta) ContactKey(args ...string) []byte {
	var buffer bytes.Buffer
	for _, arg := range args {
		buffer.WriteString(arg)
	}
	return buffer.Bytes()
}

func (m *kvMeta) FullPath(parent Ino, name string) string {
	return filepath.Join(m.InoToPath(parent), name)
}

func (m *kvMeta) findEntry(parentPath, fullPath string) ([]byte, bool) {
	data, has := m.client.Get(m.entryKey(parentPath, fullPath))
	return data, has
}

func (m *kvMeta) entryKey(parentPath, fullPath string) []byte {
	return m.ContactKey(EntryKey, parentPath, fullPath)
}

func (m *kvMeta) attrKey(fullPath string) []byte {
	return m.ContactKey(AttrKey, fullPath)
}

func (m *kvMeta) tryGetAttr(ino Ino, attr *attrCacheItem) bool {
	fullPath := m.InoToPath(ino)
	a, ok := m.client.Get(m.attrKey(fullPath))
	if !ok || cap(a) < attrCacheSize {
		return false
	}
	m.parseAttr(a, attr)
	if attr.expire < time.Now().Unix() {
		go func() {
			m.client.Dels(m.attrKey(fullPath))
		}()
		return false
	}
	return true
}

// Put inode to cache, if ino already in cache, update
func (m *kvMeta) putAttr(fullPath string, attr attrCacheItem, expire int64) {
	attr.expire = expire
	value := m.marshalAttr(&attr)
	err := m.client.Set(m.attrKey(fullPath), value)
	if err != nil {
		log.Errorf("putAttr cache err %v", err)
		return
	}
}

func (m *kvMeta) putEntry(parentPath string, entry entryCacheItem, expire int64) {
	entry.expire = expire
	value := m.marshalEntry(&entry)
	entryPath := m.InoToPath(entry.ino)
	entryKey := m.entryKey(parentPath, entryPath)
	err := m.client.Set(entryKey, value)

	if err != nil {
		m.client.Dels(entryKey)
		log.Errorf("putEntry cache err %v", err)
		return
	}
}

func (m *kvMeta) putEntries(parentEntry entryCacheItem, entries []entryCacheItem, expire int64) {
	parentPath := m.InoToPath(parentEntry.ino)
	for _, entry := range entries {
		m.putEntry(parentPath, entry, expire)
	}
	parentEntry.expire = expire
	parentEntry.done = entryDone
	value := m.marshalEntry(&parentEntry)

	err := m.client.Set(m.entryKey(parentPath, parentPath), value)
	if err != nil {
		log.Errorf("putEntries cache err %v", err)
		return
	}
	value, _ = m.client.Get(m.entryKey(parentPath, parentPath))
}

func (m *kvMeta) getEntries(entryPath string) (map[string][]byte, bool) {
	value, has := m.client.Get(m.entryKey(entryPath, entryPath))
	if !has || cap(value) < entryCacheSize {
		return nil, false
	}
	entryCacheItem_ := &entryCacheItem{}
	m.parseEntry(value, entryCacheItem_)

	if entryCacheItem_.done != entryDone || entryCacheItem_.expire < time.Now().Unix() {
		return nil, false
	}
	key := m.entryKey(entryPath, entryPath)
	en, err := m.client.ScanValues(key)
	if err != nil {
		log.Debugf("scanValues err: %v with key[%s]", err, key)
		return nil, false
	}
	for k, _ := range en {
		if k == string(key) {
			delete(en, k)
			break
		}
	}
	return en, true
}

func (m *kvMeta) removeEntry(parentPath, name string) {
	fullPath := filepath.Join(parentPath, name)
	err := m.client.Dels(m.entryKey(parentPath, fullPath))
	if err != nil {
		log.Errorf("removeEntry err: %v", err)
	}
}

func (m *kvMeta) removeAttr(fullPath string) {
	err := m.client.Dels(m.attrKey(fullPath))
	if err != nil {
		log.Errorf("removeAttr err: %v", err)
	}
}

func (m *kvMeta) SetOwner(uid, gid uint32) {
}

func (m *kvMeta) GetUFS(name string) (ufslib.UnderFileStorage, bool, string, string) {
	return m.defaultMeta.GetUFS(name)
}

func (m *kvMeta) Name() string {
	return m.client.Name()
}

func (m *kvMeta) InoToPath(inode Ino) string {
	return m.defaultMeta.InoToPath(inode)
}

func (m *kvMeta) PathToIno(path string) Ino {
	return m.defaultMeta.PathToIno(path)
}

// StatFS returns summary statistics of a volume.
func (m *kvMeta) StatFS(ctx *Context) (*base.StatfsOut, syscall.Errno) {
	return m.defaultMeta.StatFS(ctx)
}

// Access checks the access permission on given inode.
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

	if !utils.HasAccess(ctx.Uid, ctx.Gid, attr.Uid, attr.Gid, attr.Mode, mask) {
		return syscall.Errno(fuse.EACCES)
	}
	return syscall.F_OK
}

// Lookup returns the inode and attributes for the given entry in a directory.
func (m *kvMeta) Lookup(ctx *Context, parent Ino, name string) (Ino, *Attr, syscall.Errno) {
	parentPath := m.InoToPath(parent)
	fullPath := filepath.Join(parentPath, name)
	en, has := m.findEntry(parentPath, fullPath)
	if has {
		entryCache_ := &entryCacheItem{}
		m.parseEntry(en, entryCache_)
		attr := &Attr{}
		err := m.GetAttr(ctx, entryCache_.ino, attr)
		log.Debugf("LookUp attr is %+v", *attr)
		return entryCache_.ino, attr, err
	}

	ino, attr, err := m.defaultMeta.Lookup(ctx, parent, name)

	if utils.IsError(err) {
		return ino, attr, err
	}

	now := time.Now()
	attrExpire := now.Add(m.attrTimeOut).Unix()
	entryExpire := now.Add(m.entryTimeOut).Unix()
	attrCache_ := attrCacheItem{
		attr: *attr,
	}
	m.putAttr(fullPath, attrCache_, attrExpire)
	entryCache_ := entryCacheItem{
		ino:  ino,
		mode: attr.Mode,
	}
	m.putEntry(parentPath, entryCache_, entryExpire)
	return ino, attr, err
}

// Resolve fetches the inode and attributes for an entry identified by the given path.
// ENOTSUP will be returned if there's no natural implementation for this operation or
// if there are any symlink following involved.
func (m *kvMeta) Resolve(ctx *Context, parent Ino, path string, inode *Ino, attr *Attr) syscall.Errno {
	return m.defaultMeta.Resolve(ctx, parent, path, inode, attr)
}

// GetAttr returns the attributes for given node.
func (m *kvMeta) GetAttr(ctx *Context, inode Ino, attr *Attr) syscall.Errno {
	attrCache_ := &attrCacheItem{}
	if ok := m.tryGetAttr(inode, attrCache_); ok {
		log.Debugf("try get attr data %+v", attrCache_.attr)
		*attr = attrCache_.attr
		log.Debugf("try attr %+v", *attr)
		return syscall.F_OK
	}

	if err := m.doGetAttr(ctx, inode, attr); utils.IsError(err) {
		return err
	}
	attrCache_.attr = *attr
	expire := time.Now().Add(m.attrTimeOut).Unix()
	m.putAttr(m.InoToPath(inode), *attrCache_, expire)
	return syscall.F_OK
}

func (m *kvMeta) doGetAttr(ctx *Context, inode Ino, attr *Attr) syscall.Errno {
	return m.defaultMeta.GetAttr(ctx, inode, attr)
}

// SetAttr updates the attributes for given node.
func (m *kvMeta) SetAttr(ctx *Context, inode Ino, set uint32, attr *Attr) syscall.Errno {
	if err := m.defaultMeta.SetAttr(ctx, inode, set, attr); utils.IsError(err) {
		return err
	}
	expire := time.Now().Add(m.attrTimeOut).Unix()
	attrCache_ := &attrCacheItem{attr: *attr}
	m.putAttr(m.InoToPath(inode), *attrCache_, expire)
	return syscall.F_OK
}

// Truncate changes the length for given file.
func (m *kvMeta) Truncate(ctx *Context, inode Ino, size uint64) syscall.Errno {
	if err := m.defaultMeta.Truncate(ctx, inode, size); utils.IsError(err) {
		return err
	}

	attr := &Attr{}
	if err := m.doGetAttr(ctx, inode, attr); utils.IsError(err) {
		return err
	}
	expire := time.Now().Add(m.attrTimeOut).Unix()
	attrCache_ := &attrCacheItem{attr: *attr}
	m.putAttr(m.InoToPath(inode), *attrCache_, expire)
	return syscall.F_OK
}

// Fallocate preallocate given space for given file.
func (m *kvMeta) Fallocate(ctx *Context, inode Ino, mode uint8, off uint64, size uint64) syscall.Errno {
	return m.defaultMeta.Fallocate(ctx, initDirSize, mode, off, size)
}

// ReadLink returns the target of a symlink.
func (m *kvMeta) ReadLink(ctx *Context, inode Ino, path *[]byte) syscall.Errno {
	return m.defaultMeta.ReadLink(ctx, inode, path)
}

// Symlink creates a symlink in a directory with given name.
func (m *kvMeta) Symlink(ctx *Context, parent Ino, name string, path string, inode *Ino, attr *Attr) syscall.Errno {
	return m.defaultMeta.Symlink(ctx, parent, name, path, inode, attr)
}

// Mknod creates a node in a directory with given name, type and permissions.
// TODO: inode和attr是不是必须要往抛，目前是按照juicefs添加了inode和attr，后续根据vfs需求做删减
func (m *kvMeta) Mknod(ctx *Context, parent Ino, name string, mode uint32, rdev uint32, inode *Ino, attr *Attr) syscall.Errno {
	if err := m.defaultMeta.Mknod(ctx, parent, name, mode, rdev, inode, attr); utils.IsError(err) {
		return err
	}
	now := time.Now()
	attrExpire := now.Add(m.attrTimeOut).Unix()
	entryExpire := now.Add(m.entryTimeOut).Unix()
	attrCache_ := &attrCacheItem{attr: *attr}
	fullPath := m.InoToPath(*inode)
	m.putAttr(fullPath, *attrCache_, attrExpire)

	entryCache_ := &entryCacheItem{
		ino:  *inode,
		mode: attr.Mode,
	}
	m.putEntry(m.InoToPath(parent), *entryCache_, entryExpire)
	return syscall.F_OK
}

// Mkdir creates a sub-directory with given name and mode.
func (m *kvMeta) Mkdir(ctx *Context, parent Ino, name string, mode uint32, inode *Ino, attr *Attr) syscall.Errno {
	if err := m.defaultMeta.Mkdir(ctx, parent, name, mode, inode, attr); utils.IsError(err) {
		return err
	}
	now := time.Now()
	attrExpire := now.Add(m.attrTimeOut).Unix()
	entryExpire := now.Add(m.entryTimeOut).Unix()

	attrCache_ := &attrCacheItem{attr: *attr}
	fullPath := m.InoToPath(*inode)
	m.putAttr(fullPath, *attrCache_, attrExpire)

	entryCache_ := &entryCacheItem{
		ino:  *inode,
		mode: TypeDirectory,
	}
	m.putEntry(m.InoToPath(parent), *entryCache_, entryExpire)
	return syscall.F_OK
}

// Unlink removes a file entryMap from a directory.
// The file will be deleted if it's not linked by any entries and not open by any sessions.
func (m *kvMeta) Unlink(ctx *Context, parent Ino, name string) syscall.Errno {
	ino, _, err := m.Lookup(ctx, parent, name)
	if utils.IsError(err) {
		return err
	}
	path_ := m.InoToPath(ino)
	entryPath := m.InoToPath(parent)

	if err := m.defaultMeta.Unlink(ctx, parent, name); utils.IsError(err) {
		return err
	}
	m.removeAttr(path_)
	m.removeEntry(entryPath, name)
	return syscall.F_OK
}

// Rmdir removes an empty sub-directory.
func (m *kvMeta) Rmdir(ctx *Context, parent Ino, name string) syscall.Errno {
	ino, _, err := m.Lookup(ctx, parent, name)
	if utils.IsError(err) {
		return err
	}
	path_ := m.InoToPath(ino)
	entryPath := m.InoToPath(parent)

	if err := m.defaultMeta.Rmdir(ctx, parent, name); utils.IsError(err) {
		return err
	}
	m.removeAttr(path_)
	m.removeEntry(entryPath, name)
	return syscall.F_OK
}

// Rename move an entry from a source directory to another with given name.
// The targeted entry will be overwrited if it's a file or empty directory.
func (m *kvMeta) Rename(ctx *Context, parentSrc Ino, nameSrc string, parentDst Ino,
	nameDst string, flags uint32, inode *Ino, attr *Attr) syscall.Errno {

	oldIno, _, err := m.Lookup(ctx, parentSrc, nameSrc)
	if utils.IsError(err) {
		return err
	}
	parentSrcPath := m.InoToPath(parentSrc)
	oldPath := m.InoToPath(oldIno)

	if err := m.defaultMeta.Rename(ctx, parentSrc, nameSrc, parentDst, nameDst, flags, inode, attr); utils.IsError(err) {
		return err
	}
	now := time.Now()
	attrExpire := now.Add(m.attrTimeOut).Unix()
	entryExpire := now.Add(m.entryTimeOut).Unix()

	m.removeAttr(oldPath)
	m.removeEntry(parentSrcPath, nameSrc)

	attrCache_ := &attrCacheItem{attr: *attr}
	fullPath := m.InoToPath(*inode)
	m.putAttr(fullPath, *attrCache_, attrExpire)

	entryCache_ := &entryCacheItem{
		ino:  *inode,
		mode: attr.Mode,
	}
	m.putEntry(parentSrcPath, *entryCache_, entryExpire)
	return syscall.F_OK
}

// Link creates an entry for node.
func (m *kvMeta) Link(ctx *Context, inodeSrc, parent Ino, name string, attr *Attr) syscall.Errno {
	return m.defaultMeta.Link(ctx, inodeSrc, parent, name, attr)
}

// Readdir returns all entries for given directory, which include attributes if plus is true.
func (m *kvMeta) Readdir(ctx *Context, inode Ino, entries *[]*Entry) syscall.Errno {
	*entries = []*Entry{}
	ens, has := m.getEntries(m.InoToPath(inode))
	if has {
		for _, enRaw := range ens {
			entryCache_ := &entryCacheItem{}
			m.parseEntry(enRaw, entryCache_)
			fullPath := m.InoToPath(entryCache_.ino)
			en := &Entry{
				Ino:  entryCache_.ino,
				Name: filepath.Base(fullPath),
				Attr: &Attr{Mode: entryCache_.mode},
			}
			*entries = append(*entries, en)
		}
		return syscall.F_OK
	}

	err := m.defaultMeta.Readdir(ctx, inode, entries)
	now := time.Now()
	entryExpire := now.Add(m.entryTimeOut).Unix()

	if !utils.IsError(err) {
		var entriesCache []entryCacheItem
		for _, en := range *entries {
			entry := entryCacheItem{
				ino:  en.Ino,
				mode: en.Attr.Mode,
			}
			attrCache_ := &attrCacheItem{}
			attrCache_.attr = *en.Attr
			expire := time.Now().Add(m.attrTimeOut).Unix()
			m.putAttr(m.InoToPath(en.Ino), *attrCache_, expire)
			entriesCache = append(entriesCache, entry)
		}

		parentAttr := &Attr{}
		err = m.GetAttr(ctx, inode, parentAttr)
		if utils.IsError(err) {
			return err
		}
		parentEntryCache := entryCacheItem{
			ino:  inode,
			mode: parentAttr.Mode,
		}

		m.putEntries(parentEntryCache, entriesCache, entryExpire)
	}

	return err
}

// Create creates a file in a directory with given name.
func (m *kvMeta) Create(ctx *Context, parent Ino, name string, mode uint32, cumask uint16,
	flags uint32, inode *Ino, attr *Attr) (ufslib.UnderFileStorage, string, syscall.Errno) {

	ufs, newPath, err := m.defaultMeta.Create(ctx, parent, name, mode, cumask, flags, inode, attr)
	if utils.IsError(err) {
		return nil, "", err
	}
	now := time.Now()
	attrExpire := now.Add(m.attrTimeOut).Unix()
	entryExpire := now.Add(m.entryTimeOut).Unix()

	attrCache_ := &attrCacheItem{attr: *attr}

	fullPath := m.InoToPath(*inode)
	m.putAttr(fullPath, *attrCache_, attrExpire)

	entryCache_ := &entryCacheItem{
		ino:  *inode,
		mode: attr.Mode,
	}
	m.putEntry(m.InoToPath(parent), *entryCache_, entryExpire)
	return ufs, newPath, syscall.F_OK
}

// Open checks permission on a node and track it as open.
func (m *kvMeta) Open(ctx *Context, inode Ino, flags uint32, attr *Attr) (ufslib.UnderFileStorage, string, syscall.Errno) {
	attrCache_ := &attrCacheItem{}
	got := m.tryGetAttr(inode, attrCache_)
	if got {
		*attr = attrCache_.attr
		ufs, _, _, path := m.GetUFS(m.InoToPath(inode))
		return ufs, path, syscall.F_OK
	}
	ufs, path, err := m.defaultMeta.Open(ctx, inode, flags, attr)
	if utils.IsError(err) {
		return nil, "", err
	}
	now := time.Now()
	attrCache_.attr = *attr
	// cache ufs、path、 attr
	m.putAttr(m.InoToPath(inode), *attrCache_, now.Add(m.attrTimeOut).Unix())

	return ufs, path, syscall.F_OK
}

// Close a file.
func (m *kvMeta) Close(ctx *Context, inode Ino) syscall.Errno {
	return m.defaultMeta.Close(ctx, inode)
}

// Read returns the list of blocks
func (m *kvMeta) Read(ctx *Context, inode Ino, indx uint32, buf []byte) syscall.Errno {
	return syscall.ENOSYS
}

// Write put a slice of data on top of the given chunk.
func (m *kvMeta) Write(ctx *Context, inode Ino, off uint32, length int) syscall.Errno {
	err := m.defaultMeta.Write(ctx, inode, off, length)
	if utils.IsError(err) {
		return err
	}

	attrCache_ := &attrCacheItem{}
	if ok := m.tryGetAttr(inode, attrCache_); ok {
		now := time.Now()
		newLength := uint64(int(off) + length)
		if newLength > attrCache_.attr.Size {
			attrCache_.attr.Size = newLength
		}
		attrCache_.attr.Ctime = now.Unix()
		attrCache_.attr.Ctimensec = uint32(now.Nanosecond())
		attrCache_.attr.Mtime = now.Unix()
		attrCache_.attr.Mtimensec = uint32(now.Nanosecond())
		m.putAttr(m.InoToPath(inode), *attrCache_, now.Add(m.attrTimeOut).Unix())
	}

	return syscall.F_OK
}

// CopyFileRange copies part of a file to another one.
func (m *kvMeta) CopyFileRange(ctx *Context, fin Ino, offIn uint64, fout Ino, offOut uint64,
	size uint64, flags uint32, copied *uint64) syscall.Errno {
	return syscall.ENOSYS
}

// GetXattr returns the value of extended attribute for given name.
func (m *kvMeta) GetXattr(ctx *Context, inode Ino, attribute string, vbuff *[]byte) syscall.Errno {
	return m.defaultMeta.GetXattr(ctx, inode, attribute, vbuff)
}

// ListXattr returns all extended attributes of a node.
func (m *kvMeta) ListXattr(ctx *Context, inode Ino, dbuff *[]string) syscall.Errno {
	return m.defaultMeta.ListXattr(ctx, inode, dbuff)
}

// SetXattr update the extended attribute of a node.
func (m *kvMeta) SetXattr(ctx *Context, inode Ino, attribute string, value []byte, flags uint32) syscall.Errno {
	return m.defaultMeta.SetXattr(ctx, inode, attribute, value, flags)
}

// RemoveXattr removes the extended attribute of a node.
func (m *kvMeta) RemoveXattr(ctx *Context, inode Ino, name string) syscall.Errno {
	return m.defaultMeta.RemoveXattr(ctx, inode, name)
}

// Flock tries to put a lock on given file.
func (m *kvMeta) Flock(ctx *Context, inode Ino, owner uint64, ltype uint32, block bool) syscall.Errno {
	return m.defaultMeta.Flock(ctx, inode, owner, ltype, block)
}

// Getlk returns the current lock owner for a range on a file.
func (m *kvMeta) Getlk(ctx *Context, inode Ino, owner uint64, ltype *uint32, start, end *uint64, pid *uint32) syscall.Errno {
	return m.defaultMeta.Getlk(ctx, inode, owner, ltype, start, end, pid)
}

// Setlk sets a file range lock on given file.
func (m *kvMeta) Setlk(ctx *Context, inode Ino, owner uint64, block bool, ltype uint32, start, end uint64, pid uint32) syscall.Errno {
	return m.defaultMeta.Setlk(ctx, inode, owner, block, ltype, start, end, pid)
}

func (m *kvMeta) DumpMeta(w io.Writer) error {
	return syscall.ENOSYS
}

func (m *kvMeta) LoadMeta(r io.Reader) error {
	return syscall.ENOSYS
}

func (m *kvMeta) LinksMetaUpdateHandler(stopChan chan struct{}, interval int, linkMetaDirPrefix string) error {
	return m.defaultMeta.LinksMetaUpdateHandler(stopChan, interval, linkMetaDirPrefix)
}

func (m *kvMeta) parseAttr(buf []byte, attr *attrCacheItem) {
	if attr == nil {
		return
	}
	rb := utils.FromBuffer(buf)
	attr.attr.Type = rb.Get8()
	attr.attr.Mode = rb.Get32()
	attr.attr.Uid = rb.Get32()
	attr.attr.Gid = rb.Get32()
	attr.attr.Rdev = rb.Get64()
	attr.attr.Atime = int64(rb.Get64())
	attr.attr.Mtime = int64(rb.Get64())
	attr.attr.Ctime = int64(rb.Get64())
	attr.attr.Atimensec = rb.Get32()
	attr.attr.Mtimensec = rb.Get32()
	attr.attr.Ctimensec = rb.Get32()
	attr.attr.Nlink = rb.Get64()
	attr.attr.Size = rb.Get64()
	attr.attr.Blksize = int64(rb.Get64())
	attr.attr.Block = int64(rb.Get64())
	attr.expire = int64(rb.Get64())
}

func (m *kvMeta) marshalAttr(attr *attrCacheItem) []byte {
	w := utils.NewBuffer(attrCacheSize)
	w.Put8(attr.attr.Type)
	w.Put32(attr.attr.Mode)
	w.Put32(attr.attr.Uid)
	w.Put32(attr.attr.Gid)
	w.Put64(attr.attr.Rdev)
	w.Put64(uint64(attr.attr.Atime))
	w.Put64(uint64(attr.attr.Mtime))
	w.Put64(uint64(attr.attr.Ctime))
	w.Put32(attr.attr.Atimensec)
	w.Put32(attr.attr.Mtimensec)
	w.Put32(attr.attr.Ctimensec)
	w.Put64(attr.attr.Nlink)
	w.Put64(attr.attr.Size)
	w.Put64(uint64(attr.attr.Blksize))
	w.Put64(uint64(attr.attr.Block))
	w.Put64(uint64(attr.expire))
	return w.Bytes()
}

func (m *kvMeta) parseEntry(buf []byte, entry *entryCacheItem) {
	if entry == nil {
		return
	}
	rb := utils.FromBuffer(buf)
	entry.ino = Ino(rb.Get64())
	entry.mode = rb.Get32()
	entry.expire = int64(rb.Get64())
	entry.done = rb.Get8()
}

func (m *kvMeta) marshalEntry(attr *entryCacheItem) []byte {
	w := utils.NewBuffer(entryCacheSize)
	w.Put64(uint64(attr.ino))
	w.Put32(attr.mode)
	w.Put64(uint64(attr.expire))
	w.Put8(attr.done)
	return w.Bytes()
}

type Config struct {
	kv.Config
	AttrCacheExpire    time.Duration
	EntryCacheExpire   time.Duration
	AttrCacheSize      uint64
	EntryAttrCacheSize uint64
}
