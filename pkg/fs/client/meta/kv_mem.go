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

package meta

import (
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"paddleflow/pkg/fs/client/base"
	ufslib "paddleflow/pkg/fs/client/ufs"
	"paddleflow/pkg/fs/client/utils"
)

const MemMetaName = "mem"

type memAttrCache struct {
	attr   Attr
	refs   int // open refs
	ufs    *ufslib.UnderFileStorage
	path   string
	expire time.Time
	sync.RWMutex
}

type memEntryCache struct {
	ino    Ino
	name   string
	mode   uint32
	expire time.Time
	sync.RWMutex
}

// entryMap used to list directory or findEntry by parent inode and name
type entryMap struct {
	children map[string]*memEntryCache
	// if entryDone is true, means the children can be used readdir cache， or the children data may be missing
	entryDone    bool
	childrenLock sync.RWMutex
	expire       time.Time
}

// memCache store file attr in memory
type memCache struct {
	attrs       map[Ino]*memAttrCache
	attrLock    sync.RWMutex
	attrTimeOut time.Duration

	entries      map[Ino]*entryMap
	entryLock    sync.RWMutex
	entryTimeOut time.Duration
}

func newMemCache(attrTimeOut, entryTimeOut time.Duration) *memCache {
	mc := &memCache{
		attrs:        make(map[Ino]*memAttrCache),
		entries:      make(map[Ino]*entryMap),
		attrTimeOut:  attrTimeOut,
		entryTimeOut: entryTimeOut,
	}
	go mc.cleanupCache()
	return mc
}

func (mc *memCache) cleanupCache() {
	for {
		now := time.Now()
		cnt := 0
		mc.attrLock.Lock()
		for inode, attr := range mc.attrs {
			if now.After(attr.expire) {
				delete(mc.attrs, inode)
			}
			cnt++
			if cnt > 1000 {
				break
			}
		}
		mc.attrLock.Unlock()
		cnt = 0

		mc.entryLock.Lock()
	OUTER:
		for inode, es := range mc.entries {
			es.childrenLock.Lock()
			for name, child := range es.children {
				if now.After(child.expire) {
					delete(es.children, name)
					es.entryDone = false
					if len(es.children) == 0 {
						delete(mc.entries, inode)
					}
				}
				cnt++
				if cnt > 1000 {
					break OUTER
				}
			}
			es.childrenLock.Unlock()
		}
		cnt = 0
		mc.entryLock.Unlock()
		// todo:: sleep time config need verification
		time.Sleep(2 * time.Second)
	}
}

func (mc *memCache) TryGetAttr(ino Ino, attr *Attr) bool {
	mc.attrLock.RLock()
	a, ok := mc.attrs[ino]
	mc.attrLock.RUnlock()
	if ok && time.Since(a.expire) < 0 {
		if attr != nil {
			*attr = a.attr
		}
		return true
	}

	return false
}

// Put inode to cache, if ino already in cache, update
func (mc *memCache) PutAttr(ino Ino, attr *Attr) {
	mc.attrLock.RLock()
	a, ok := mc.attrs[ino]
	mc.attrLock.RUnlock()
	if ok {
		a.Lock()
		a.attr = *attr
		a.expire = time.Now().Add(mc.attrTimeOut)
		a.Unlock()
		return
	}

	p := &memAttrCache{
		attr:   *attr,
		expire: time.Now().Add(mc.attrTimeOut),
	}
	mc.attrLock.Lock()
	mc.attrs[ino] = p
	mc.attrLock.Unlock()
}

func (mc *memCache) PutEntry(parent Ino, name string, ino Ino, mode uint32) {
	mc.entryLock.RLock()
	p, ok := mc.entries[parent]
	mc.entryLock.RUnlock()
	now := time.Now()
	if !ok {
		childs := make(map[string]*memEntryCache)
		childs[name] = &memEntryCache{
			ino:    ino,
			name:   name,
			mode:   mode,
			expire: now.Add(mc.entryTimeOut),
		}
		_entry := &entryMap{
			children: childs,
		}
		mc.entryLock.Lock()
		mc.entries[parent] = _entry
		mc.entryLock.Unlock()
		return
	}

	p.childrenLock.RLock()
	e, ok := p.children[name]
	p.childrenLock.RUnlock()
	if ok {
		e.Lock()
		e.ino = ino
		e.mode = mode
		e.expire = now.Add(mc.entryTimeOut)
		e.Unlock()
		return
	}
	mc.entryLock.Lock()
	p.childrenLock.Lock()
	p.children[name] = &memEntryCache{
		ino:    ino,
		name:   name,
		mode:   mode,
		expire: now.Add(mc.entryTimeOut),
	}
	p.childrenLock.Unlock()
	mc.entryLock.Unlock()
}

func (mc *memCache) PutEntries(parent Ino, entries *entryMap) {
	mc.entryLock.Lock()
	mc.entries[parent] = entries
	mc.entryLock.Unlock()
}

func (mc *memCache) FindEntry(parent Ino, name string) *memEntryCache {
	mc.entryLock.RLock()
	p, ok := mc.entries[parent]
	mc.entryLock.RUnlock()
	if !ok {
		return nil
	}

	p.childrenLock.RLock()
	e, ok := p.children[name]
	p.childrenLock.RUnlock()
	if ok && time.Since(e.expire) < 0 {
		return e
	}
	return nil
}

func (mc *memCache) GetEntries(parent Ino) *entryMap {
	mc.entryLock.RLock()
	p, ok := mc.entries[parent]
	mc.entryLock.RUnlock()
	if ok {
		return p
	}
	return nil
}

func (mc *memCache) RemoveEntry(parent Ino, name string) {
	mc.entryLock.RLock()
	p, ok := mc.entries[parent]
	mc.entryLock.RUnlock()
	if !ok {
		return
	}
	p.childrenLock.RLock()
	e, ok := p.children[name]
	p.childrenLock.RUnlock()
	if ok {
		p.childrenLock.Lock()
		delete(p.children, name)
		p.childrenLock.Unlock()
	}

	// delete child e.ino
	if utils.StatModeToFileMode(int(e.mode)).IsDir() {
		mc.entryLock.Lock()
		delete(mc.entries, e.ino)
		mc.entryLock.Unlock()
	}
}

func (mc *memCache) OpenCheck(ino Ino) *memAttrCache {
	mc.attrLock.RLock()
	a, ok := mc.attrs[ino]
	mc.attrLock.RUnlock()
	if ok {
		if ok && time.Since(a.expire) < 0 {
			if a.ufs != nil && a.path != "" {
				return a
			}
		}
	}
	return nil
}

// Open file
func (mc *memCache) Open(ino Ino, ufs *ufslib.UnderFileStorage, path string, attr *Attr) {
	mc.attrLock.RLock()
	a, ok := mc.attrs[ino]
	mc.attrLock.RUnlock()

	if !ok {
		a = &memAttrCache{}
	}
	a.Lock()
	defer a.Unlock()

	if attr != nil {
		a.attr = *attr
	}
	if ufs != nil {
		a.ufs = ufs
	}

	a.path = path
	a.expire = time.Now().Add(mc.attrTimeOut)
	a.refs++
}

// Close reduce refs
func (mc *memCache) Close(ino Ino) {
	mc.attrLock.RLock()
	delete(mc.attrs, ino)
	mc.attrLock.RUnlock()
}

func (mc *memCache) RemoveAttr(ino Ino) {
	mc.attrLock.Lock()
	delete(mc.attrs, ino)
	mc.attrLock.Unlock()
}

type MemMeta struct {
	d  Meta
	mc *memCache
}

func NewMemMeta(meta Meta, config Config) (Meta, error) {
	mc := newMemCache(config.AttrCacheExpire, config.EntryCacheExpire)

	m := MemMeta{
		d:  meta,
		mc: mc,
	}
	return &m, nil
}

var _ Meta = &MemMeta{}

func (m *MemMeta) SetOwner(uid, gid uint32) {
	m.d.SetOwner(uid, gid)
}

func (m *MemMeta) GetUFS(name string) (ufslib.UnderFileStorage, bool, string, string) {
	return m.d.GetUFS(name)
}

func (m *MemMeta) Name() string {
	return "memcache"
}

func (m *MemMeta) InoToPath(inode Ino) string {
	return m.d.InoToPath(inode)
}

func (m *MemMeta) PathToIno(path string) Ino {
	return m.d.PathToIno(path)
}

// StatFS returns summary statistics of a volume.
func (m *MemMeta) StatFS(ctx *Context) (*base.StatfsOut, syscall.Errno) {
	return m.d.StatFS(ctx)
}

// Access checks the access permission on given inode.
func (m *MemMeta) Access(ctx *Context, inode Ino, mask uint32, attr *Attr) syscall.Errno {
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
func (m *MemMeta) Lookup(ctx *Context, parent Ino, name string) (Ino, *Attr, syscall.Errno) {
	en := m.mc.FindEntry(parent, name)
	if en != nil && en.ino > 0 {
		attr := &Attr{}
		if m.mc.TryGetAttr(en.ino, attr) {
			return en.ino, attr, syscall.F_OK
		}
	}

	ino, attr, err := m.d.Lookup(ctx, parent, name)

	if utils.IsError(err) {
		return ino, attr, err
	}

	m.mc.PutAttr(ino, attr)
	m.mc.PutEntry(parent, name, ino, attr.Mode)
	return ino, attr, err
}

// Resolve fetches the inode and attributes for an entry identified by the given path.
// ENOTSUP will be returned if there's no natural implementation for this operation or
// if there are any symlink following involved.
func (m *MemMeta) Resolve(ctx *Context, parent Ino, path string, inode *Ino, attr *Attr) syscall.Errno {
	return m.d.Resolve(ctx, parent, path, inode, attr)
}

// GetAttr returns the attributes for given node.
func (m *MemMeta) GetAttr(ctx *Context, inode Ino, attr *Attr) syscall.Errno {
	if ok := m.mc.TryGetAttr(inode, attr); ok {
		return syscall.F_OK
	}

	if err := m.doGetAttr(ctx, inode, attr); utils.IsError(err) {
		return err
	}

	m.mc.PutAttr(inode, attr)
	return syscall.F_OK
}

func (m *MemMeta) doGetAttr(ctx *Context, inode Ino, attr *Attr) syscall.Errno {
	return m.d.GetAttr(ctx, inode, attr)
}

// SetAttr updates the attributes for given node.
func (m *MemMeta) SetAttr(ctx *Context, inode Ino, set uint32, attr *Attr) syscall.Errno {
	if err := m.d.SetAttr(ctx, inode, set, attr); utils.IsError(err) {
		return err
	}

	m.mc.PutAttr(inode, attr)
	return syscall.F_OK
}

// Truncate changes the length for given file.
func (m *MemMeta) Truncate(ctx *Context, inode Ino, size uint64) syscall.Errno {
	if err := m.d.Truncate(ctx, inode, size); utils.IsError(err) {
		return err
	}

	attr := &Attr{}
	if err := m.doGetAttr(ctx, inode, attr); utils.IsError(err) {
		return err
	}
	m.mc.PutAttr(inode, attr)
	return syscall.F_OK
}

// Fallocate preallocate given space for given file.
func (m *MemMeta) Fallocate(ctx *Context, inode Ino, mode uint8, off uint64, size uint64) syscall.Errno {
	return m.d.Fallocate(ctx, initDirSize, mode, off, size)
}

// ReadLink returns the target of a symlink.
func (m *MemMeta) ReadLink(ctx *Context, inode Ino, path *[]byte) syscall.Errno {
	return m.d.ReadLink(ctx, inode, path)
}

// Symlink creates a symlink in a directory with given name.
func (m *MemMeta) Symlink(ctx *Context, parent Ino, name string, path string, inode *Ino, attr *Attr) syscall.Errno {
	return m.d.Symlink(ctx, parent, name, path, inode, attr)
}

// Mknod creates a node in a directory with given name, type and permissions.
// TODO: inode和attr是不是必须要往抛，目前是按照juicefs添加了inode和attr，后续根据vfs需求做删减
func (m *MemMeta) Mknod(ctx *Context, parent Ino, name string, mode uint32, rdev uint32, inode *Ino, attr *Attr) syscall.Errno {
	if err := m.d.Mknod(ctx, parent, name, mode, rdev, inode, attr); utils.IsError(err) {
		return err
	}

	m.mc.PutAttr(*inode, attr)
	m.mc.PutEntry(parent, name, *inode, attr.Mode)
	return syscall.F_OK
}

// Mkdir creates a sub-directory with given name and mode.
func (m *MemMeta) Mkdir(ctx *Context, parent Ino, name string, mode uint32, inode *Ino, attr *Attr) syscall.Errno {
	if err := m.d.Mkdir(ctx, parent, name, mode, inode, attr); utils.IsError(err) {
		return err
	}

	m.mc.PutAttr(*inode, attr)
	m.mc.PutEntry(parent, name, *inode, TypeDirectory)
	return syscall.F_OK
}

// Unlink removes a file entryMap from a directory.
// The file will be deleted if it's not linked by any entries and not open by any sessions.
func (m *MemMeta) Unlink(ctx *Context, parent Ino, name string) syscall.Errno {
	ino, _, err := m.Lookup(ctx, parent, name)
	if utils.IsError(err) {
		return err
	}

	if err := m.d.Unlink(ctx, parent, name); utils.IsError(err) {
		return err
	}
	m.mc.RemoveAttr(ino)
	m.mc.RemoveEntry(parent, name)
	return syscall.F_OK
}

// Rmdir removes an empty sub-directory.
func (m *MemMeta) Rmdir(ctx *Context, parent Ino, name string) syscall.Errno {
	ino, _, err := m.Lookup(ctx, parent, name)
	if utils.IsError(err) {
		return err
	}

	if err := m.d.Rmdir(ctx, parent, name); utils.IsError(err) {
		return err
	}
	m.mc.RemoveAttr(ino)
	m.mc.RemoveEntry(parent, name)
	return syscall.F_OK
}

// Rename move an entry from a source directory to another with given name.
// The targeted entry will be overwrited if it's a file or empty directory.
func (m *MemMeta) Rename(ctx *Context, parentSrc Ino, nameSrc string, parentDst Ino,
	nameDst string, flags uint32, inode *Ino, attr *Attr) syscall.Errno {

	oldIno, _, err := m.Lookup(ctx, parentSrc, nameSrc)
	if utils.IsError(err) {
		return err
	}

	if err := m.d.Rename(ctx, parentSrc, nameSrc, parentDst, nameDst, flags, inode, attr); utils.IsError(err) {
		return err
	}

	m.mc.RemoveAttr(oldIno)
	m.mc.RemoveEntry(parentSrc, nameSrc)
	m.mc.PutAttr(*inode, attr)
	m.mc.PutEntry(parentDst, nameDst, *inode, attr.Mode)
	return syscall.F_OK
}

// Link creates an entry for node.
func (m *MemMeta) Link(ctx *Context, inodeSrc, parent Ino, name string, attr *Attr) syscall.Errno {
	return m.d.Link(ctx, inodeSrc, parent, name, attr)
}

// Readdir returns all entries for given directory, which include attributes if plus is true.
func (m *MemMeta) Readdir(ctx *Context, inode Ino, entries *[]*Entry) syscall.Errno {
	*entries = []*Entry{}
	ens := m.mc.GetEntries(inode)
	if ens != nil && ens.entryDone == true && time.Since(ens.expire) < 0 && m.mc.TryGetAttr(inode, nil) {
		ens.childrenLock.RLock()
		for _, en := range ens.children {
			entry := &Entry{
				Ino:  en.ino,
				Name: en.name,
				Attr: &Attr{Mode: en.mode},
			}
			*entries = append(*entries, entry)
		}
		ens.childrenLock.RUnlock()
		return syscall.F_OK
	}

	err := m.d.Readdir(ctx, inode, entries)

	if !utils.IsError(err) {
		children := make(map[string]*memEntryCache, len(*entries))
		for _, en := range *entries {
			children[en.Name] = &memEntryCache{
				name:   en.Name,
				mode:   en.Attr.Mode,
				expire: time.Now().Add(m.mc.entryTimeOut),
			}
		}
		_entry := &entryMap{children: children, entryDone: true, expire: time.Now().Add(m.mc.entryTimeOut)}

		m.mc.PutEntries(inode, _entry)
		attr := &Attr{}
		m.GetAttr(ctx, inode, attr)
	}

	return err
}

// Create creates a file in a directory with given name.
func (m *MemMeta) Create(ctx *Context, parent Ino, name string, mode uint32, cumask uint16,
	flags uint32, inode *Ino, attr *Attr) (ufslib.UnderFileStorage, string, syscall.Errno) {

	ufs, newPath, err := m.d.Create(ctx, parent, name, mode, cumask, flags, inode, attr)
	if utils.IsError(err) {
		return nil, "", err
	}
	m.mc.Open(*inode, &ufs, newPath, attr)
	m.mc.PutEntry(parent, name, *inode, attr.Mode)
	return ufs, newPath, syscall.F_OK
}

// Open checks permission on a node and track it as open.
func (m *MemMeta) Open(ctx *Context, inode Ino, flags uint32, attr *Attr) (ufslib.UnderFileStorage, string, syscall.Errno) {
	a := m.mc.OpenCheck(inode)
	if a != nil {
		*attr = a.attr
		return *a.ufs, a.path, syscall.F_OK
	}
	ufs, path, err := m.d.Open(ctx, inode, flags, attr)
	if utils.IsError(err) {
		return nil, "", err
	}
	// cache ufs、path、 attr
	m.mc.Open(inode, &ufs, path, attr)

	return ufs, path, syscall.F_OK
}

// Close a file.
func (m *MemMeta) Close(ctx *Context, inode Ino) syscall.Errno {
	m.mc.Close(inode)
	return m.d.Close(ctx, inode)
}

// Read returns the list of blocks
func (m *MemMeta) Read(ctx *Context, inode Ino, indx uint32, buf []byte) syscall.Errno {
	return syscall.ENOSYS
}

// Write put a slice of data on top of the given chunk.
func (m *MemMeta) Write(ctx *Context, inode Ino, off uint32, length int) syscall.Errno {
	err := m.d.Write(ctx, inode, off, length)
	if utils.IsError(err) {
		return err
	}

	attr := &Attr{}
	if ok := m.mc.TryGetAttr(inode, attr); ok {
		newLength := uint64(int(off) + length)
		if newLength > attr.Size {
			attr.Size = newLength
		}
		now := time.Now()
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())
		attr.Mtime = now.Unix()
		attr.Mtimensec = uint32(now.Nanosecond())
		m.mc.PutAttr(inode, attr)
	}
	return syscall.F_OK
}

// CopyFileRange copies part of a file to another one.
func (m *MemMeta) CopyFileRange(ctx *Context, fin Ino, offIn uint64, fout Ino, offOut uint64,
	size uint64, flags uint32, copied *uint64) syscall.Errno {
	return syscall.ENOSYS
}

// GetXattr returns the value of extended attribute for given name.
func (m *MemMeta) GetXattr(ctx *Context, inode Ino, attribute string, vbuff *[]byte) syscall.Errno {
	return m.d.GetXattr(ctx, inode, attribute, vbuff)
}

// ListXattr returns all extended attributes of a node.
func (m *MemMeta) ListXattr(ctx *Context, inode Ino, dbuff *[]string) syscall.Errno {
	return m.d.ListXattr(ctx, inode, dbuff)
}

// SetXattr update the extended attribute of a node.
func (m *MemMeta) SetXattr(ctx *Context, inode Ino, attribute string, value []byte, flags uint32) syscall.Errno {
	return m.d.SetXattr(ctx, inode, attribute, value, flags)
}

// RemoveXattr removes the extended attribute of a node.
func (m *MemMeta) RemoveXattr(ctx *Context, inode Ino, name string) syscall.Errno {
	return m.d.RemoveXattr(ctx, inode, name)
}

// Flock tries to put a lock on given file.
func (m *MemMeta) Flock(ctx *Context, inode Ino, owner uint64, ltype uint32, block bool) syscall.Errno {
	return m.d.Flock(ctx, inode, owner, ltype, block)
}

// Getlk returns the current lock owner for a range on a file.
func (m *MemMeta) Getlk(ctx *Context, inode Ino, owner uint64, ltype *uint32, start, end *uint64, pid *uint32) syscall.Errno {
	return m.d.Getlk(ctx, inode, owner, ltype, start, end, pid)
}

// Setlk sets a file range lock on given file.
func (m *MemMeta) Setlk(ctx *Context, inode Ino, owner uint64, block bool, ltype uint32, start, end uint64, pid uint32) syscall.Errno {
	return m.d.Setlk(ctx, inode, owner, block, ltype, start, end, pid)
}

func (m *MemMeta) DumpMeta(w io.Writer) error {
	return syscall.ENOSYS
}

func (m *MemMeta) LoadMeta(r io.Reader) error {
	return syscall.ENOSYS
}

func (m *MemMeta) LinksMetaUpdateHandler(stopChan chan struct{}, interval int, linkMetaDirPrefix string) error {
	return m.d.LinksMetaUpdateHandler(stopChan, interval, linkMetaDirPrefix)
}
