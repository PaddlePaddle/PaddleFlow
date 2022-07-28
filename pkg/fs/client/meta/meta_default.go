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
	"encoding/json"
	"io"
	"os"
	pathlib "path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"

	apicommon "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	ufslib "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

const DefaultName = "default"
const DefaultRootPath = "/"
const DefaultLinkUpdateInterval = 15

type DefaultMeta struct {
	name        string
	defaultUfs  ufslib.UnderFileStorage
	ufsMap      *sync.Map
	ufsMapLock  sync.RWMutex
	ufsMapUT    int64
	inodeHandle *InodeHandle
	setOwner    bool
	uid         uint32
	gid         uint32
}

var _ Meta = &DefaultMeta{}

func InitDefaultMeta(fsMeta common.FSMeta, links map[string]common.FSMeta, inodeHandle *InodeHandle) (Meta, error) {
	meta := &DefaultMeta{
		name:        DefaultName,
		inodeHandle: inodeHandle,
	}
	ufs, err := newUFS(fsMeta)
	if err != nil {
		return nil, err
	}
	meta.defaultUfs = ufs
	err = meta.UpdateUFSMap(links)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (m *DefaultMeta) SetOwner(uid, gid uint32) {
	m.setOwner = true
	m.uid = uid
	m.gid = gid
}

// TODO: 可以将link对应路径的Ino缓存下来，通过Ino可以直接判断是否为link
func (m *DefaultMeta) GetUFS(name string) (ufslib.UnderFileStorage, bool, string, string) {
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

func (m *DefaultMeta) Name() string {
	return m.name
}

func (m *DefaultMeta) InoToPath(inode Ino) string {
	return m.inodeHandle.InoToPath(inode)
}

func (m *DefaultMeta) PathToIno(path string) Ino {
	return m.inodeHandle.PathToInode(path).inode
}

// StatFS returns summary statistics of a volume.
func (m *DefaultMeta) StatFS(ctx *Context) (*base.StatfsOut, syscall.Errno) {
	log.Debugf("defaultMeta, StatFs: name[%s]", DefaultRootPath)
	ufs := m.defaultUfs
	return ufs.StatFs(DefaultRootPath), syscall.F_OK
}

// Access checks the access permission on given inode.
func (m *DefaultMeta) Access(ctx *Context, inode Ino, mask uint32, attr *Attr) syscall.Errno {
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
		return syscall.Errno(fuse.EACCES)
	}
	return syscall.F_OK
}

// Lookup returns the inode and attributes for the given entry in a directory.
func (m *DefaultMeta) Lookup(ctx *Context, parent Ino, name string) (Ino, *Attr, syscall.Errno) {
	parentInode := m.inodeHandle.toInode(parent)
	if !parentInode.IsDir() {
		return 0, nil, syscall.ENOTDIR
	}
	attr := &Attr{}

	absolutePath := m.inodeHandle.ParentInodeToPath(parentInode, name)
	ufs, isLink, prefix, path := m.GetUFS(absolutePath)
	info, err := ufs.GetAttr(path)
	log.Debugf("Lookup GetAttr [%+v]", info)
	if err != nil {
		log.Debugf("[vfs-lookup] GetAttr failed: %v with path[%s] name[%s] and absolutePath[%s]", err, path, name, absolutePath)
		return 0, nil, utils.ToSyscallErrno(err)
	}
	if isLink {
		info.FixLinkPrefix(prefix)
	}
	attr.FromFileInfo(info)
	var inode Ino
	parentInode.RLock()
	if parentInode.child[name] == 0 {
		parentInode.RUnlock()
		newNode := parentInode.NewChild(name, info.IsDir)
		inode = newNode.inode
		parentInode.AddChild(name, newNode)
	} else {
		parentInode.RUnlock()
		inode = m.inodeHandle.toInode(parentInode.child[name]).inode
	}
	return inode, attr, syscall.F_OK
}

// Resolve fetches the inode and attributes for an entry identified by the given path.
// ENOTSUP will be returned if there's no natural implementation for this operation or
// if there are any symlink following involved.
func (m *DefaultMeta) Resolve(ctx *Context, parent Ino, path string, inode *Ino, attr *Attr) syscall.Errno {
	return syscall.ENOSYS
}

// GetAttr returns the attributes for given node.
func (m *DefaultMeta) GetAttr(ctx *Context, inode Ino, attr *Attr) syscall.Errno {
	name := m.inodeHandle.InoToPath(inode)
	return m.getAttr(name, attr)
}

func (m *DefaultMeta) getAttr(name string, attr *Attr) syscall.Errno {
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

// SetAttr updates the attributes for given node.
func (m *DefaultMeta) SetAttr(ctx *Context, inode Ino, set uint32, attr *Attr) syscall.Errno {
	name := m.inodeHandle.InoToPath(inode)
	ufs, _, _, path := m.GetUFS(name)
	log.Tracef("DefaultMeta SetAttr: name[%s] set[%d] attr:%+v", name, set, attr)
	if set&FATTR_UID != 0 || set&FATTR_GID != 0 {
		if err := ufs.Chown(path, attr.Uid, attr.Gid); err != nil {
			return utils.ToSyscallErrno(err)
		}
	}

	if set&FATTR_MODE != 0 {
		if err := ufs.Chmod(path, attr.Mode); err != nil {
			return utils.ToSyscallErrno(err)
		}
	}

	// s3未实现utimes函数，创建文件时存在报错：setting times of ‘xx’: Function not implemented。因此这里忽略enosys报错
	if set&FATTR_ATIME != 0 || set&FATTR_MTIME != 0 || set&FATTR_CTIME != 0 {
		atime := time.Unix(attr.Atime, int64(attr.Atimensec))
		ctime := time.Unix(attr.Ctime, int64(attr.Ctimensec))
		if err := ufs.Utimens(path, &atime, &ctime); err != nil && err != syscall.ENOSYS {
			return utils.ToSyscallErrno(err)
		}
	}
	if err := m.getAttr(name, attr); utils.IsError(err) {
		return err
	}
	return syscall.F_OK
}

// Truncate changes the length for given file.
func (m *DefaultMeta) Truncate(ctx *Context, inode Ino, size uint64) syscall.Errno {
	return syscall.F_OK
}

// Fallocate preallocate given space for given file.
func (m *DefaultMeta) Fallocate(ctx *Context, inode Ino, mode uint8, off uint64, size uint64) syscall.Errno {
	return syscall.ENOSYS
}

// ReadLink returns the target of a symlink.
func (m *DefaultMeta) ReadLink(ctx *Context, inode Ino, path *[]byte) syscall.Errno {
	return syscall.ENOSYS
}

// Symlink creates a symlink in a directory with given name.
func (m *DefaultMeta) Symlink(ctx *Context, parent Ino, name string, path string, inode *Ino, attr *Attr) syscall.Errno {
	return syscall.ENOSYS
}

// Mknod creates a node in a directory with given name, type and permissions.
// TODO: inode和attr是不是必须要往抛，目前是按照juicefs添加了inode和attr，后续根据vfs需求做删减
func (m *DefaultMeta) Mknod(ctx *Context, parent Ino, name string, mode uint32, rdev uint32, inode *Ino, attr *Attr) syscall.Errno {
	pnode := m.inodeHandle.toInode(parent)
	path := m.inodeHandle.ParentInodeToPath(pnode, name)
	ufs, _, _, newPath := m.GetUFS(path)
	if err := ufs.Mknod(newPath, mode, rdev); err != nil {
		return utils.ToSyscallErrno(err)
	}
	node := pnode.NewChild(name, true)
	*inode = node.inode
	// TODO: 添加link到inode的缓存后，这里可改为m.GetAttr
	if err := m.getAttr(path, attr); utils.IsError(err) {
		return err
	}
	return syscall.F_OK
}

// Mkdir creates a sub-directory with given name and mode.
func (m *DefaultMeta) Mkdir(ctx *Context, parent Ino, name string, mode uint32, inode *Ino, attr *Attr) syscall.Errno {
	pnode := m.inodeHandle.toInode(parent)
	path := m.inodeHandle.ParentInodeToPath(m.inodeHandle.toInode(parent), name)
	ufs, _, _, newPath := m.GetUFS(path)
	if err := ufs.Mkdir(newPath, mode); err != nil {
		return utils.ToSyscallErrno(err)
	}

	node := pnode.NewChild(name, true)
	*inode = node.inode
	// TODO: 添加link到inode的缓存后，这里可改为m.GetAttr
	if err := m.getAttr(path, attr); utils.IsError(err) {
		return err
	}
	return syscall.F_OK
}

// Unlink removes a file entry from a directory.
// The file will be deleted if it's not linked by any entries and not open by any sessions.
func (m *DefaultMeta) Unlink(ctx *Context, parent Ino, name string) syscall.Errno {
	pnode := m.inodeHandle.toInode(parent)
	path := m.inodeHandle.ParentInodeToPath(pnode, name)
	ufs, _, _, path := m.GetUFS(path)
	if err := ufs.Unlink(path); err != nil {
		return utils.ToSyscallErrno(err)
	}
	pnode.RmChild(name)
	return syscall.F_OK
}

// Rmdir removes an empty sub-directory.
func (m *DefaultMeta) Rmdir(ctx *Context, parent Ino, name string) syscall.Errno {
	pnode := m.inodeHandle.toInode(parent)
	path := m.inodeHandle.ParentInodeToPath(pnode, name)
	ufs, _, _, path := m.GetUFS(path)
	if err := ufs.Rmdir(path); err != nil {
		return utils.ToSyscallErrno(err)
	}
	pnode.RmChild(name)
	return syscall.F_OK
}

// Rename move an entry from a source directory to another with given name.
// The targeted entry will be overwrited if it's a file or empty directory.
func (m *DefaultMeta) Rename(ctx *Context, parentSrc Ino, nameSrc string, parentDst Ino,
	nameDst string, flags uint32, inode *Ino, attr *Attr) syscall.Errno {
	pnodeSrc := m.inodeHandle.toInode(parentSrc)
	pathSrc := m.inodeHandle.ParentInodeToPath(pnodeSrc, nameSrc)
	ufsSrc, _, _, newPathSrc := m.GetUFS(pathSrc)

	pnodeDst := m.inodeHandle.toInode(parentDst)
	pathDst := m.inodeHandle.ParentInodeToPath(pnodeDst, nameDst)
	ufsDst, _, _, newPathDst := m.GetUFS(pathDst)
	// todo：跨底层存储rename暂不支持
	if ufsSrc != ufsDst {
		log.Errorf("Rename between two ufs is not supported")
		return syscall.ENOSYS
	}

	if err := ufsSrc.Rename(newPathSrc, newPathDst); err != nil {
		return utils.ToSyscallErrno(err)
	}

	err := m.getAttr(pathDst, attr)
	if utils.IsError(err) {
		return err
	}

	pnodeSrc.RmChild(nameSrc)
	node := pnodeDst.NewChild(nameDst, attr.IsDir())
	*inode = node.inode
	return syscall.F_OK
}

// Link creates an entry for node.
func (m *DefaultMeta) Link(ctx *Context, inodeSrc, parent Ino, name string, attr *Attr) syscall.Errno {
	return syscall.ENOSYS
}

// Readdir returns all entries for given directory, which include attributes if plus is true.
func (m *DefaultMeta) Readdir(ctx *Context, inode Ino, entries *[]*Entry) syscall.Errno {
	name := m.inodeHandle.InoToPath(inode)
	ufs, isLink, _, path := m.GetUFS(name)
	dirs, err := ufs.ReadDir(path)
	parentInode := m.inodeHandle.toInode(inode)
	if err != nil {
		log.Errorf("[vfs] ReadDir failed: %v", err)
		return utils.ToSyscallErrno(err)
	}
	// 不存在link嵌套，因此只有非link情况下才需要填充link文件
	if !isLink {
		m.ufsMap.Range(func(key, value interface{}) bool {
			dir := pathlib.Dir(key.(string))
			linkUfs := value.(ufslib.UnderFileStorage)
			if pathlib.Clean("/"+name) == pathlib.Clean("/"+dir) {
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
	for _, dir := range dirs {
		entry := &Entry{
			Ino:  m.PathToIno(dir.Name),
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

		isDir := false
		if dir.Attr.Type == TypeDirectory {
			isDir = true
		}
		var newInode Ino
		parentInode.RLock()
		if parentInode.child[dir.Name] == 0 {
			parentInode.RUnlock()
			newNode := parentInode.NewChild(dir.Name, isDir)
			newInode = newNode.inode
			parentInode.AddChild(dir.Name, newNode)
		} else {
			parentInode.RUnlock()
			newInode = m.inodeHandle.toInode(parentInode.child[dir.Name]).inode
		}
		entry.Ino = newInode
		*entries = append(*entries, entry)
	}
	return syscall.F_OK
}

// Create creates a file in a directory with given name.
func (m *DefaultMeta) Create(ctx *Context, parent Ino, name string, mode uint32, cumask uint16,
	flags uint32, inode *Ino, attr *Attr) (ufslib.UnderFileStorage, string, syscall.Errno) {
	pnode := m.inodeHandle.toInode(parent)
	path := m.inodeHandle.ParentInodeToPath(pnode, name)
	ufs, _, _, newPath := m.GetUFS(path)
	fh, err := ufs.Create(newPath, flags, mode)
	if err != nil {
		log.Errorf("Create: name[%s], flags[%d], mode[%d], failed: [%v]",
			name, flags, mode, err)
		return nil, newPath, utils.ToSyscallErrno(err)
	}
	defer fh.Release()
	node := pnode.NewChild(name, false)
	*inode = node.inode
	errGetAttr := m.getAttr(path, attr)
	if utils.IsError(errGetAttr) {
		return nil, newPath, errGetAttr
	}
	return ufs, newPath, syscall.F_OK
}

// Open checks permission on a node and track it as open.
func (m *DefaultMeta) Open(ctx *Context, inode Ino, flags uint32, attr *Attr) (ufslib.UnderFileStorage, string, syscall.Errno) {
	name := m.InoToPath(inode)
	err := m.getAttr(name, attr)
	if utils.IsError(err) {
		return nil, "", err
	}
	ufs, _, _, path := m.GetUFS(name)
	return ufs, path, syscall.F_OK
}

// Close a file.
func (m *DefaultMeta) Close(ctx *Context, inode Ino) syscall.Errno {
	return syscall.ENOSYS
}

// Read returns the list of blocks
func (m *DefaultMeta) Read(ctx *Context, inode Ino, indx uint32, buf []byte) syscall.Errno {
	return syscall.ENOSYS
}

// Write put a slice of data on top of the given chunk.
func (m *DefaultMeta) Write(ctx *Context, inode Ino, off uint32, length int) syscall.Errno {
	return syscall.F_OK
}

// CopyFileRange copies part of a file to another one.
func (m *DefaultMeta) CopyFileRange(ctx *Context, fin Ino, offIn uint64, fout Ino, offOut uint64,
	size uint64, flags uint32, copied *uint64) syscall.Errno {
	return syscall.ENOSYS
}

// GetXattr returns the value of extended attribute for given name.
func (m *DefaultMeta) GetXattr(ctx *Context, inode Ino, attribute string, vbuff *[]byte) syscall.Errno {
	name := m.inodeHandle.InoToPath(inode)
	ufs, _, _, path := m.GetUFS(name)
	if data, err := ufs.GetXAttr(path, attribute); err != nil {
		return utils.ToSyscallErrno(err)
	} else {
		*vbuff = data
	}
	return syscall.F_OK
}

// ListXattr returns all extended attributes of a node.
func (m *DefaultMeta) ListXattr(ctx *Context, inode Ino, dbuff *[]string) syscall.Errno {
	name := m.inodeHandle.InoToPath(inode)
	ufs, _, _, path := m.GetUFS(name)
	if data, err := ufs.ListXAttr(path); err != nil {
		return utils.ToSyscallErrno(err)
	} else {
		*dbuff = data
	}
	return syscall.F_OK
}

// SetXattr update the extended attribute of a node.
func (m *DefaultMeta) SetXattr(ctx *Context, inode Ino, attribute string, value []byte, flags uint32) syscall.Errno {
	name := m.inodeHandle.InoToPath(inode)
	ufs, _, _, path := m.GetUFS(name)
	if err := ufs.SetXAttr(path, attribute, value, int(flags)); err != nil {
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

// RemoveXattr removes the extended attribute of a node.
func (m *DefaultMeta) RemoveXattr(ctx *Context, inode Ino, attribute string) syscall.Errno {
	name := m.inodeHandle.InoToPath(inode)
	ufs, _, _, path := m.GetUFS(name)
	if err := ufs.RemoveXAttr(path, attribute); err != nil {
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

// Flock tries to put a lock on given file.
func (m *DefaultMeta) Flock(ctx *Context, inode Ino, owner uint64, ltype uint32, block bool) syscall.Errno {
	return syscall.ENOSYS
}

// Getlk returns the current lock owner for a range on a file.
func (m *DefaultMeta) Getlk(ctx *Context, inode Ino, owner uint64, ltype *uint32, start, end *uint64, pid *uint32) syscall.Errno {
	return syscall.ENOSYS
}

// Setlk sets a file range lock on given file.
func (m *DefaultMeta) Setlk(ctx *Context, inode Ino, owner uint64, block bool, ltype uint32, start, end uint64, pid uint32) syscall.Errno {
	return syscall.ENOSYS
}

func (m *DefaultMeta) DumpMeta(w io.Writer) error {
	return syscall.ENOSYS
}

func (m *DefaultMeta) LoadMeta(r io.Reader) error {
	return syscall.ENOSYS
}

func (m *DefaultMeta) LinksMetaUpdateHandler(stopChan chan struct{}, interval int, linkMetaDirPrefix string) error {
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

func (m *DefaultMeta) linksMetaUpdate(linkMetaDirPrefix string) error {
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
	fileHandle, err := m.defaultUfs.Open(filePath, flags)
	if err != nil {
		log.Errorf("open file[%s] failed: %v", filePath, err)
		return err
	}
	buf := make([]byte, attr.Size)
	r, status := fileHandle.Read(buf, 0)
	if utils.IsError(syscall.Errno(status)) {
		log.Errorf("fileHandle Read err[%v]", syscall.Errno(status))
		return syscall.Errno(status)
	}
	content, status := r.Bytes(buf)
	if utils.IsError(syscall.Errno(status)) {
		log.Errorf("r.Bytes err[%v]", syscall.Errno(status))
		return syscall.Errno(status)
	}

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

func (m *DefaultMeta) UpdateUFSMap(fsMetas map[string]common.FSMeta) error {
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
