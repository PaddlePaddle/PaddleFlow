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
	"encoding/json"
	"os"
	pathlib "path"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	apicommon "paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/meta"
	ufslib "paddleflow/pkg/fs/client/ufs"
	"paddleflow/pkg/fs/client/utils"
)

type OldVFS struct {
	fsMeta base.FSMeta
	// TODO: 是否需要考虑并发
	defaultUfs ufslib.UnderFileStorage
	links      []base.FSMeta
	ufsMap     *ufsMap
	ufsMapLock sync.RWMutex
	ufsMapUT   int64
	reader     DataReader
	writer     DataWriter
	handleMap  map[Ino][]*handle
	handleLock sync.Mutex
	nextfh     uint64
	meta       meta.Meta
}

var (
	oldVfsop *OldVFS
)

func InitOldVFS(fsMeta base.FSMeta, links map[string]base.FSMeta, global bool) (*OldVFS, error) {
	vfs := &OldVFS{
		fsMeta: fsMeta,
	}
	ufs, err := vfs.newUFS(fsMeta)
	if err != nil {
		log.Errorf("init ufs failed: %v", err)
		return nil, err
	}
	vfs.defaultUfs = ufs
	err = vfs.UpdateUFSMap(links)
	if err != nil {
		log.Errorf("init link ufs maps failed: %v", err)
		return nil, err
	}
	// todo: 传递meta信息，可以做缓存
	vfs.ufsMapLock.RLock()
	defer vfs.ufsMapLock.RUnlock()
	vfs.handleMap = make(map[Ino][]*handle)
	vfs.nextfh = 1
	inodeHandle := meta.NewInodeHandle()
	vfs.meta, err = meta.NewDefaultMeta(fsMeta, links, inodeHandle)
	if err != nil {
		log.Errorf("NewDefaultMeta failed: %v", err)
		return nil, err
	}
	if global {
		oldVfsop = vfs
	}
	return vfs, nil
}

func (v *OldVFS) LinksMetaUpdateHandler(stopChan chan struct{}, interval int, linkMetaDirPrefix string) error {
	for {
		v.linksMetaUpdate(linkMetaDirPrefix)
		select {
		case <-stopChan:
			log.Errorf("links meta update handler stopped")
			return nil
		default:
			time.Sleep(time.Duration(interval) * time.Second)
		}
	}
}

func (v *OldVFS) linksMetaUpdate(linkMetaDirPrefix string) error {
	filePath := pathlib.Join(linkMetaDirPrefix, base.LinkMetaDir, base.LinkMetaFile)
	info, err := v.defaultUfs.GetAttr(filePath)
	if err != nil {
		log.Errorf("GetAttr file[%s] failed: %v", filePath, err)
		return err
	}
	attr := &Attr{}
	attr.FromFileInfo(info)
	if attr.Mtime <= v.ufsMapUT {
		return nil
	}

	flags := uint32(syscall.O_RDONLY)
	fileHandle, err := v.defaultUfs.Open(filePath, flags)
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

	decodedLinksMeta, err := apicommon.AesDecrypt(string(content), apicommon.AESEncryptKey)
	if err != nil {
		log.Errorf("aes decrypt links meta json string err[%v]", err)
		return err
	}

	var result map[string]base.FSMeta
	if err := json.Unmarshal([]byte(decodedLinksMeta), &result); err != nil {
		log.Errorf("json unmarshal links meta err[%v]", err)
		return err
	}

	if err := v.UpdateUFSMap(result); err != nil {
		log.Errorf("update ufs map err[%v]", err)
		return err
	}
	v.ufsMapUT = attr.Mtime
	return nil
}

func (v *OldVFS) UpdateUFSMap(fsMetas map[string]base.FSMeta) error {
	var ufsMap sync.Map
	for key, value := range fsMetas {
		linkUfs, err := v.newUFS(value)
		if err != nil {
			log.Errorf("new ufs for fsMeta[%+v] failed: %v", value, err)
			return err
		}
		ufsMap.Store(pathlib.Clean(key), linkUfs)
	}
	v.ufsMapLock.Lock()
	defer v.ufsMapLock.Unlock()
	v.ufsMap = &ufsMap
	return nil
}

func (v *OldVFS) newUFS(fsMeta base.FSMeta) (ufslib.UnderFileStorage, error) {
	log.Debugf("begin to new UFS: fsMeta[%+v]", fsMeta)
	properties := make(map[string]interface{})
	for k, v := range fsMeta.Properties {
		properties[k] = v
	}
	properties[base.SubPath] = fsMeta.SubPath

	switch fsMeta.UfsType {
	case base.HDFSType:
		properties[base.NameNodeAddress] = fsMeta.ServerAddress
	case base.HDFSWithKerberosType:
		properties[base.NameNodeAddress] = fsMeta.ServerAddress
	case base.SFTPType:
		properties[base.Address] = fsMeta.ServerAddress
	}
	return ufslib.NewUFS(fsMeta.UfsType, properties)
}

func GetOldVFS() *OldVFS {
	if oldVfsop == nil {
		log.Errorf("vfs is not initialized")
		os.Exit(-1)
	}
	return oldVfsop
}

func (v *OldVFS) getUFS(name string) (ufslib.UnderFileStorage, bool, string, string) {
	return v.meta.GetUFS(name)
}

func (v *OldVFS) GetAttr(name string, context *base.Context) (*base.FileInfo, syscall.Errno) {
	log.Debugf("GetAttr: name [%s]", name)
	ufs, isLink, prefix, path := v.getUFS(name)
	attr, err := ufs.GetAttr(path)
	if err != nil {
		log.Errorf("[vfs] GetAttr failed: %v", err)
		return nil, utils.ToSyscallErrno(err)
	}
	log.Debugf("before fix: the attr mode is [%d]", attr.Mode)
	if isLink {
		attr.FixLinkPrefix(prefix)
	}
	log.Debugf("after fix: the attr mode is [%d]", attr.Mode)

	return attr, syscall.F_OK
}

// These should update the file's ctime too.
func (v *OldVFS) Chmod(name string, mode uint32, context *base.Context) syscall.Errno {
	log.Debugf("Chmod: name [%s], mode [%d]", name, mode)
	ufs, _, _, path := v.getUFS(name)
	err := ufs.Chmod(path, mode)
	if err != nil {
		log.Errorf("Chmod: name [%s], mode [%d], failed: %v", name, mode, err)
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

func (v *OldVFS) Chown(name string, uid uint32, gid uint32, context *base.Context) syscall.Errno {
	log.Debugf("Chown: name [%s], uid [%d], gid [%d]", name, uid, gid)
	ufs, _, _, path := v.getUFS(name)
	err := ufs.Chown(path, uid, gid)
	if err != nil {
		log.Errorf("Chown: name [%s], uid [%d], gid [%d] failed: %v", name, uid, gid, err)
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

func (v *OldVFS) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *base.Context) syscall.Errno {
	log.Debugf("Utimens: name[%s] Atime [%v], Mtime[%v]", name, Atime, Mtime)
	ufs, _, _, path := v.getUFS(name)
	err := ufs.Utimens(path, Atime, Mtime)
	if err != nil {
		log.Errorf("Utimens: name[%s] Atime [%v], Mtime[%v] failed: [%v]", name, Atime, Mtime, err)
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

func (v *OldVFS) Truncate(name string, size uint64, context *base.Context) syscall.Errno {
	log.Debugf("Truncate: name[%s], size[%d]", name, size)
	ufs, _, _, path := v.getUFS(name)
	err := ufs.Truncate(path, size)
	if err != nil {
		log.Errorf("Truncate: name[%s], size[%d], failed: [%v]", name, size, err)
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

func (v *OldVFS) Access(name string, mode uint32, context *base.Context) syscall.Errno {
	log.Debugf("Access: name[%s], mode[%d]", name, mode)
	ufs, _, _, path := v.getUFS(name)
	err := ufs.Access(path, mode, context.Uid, context.Gid)
	if err != nil {
		log.Errorf("Access: name[%s], mode[%d], failed: [%v]", name, mode, err)
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

// Tree structure
func (v *OldVFS) Link(oldName string, newName string, context *base.Context) syscall.Errno {
	return syscall.ENOSYS
}

func (v *OldVFS) Mkdir(name string, mode uint32, context *base.Context) syscall.Errno {
	log.Debugf("Mkdir: name[%s], mode[%d]", name, mode)
	ufs, _, _, path := v.getUFS(name)
	err := ufs.Mkdir(path, mode)
	if err != nil {
		log.Errorf("Mkdir: name[%s], mode[%d], failed: [%v]", name, mode, err)
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}
func (v *OldVFS) Mknod(name string, mode uint32, dev uint32, context *base.Context) syscall.Errno {
	log.Debugf("Mknod: name[%s], mode[%d], dev[%d]", name, mode, dev)
	ufs, _, _, path := v.getUFS(name)
	err := ufs.Mknod(path, mode, dev)
	if err != nil {
		log.Errorf("Mknod: name[%s], mode[%d], dev[%d], failed: [%v]",
			name, mode, dev, err)
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

func (v *OldVFS) Rename(oldName string, newName string, context *base.Context) syscall.Errno {
	log.Debugf("Rename: oldName[%s], newName[%s]", oldName, newName)
	oldUfs, _, _, oldPath := v.getUFS(oldName)
	newUfs, _, _, newPath := v.getUFS(newName)
	// todo：跨底层存储rename暂不支持
	if oldUfs != newUfs {
		log.Errorf("Rename between two ufs is not supported")
		return syscall.ENOSYS
	}
	err := oldUfs.Rename(oldPath, newPath)
	if err != nil {
		log.Errorf("Rename: oldName[%s], newName[%s], failed: [%v]",
			oldName, newName, err)
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

func (v *OldVFS) Rmdir(name string, context *base.Context) syscall.Errno {
	log.Debugf("Rmdir: name[%s]", name)
	ufs, isLink, _, path := v.getUFS(name)
	// link的根目录不允许删除
	if isLink && (path == "" || path == "/") {
		log.Debugf("name[%s] is link file", name)
		return syscall.EPERM
	}
	err := ufs.Rmdir(path)
	if err != nil {
		log.Errorf("Rmdir: name[%s], failed: [%v]", name, err)
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

func (v *OldVFS) Unlink(name string, context *base.Context) syscall.Errno {
	log.Debugf("Unlink: name[%s]", name)
	ufs, isLink, _, path := v.getUFS(name)
	// link的根目录不允许删除
	if isLink && (path == "" || path == "/") {
		log.Debugf("name[%s] is link file", name)
		return syscall.EPERM
	}
	err := ufs.Unlink(path)
	if err != nil {
		log.Errorf("Unlink: name[%s], failed: [%v]", name, err)
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

// Extended attributes.
func (v *OldVFS) GetXAttr(name string, attribute string, context *base.Context) (data []byte, code syscall.Errno) {
	log.Debugf("GetXAttr: name[%s], attribute[%s]", name, attribute)
	ufs, _, _, path := v.getUFS(name)
	data, err := ufs.GetXAttr(path, attribute)
	if err != nil {
		log.Errorf("GetXAttr: name[%s], attribute[%s], failed: [%v]", name, attribute, err)
		return data, utils.ToSyscallErrno(err)
	}
	return data, syscall.F_OK
}

func (v *OldVFS) ListXAttr(name string, context *base.Context) (attributes []string, code syscall.Errno) {
	log.Debugf("ListXAttr: name[%s]", name)
	ufs, _, _, path := v.getUFS(name)
	data, err := ufs.ListXAttr(path)
	if err != nil {
		log.Errorf("ListXAttr: name[%s], failed: [%v]", name, err)
		return data, utils.ToSyscallErrno(err)
	}
	return data, syscall.F_OK
}

func (v *OldVFS) RemoveXAttr(name string, attr string, context *base.Context) syscall.Errno {
	log.Debugf("RemoveXAttr: name[%s], attr[%s]", name, attr)
	ufs, _, _, path := v.getUFS(name)
	err := ufs.RemoveXAttr(path, attr)
	if err != nil {
		log.Errorf("RemoveXAttr: name[%s], attr[%s], failed: [%v]", name, attr, err)
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

func (v *OldVFS) SetXAttr(name string, attr string, data []byte, flags int, context *base.Context) syscall.Errno {
	log.Debugf("SetXAttr: name[%s], attr[%s], data[%s], flags[%d]", name, attr, data, flags)
	ufs, _, _, path := v.getUFS(name)
	err := ufs.SetXAttr(path, attr, data, flags)
	if err != nil {
		log.Errorf("SetXAttr: name[%s], attr[%s], data[%s], flags[%d], failed: [%v]",
			name, attr, data, flags, err)
		return utils.ToSyscallErrno(err)
	}
	return syscall.F_OK
}

// File handling.  If opening for writing, the file's mtime
// should be updated too.
func (v *OldVFS) Open(name string, flags uint32, context *base.Context) (base.FileHandle, syscall.Errno) {
	log.Debugf("Open: name[%s], flags[%d]", name, flags)
	ufs, _, _, path := v.getUFS(name)
	fileHandle, err := ufs.Open(path, flags)
	if err != nil {
		log.Errorf("Open: name[%s], flags[%d], failed: [%v]", name, flags, err)
		return fileHandle, utils.ToSyscallErrno(err)
	}
	return fileHandle, syscall.F_OK
}

func (v *OldVFS) Create(name string, flags uint32, mode uint32, context *base.Context) (
	file base.FileHandle, code syscall.Errno) {
	log.Debugf("Create: name[%s], flags[%d], mode[%d]", name, flags, mode)
	ufs, _, _, path := v.getUFS(name)
	fileHandle, err := ufs.Create(path, flags, mode)
	if err != nil {
		log.Errorf("Create: name[%s], flags[%d], mode[%d], failed: [%v]",
			name, flags, mode, err)
		return fileHandle, utils.ToSyscallErrno(err)
	}
	return fileHandle, syscall.F_OK
}

// Directory handling
func (v *OldVFS) OpenDir(name string, context *base.Context) (stream []base.DirEntry, code syscall.Errno) {
	log.Debugf("OpenDir: name[%s]", name)
	ufs, isLink, _, path := v.getUFS(name)
	dirs, err := ufs.ReadDir(path)
	if err != nil {
		log.Errorf("OpenDir: name[%s] failed: [%v]", name, err)
		return dirs, utils.ToSyscallErrno(err)
	}
	// 不存在link嵌套，因此只有非link情况下才需要填充link文件
	v.ufsMapLock.RLock()
	ufsMap := v.ufsMap
	v.ufsMapLock.RUnlock()
	if !isLink && ufsMap != nil {
		ufsMap.Range(func(key, value interface{}) bool {
			dir := pathlib.Dir(key.(string))
			linkUfs := value.(ufslib.UnderFileStorage)
			if pathlib.Clean("/"+name) == pathlib.Clean("/"+dir) {
				entry := base.DirEntry{
					Mode: uint32(os.ModeSymlink),
					Name: pathlib.Base(key.(string)),
				}
				attr, err := linkUfs.GetAttr("")
				if err != nil {
					// TODO: 获取link文件报错，暂不往上抛出
					log.Errorf("getAttr : name[%s] failed: [%v]", key.(string), err)
				}
				entry.Mode = uint32(os.ModeSymlink | attr.Mode)
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

	log.Debugf("the dirs is [%+v]", dirs)
	return dirs, syscall.F_OK
}

// Symlinks.
func (v *OldVFS) Symlink(value string, linkName string, context *base.Context) (code syscall.Errno) {
	return syscall.ENOSYS
}

func (v *OldVFS) Readlink(name string, context *base.Context) (string, syscall.Errno) {
	return "", syscall.ENOSYS
}

func (v *OldVFS) StatFs(name string) *base.StatfsOut {
	log.Debugf("StatFs: name[%s]", name)
	statfs := v.defaultUfs.StatFs(name)
	return statfs
}
