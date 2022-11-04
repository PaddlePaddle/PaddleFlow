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

package utils

import (
	"os"
	"os/user"
	"strconv"
	"sync"
	"syscall"

	"github.com/pkg/sftp"
)

// HasAccess tests if a caller can access a file with permissions
// `perm` in mode `mask`
func HasAccess(callerUid, callerGid, fileUid, fileGid uint32, perm uint32, mask uint32) bool {
	if callerUid == 0 {
		// root can do anything.
		return true
	}
	mask = mask & 7
	if mask == 0 {
		return true
	}
	if callerUid == fileUid {
		if perm&(mask<<6) != 0 {
			return true
		}
	}
	if callerGid == fileGid {
		if perm&(mask<<3) != 0 {
			return true
		}
	}
	if perm&mask != 0 {
		return true
	}
	// Check other groups.
	if perm&(mask<<3) == 0 {
		// avoid expensive lookup if it's not allowed anyway
		return false
	}
	u, err := user.LookupId(strconv.Itoa(int(callerUid)))
	if err != nil {
		return false
	}
	gs, err := u.GroupIds()
	if err != nil {
		return false
	}
	fileGidStr := strconv.Itoa(int(fileGid))
	for _, gidStr := range gs {
		if gidStr == fileGidStr {
			return true
		}
	}
	return false
}

var uids = make(map[int]string)
var gids = make(map[int]string)
var users = make(map[string]int)
var groups = make(map[string]int)
var mutex sync.Mutex

func UserName(uid int) string {
	mutex.Lock()
	defer mutex.Unlock()
	name, ok := uids[uid]
	if !ok {
		if u, err := user.LookupId(strconv.Itoa(uid)); err == nil {
			name = u.Username
			uids[uid] = name
		}
	}
	return name
}

func GroupName(gid int) string {
	mutex.Lock()
	defer mutex.Unlock()
	name, ok := gids[gid]
	if !ok {
		if g, err := user.LookupGroupId(strconv.Itoa(gid)); err == nil {
			name = g.Name
			gids[gid] = name
		}
	}
	return name
}

func GetOwnerGroup(info os.FileInfo) (string, string) {
	var owner, group string
	switch st := info.Sys().(type) {
	case *syscall.Stat_t:
		owner = UserName(int(st.Uid))
		group = GroupName(int(st.Gid))
	case *sftp.FileStat:
		owner = UserName(int(st.UID))
		group = GroupName(int(st.GID))
	}
	return owner, group
}

func LookupUser(name string) int {
	mutex.Lock()
	defer mutex.Unlock()
	if u, ok := users[name]; ok {
		return u
	}
	var uid = 999 // nobody
	if u, err := user.Lookup(name); err == nil {
		uid, _ = strconv.Atoi(u.Uid)
	}
	users[name] = uid
	return uid
}

func LookupGroup(name string) int {
	mutex.Lock()
	defer mutex.Unlock()
	if u, ok := groups[name]; ok {
		return u
	}
	var gid = 999 // nobody
	if u, err := user.LookupGroup(name); err == nil {
		gid, _ = strconv.Atoi(u.Gid)
	}
	groups[name] = gid
	return gid
}

func StatModeToFileMode(mode int) os.FileMode {
	fmode := os.FileMode(mode & 0777)
	switch mode & syscall.S_IFMT {
	case syscall.S_IFBLK:
		fmode |= os.ModeDevice
	case syscall.S_IFCHR:
		fmode |= os.ModeDevice | os.ModeCharDevice
	case syscall.S_IFDIR:
		fmode |= os.ModeDir
	case syscall.S_IFIFO:
		fmode |= os.ModeNamedPipe
	case syscall.S_IFLNK:
		fmode |= os.ModeSymlink
	case syscall.S_IFREG:
		// nothing to do
	case syscall.S_IFSOCK:
		fmode |= os.ModeSocket
	}
	if mode&syscall.S_ISGID != 0 {
		fmode |= os.ModeSetgid
	}
	if mode&syscall.S_ISUID != 0 {
		fmode |= os.ModeSetuid
	}
	if mode&syscall.S_ISVTX != 0 {
		fmode |= os.ModeSticky
	}
	return fmode
}
