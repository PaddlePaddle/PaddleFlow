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
	"sync"
	"syscall"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta"
	ufslib "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs"
)

type handle struct {
	lock     sync.RWMutex
	fh       uint64
	inode    Ino
	reader   FileReader
	writer   FileWriter
	children []*meta.Entry

	// internal files
	off  uint64
	data []byte
}

func (v *VFS) newHandle(inode Ino) *handle {
	v.handleLock.Lock()
	defer v.handleLock.Unlock()
	fh := v.nextfh
	h := &handle{inode: inode, fh: fh}
	v.nextfh++
	v.handleMap[inode] = append(v.handleMap[inode], h)
	return h
}

func (v *VFS) findAllHandle(inode Ino) []*handle {
	v.handleLock.RLock()
	defer v.handleLock.RUnlock()
	return v.handleMap[inode]
}

func (v *VFS) findHandle(inode Ino, fh uint64) *handle {
	v.handleLock.RLock()
	defer v.handleLock.RUnlock()
	for _, f := range v.handleMap[inode] {
		if f.fh == fh {
			return f
		}
	}
	return nil
}

func (v *VFS) releaseHandle(inode Ino, fh uint64) {
	v.handleLock.Lock()
	defer v.handleLock.Unlock()
	handles := v.handleMap[inode]
	if handles == nil {
		return
	}
	for i, f := range handles {
		if f.fh == fh {
			if i+1 < len(handles) {
				handles[i] = handles[len(handles)-1]
			}
			if len(handles) > 1 {
				v.handleMap[inode] = handles[:len(handles)-1]
			} else {
				delete(v.handleMap, inode)
			}
			break
		}
	}
}

func (v *VFS) releaseFileHandle(ino Ino, fh uint64) {
	h := v.findHandle(ino, fh)
	if h == nil {
		return
	}
	h.Close()
	v.releaseHandle(ino, fh)
}

//  O_ACCMODE<0003>：读写文件操作时，用于取出flag的低2位
//  O_RDONLY<00>：只读打开
//  O_WRONLY<01>：只写打开
//  O_RDWR<02>：读写打开
func (v *VFS) newFileHandle(inode Ino, length uint64, flags uint32, ufs ufslib.UnderFileStorage, path string) (uint64, error) {
	h := v.newHandle(inode)
	var err error
	h.lock.Lock()
	defer h.lock.Unlock()
	switch flags & syscall.O_ACCMODE {
	case syscall.O_RDONLY:
		h.reader, err = v.reader.Open(inode, length, ufs, path)
	case syscall.O_WRONLY:
		h.writer, err = v.writer.Open(inode, length, ufs, path)
	case syscall.O_RDWR:
		h.reader, err = v.reader.Open(inode, length, ufs, path)
		h.writer, err = v.writer.Open(inode, length, ufs, path)
	}
	return h.fh, err
}

func (h *handle) Close() {
	if h.reader != nil {
		h.reader.Close()
		h.reader = nil
	}
	if h.writer != nil {
		h.writer.Close()
		h.writer = nil
	}
}
