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
	"path"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

const initDirSize = 20

type Inode struct {
	inode  Ino
	name   string
	parent *Inode
	child  map[string]Ino
	sync.RWMutex

	inodeHandle *InodeHandle
}

type InodeHandle struct {
	sync.RWMutex
	// Map of Go objects indexed by NodeId
	handles    map[Ino]*Inode
	nextNodeID uint64
}

func NewInodeHandle() *InodeHandle {
	return &InodeHandle{
		handles: make(map[Ino]*Inode),
		// todo:: root nodeid can configure
		nextNodeID: 1,
	}
}

const (
	rootInodeID = Ino(1)
)

func (m *InodeHandle) InitRootNode() {
	if m.handles[rootInodeID] == nil {
		m.handles[rootInodeID] = &Inode{
			name:        "",
			inode:       rootInodeID,
			parent:      nil,
			child:       make(map[string]Ino, initDirSize),
			inodeHandle: m,
		}
		m.nextNodeID++
	}
}

func (m *InodeHandle) NewInode(name string, isDir bool) *Inode {
	node := new(Inode)
	if isDir {
		node.child = make(map[string]Ino, initDirSize)
	}
	node.name = name
	m.Lock()
	node.inode = Ino(m.nextNodeID)
	m.handles[node.inode] = node
	m.nextNodeID++
	m.Unlock()
	node.inodeHandle = m
	return node
}

func (m *InodeHandle) toInode(i Ino) *Inode {
	m.RLock()
	v := m.handles[i]
	m.RUnlock()
	return v
}

func (m *InodeHandle) InodeToPath(inode *Inode) string {
	if inode == nil {
		log.Errorf("inode is nil inodeHandle[%+v]", m)
		return ""
	}
	if inode.parent == nil {
		return "/"
	}
	var builder strings.Builder
	var segments []string
	for {
		if inode.parent == nil {
			break
		}
		segments = append(segments, inode.name)
		inode = inode.parent
	}
	for i := len(segments) - 1; i >= 0; i-- {
		builder.WriteString("/")
		builder.WriteString(segments[i])
	}
	return builder.String()
}

func (m *InodeHandle) ParentInodeToPath(parent *Inode, name string) string {
	prefixPath := m.InodeToPath(parent)
	return path.Join(prefixPath, name)
}

func (m *InodeHandle) InoToPath(inode Ino) string {
	return m.InodeToPath(m.toInode(inode))
}

func (m *InodeHandle) PathToInode(path string) *Inode {
	parent := Ino(1)
	ss := strings.Split(path, "/")
	for _, name := range ss {
		if len(name) == 0 {
			continue
		}
		inode := m.toInode(parent)
		if inode != nil {
			if inode.child != nil {
				n, ok := inode.child[name]
				if ok {
					parent = n
				}
			}
		}
	}
	result := m.toInode(parent)
	return result

}

func (n *Inode) NewChild(name string, isDir bool) *Inode {
	node := n.inodeHandle.NewInode(name, isDir)
	node.parent = n
	n.AddChild(name, node)
	return node
}

func (n *Inode) AddChild(name string, child *Inode) {
	n.Lock()
	n.child[name] = child.inode
	child.parent = n
	n.Unlock()
}

func (n *Inode) RmChild(name string) (child Ino) {
	n.RLock()
	ino := n.child[name]
	n.RUnlock()
	if ino != 0 {
		n.Lock()
		delete(n.child, name)
		n.Unlock()
		n.inodeHandle.Lock()
		delete(n.inodeHandle.handles, ino)
		n.inodeHandle.Unlock()
	}
	return ino
}

func (n *Inode) GetChild(name string) (child Ino) {
	n.RLock()
	ino := n.child[name]
	n.RUnlock()
	return ino
}

// IsDir returns true if this is a directory.
func (n *Inode) IsDir() bool {
	return n.child != nil
}
