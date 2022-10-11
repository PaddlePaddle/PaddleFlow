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

package cache

import (
	"io"
	"sync"
	"syscall"

	ufslib "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs"
)

type ReadBuffer struct {
	ufs      ufslib.UnderFileStorage
	nRetries uint8
	page     *Page
	path     string
	flags    uint32
	offset   uint64
	size     uint32
	index    int
	lock     sync.RWMutex
	r        *rCache
	Buffer   *Buffer
}

type ReadBufferMap map[uint64]*ReadBuffer

func (b *ReadBuffer) Init(pool *BufferPool, blocksize int) *ReadBuffer {
	b.nRetries = 3
	p := &Page{r: b.r, index: b.index}
	b.page = p.Init(pool, uint64(b.size), false, blocksize)
	if b.page == nil {
		return nil
	}

	b.initBuffer(b.offset, b.size)
	return b
}

func (b *ReadBuffer) initBuffer(offset uint64, size uint32) {
	getFunc := func() (io.ReadCloser, error) {
		resp, err := b.ufs.Get(b.path, syscall.O_RDONLY, int64(offset), int64(size))
		if err != nil {
			return nil, err
		}

		return resp, nil
	}

	if b.Buffer == nil {
		buf := &Buffer{offset: offset}
		b.Buffer = buf.Init(b.page, getFunc)
	} else {
		b.Buffer = b.Buffer.ReInit(getFunc)
	}
}

func (b *ReadBuffer) ReadAt(offset uint64, p []byte) (n int, err error) {
	n, err = b.Buffer.ReadAt(p, offset)
	b.lock.Lock()
	b.size -= uint32(n)
	b.lock.Unlock()
	return
}
