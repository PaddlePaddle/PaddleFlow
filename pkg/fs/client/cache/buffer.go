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
	"fmt"
	"io"

	ufslib "paddleflow/pkg/fs/client/ufs"
)

type ReadBuffer struct {
	ufs      ufslib.UnderFileStorage
	nRetries uint8
	page     *Page
	r        *rCache
	path     string
	flags    uint32
	offset   uint64
	size     uint32
	Buffer   *Buffer
}

type ReadBufferMap map[uint64]*ReadBuffer

func (b ReadBuffer) Init() *ReadBuffer {
	b.nRetries = 3
	b.page = Page{}.Init(uint64(b.size))

	b.initBuffer(b.offset, b.size)
	return &b
}

func (b *ReadBuffer) initBuffer(offset uint64, size uint32) {
	getFunc := func() (io.ReadCloser, error) {
		resp, err := b.ufs.Get(b.path, b.flags, int64(offset), int64(size))
		if err != nil {
			return nil, err
		}

		return resp, nil
	}

	if b.Buffer == nil {
		buf := &Buffer{r: b.r, offset: offset}
		b.Buffer = buf.Init(b.page, getFunc)
	} else {
		b.Buffer = b.Buffer.ReInit(getFunc)
	}
}

func (b *ReadBuffer) Read(offset uint64, p []byte) (n int, err error) {
	b.Buffer.page.offset = offset
	n, err = io.ReadFull(b.Buffer, p)
	if n != 0 && err == io.ErrUnexpectedEOF {
		err = nil
	}
	if n > 0 {
		if uint32(n) > b.size {
			panic(fmt.Sprintf("read more than available %v %v", n, b.size))
		}

		b.offset += uint64(n)
		b.size -= uint32(n)
	}
	if b.size == 0 && err != nil {
		// we've read everything, sometimes we may
		// request for more bytes then there's left in
		// this chunk so we could get an error back,
		// ex: http2: response body closed this
		// doesn't tend to happen because our chunks
		// are aligned to 4K and also 128K (except for
		// the last chunk, but seems kernel requests
		// for a smaller buffer for the last chunk)
		err = nil
	}
	return
}
