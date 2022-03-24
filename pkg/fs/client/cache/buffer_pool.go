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
	"errors"
	"io"
	"runtime"
	"sync"
)

type Page struct {
	offset uint64
	buffer []byte
}

func (p *Page) ReadAt(buf []byte, offset uint64) (n int, err error) {
	n = copy(buf, p.buffer[offset:])
	return
}

func (p *Page) WriteFrom(reader io.Reader) (n int, err error) {
	n, err = io.ReadFull(reader, p.buffer)
	return
}

func (p Page) Init(size uint64) *Page {
	p.buffer = make([]byte, size)
	return &p
}

type Buffer struct {
	mu     sync.Mutex
	cond   *sync.Cond
	offset uint64
	page   *Page
	reader io.ReadCloser
	err    error
	r      *rCache
}

type ReaderProvider func() (io.ReadCloser, error)

func (b *Buffer) Init(page *Page, r ReaderProvider) *Buffer {
	b.cond = sync.NewCond(&b.mu)
	b.page = page

	go func() {
		b.readLoop(r)
	}()

	return b
}

func (b *Buffer) readLoop(r ReaderProvider) {
	b.mu.Lock()
	if b.reader == nil {
		b.reader, b.err = r()
		b.cond.Broadcast()
		if b.err != nil {
			b.mu.Unlock()
			return
		}
	}

	nread, err := b.page.WriteFrom(b.reader)
	if err != nil {
		b.err = err
		b.mu.Unlock()
		return
	}

	if nread == 0 {
		_ = b.reader.Close()
		b.mu.Unlock()
		return
	}

	go func() {
		b.r.setCache(int(b.offset), b.page.buffer, nread)
	}()

	b.mu.Unlock()
	// if we get here we've read _something_, bounce this goroutine
	// to allow another one to read
	runtime.Gosched()
}

func (b *Buffer) Read(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for b.reader == nil && b.err == nil {
		b.cond.Wait()
	}

	// we could have received the err before Read was called
	if b.reader == nil {
		if b.err == nil {
			return 0, errors.New("reader and err are both nil")
		}
		err = b.err
		return
	}

	if b.page != nil {
		n, err = b.page.ReadAt(p, b.page.offset)
		b.page.offset += uint64(n)
		if n == 0 {
			return n, io.EOF
		}
	} else {
		return 0, errors.New("page is empty, maybe oom")
	}

	return
}

func (b *Buffer) ReInit(r ReaderProvider) *Buffer {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.reader != nil {
		_ = b.reader.Close()
		b.reader = nil
	}

	b.err = nil
	go func() {
		b.readLoop(r)
	}()
	return b
}

func (b *Buffer) Close() (err error) {

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.reader != nil {
		err = b.reader.Close()
	}

	return
}
