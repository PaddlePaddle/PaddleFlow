// Copyright 2015 - 2017 Ka-Hing Cheung
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"io"
	"runtime"
	"sync"
)

type Buf struct {
	offset uint64
	buffer []byte
}

func (b *Buf) ReadAt(p []byte, offset uint64) (n int, err error) {
	n = copy(p, b.buffer[offset:])
	return
}

func (b *Buf) WriteFrom(reader io.Reader) (n int, err error) {
	n, err = io.ReadFull(reader, b.buffer)
	go func() {

	}()
	return
}

func (b Buf) Init(size uint64) *Buf {
	b.buffer = make([]byte, size)
	return &b
}

type Buffer struct {
	mu   sync.Mutex
	cond *sync.Cond

	offset uint64
	buf    *Buf
	reader io.ReadCloser
	err    error
	r      *rCache
}

type ReaderProvider func() (io.ReadCloser, error)

func (b Buffer) Init(buf *Buf, r ReaderProvider) *Buffer {
	b.cond = sync.NewCond(&b.mu)
	b.buf = buf

	go func() {
		b.readLoop(r)
	}()

	return &b
}

func (b *Buffer) readLoop(r ReaderProvider) {
	for {
		b.mu.Lock()
		if b.reader == nil {
			b.reader, b.err = r()
			b.cond.Broadcast()
			if b.err != nil {
				b.mu.Unlock()
				break
			}
		}

		nread, err := b.buf.WriteFrom(b.reader)
		if err != nil {
			b.err = err
			b.mu.Unlock()
			break
		}

		if nread == 0 {
			b.reader.Close()
			b.mu.Unlock()
			break
		}

		go func() {
			b.r.setCache(int(b.offset), b.buf.buffer, nread)
		}()

		b.mu.Unlock()
		// if we get here we've read _something_, bounce this goroutine
		// to allow another one to read
		runtime.Gosched()
	}
}

func (b *Buffer) readFromStream(p []byte) (n int, err error) {

	n, err = b.reader.Read(p)
	if n != 0 && err == io.ErrUnexpectedEOF {
		err = nil
	}
	return
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
			panic("reader and err are both nil")
		}
		err = b.err
		return
	}

	if b.buf != nil {
		n, err = b.buf.ReadAt(p, b.buf.offset)
		b.buf.offset += uint64(n)
		if n == 0 {
			return n, io.EOF
		}
	} else {
		panic("buf is empty")
	}

	return
}

func (b *Buffer) ReInit(r ReaderProvider) *Buffer {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.reader != nil {
		b.reader.Close()
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
