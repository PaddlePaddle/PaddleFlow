/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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
	"runtime/debug"
	"sync"

	"github.com/shirou/gopsutil/v3/mem"
	log "github.com/sirupsen/logrus"
)

type Page struct {
	bufferPool  *BufferPool
	writeLength int
	lock        sync.RWMutex
	buffer      []byte
	end         bool
	index       int
	r           *rCache
}

type BufferPool struct {
	mu   sync.Mutex
	cond *sync.Cond

	bufSize            uint64
	totalBuffers       uint64
	computedMaxBuffers uint64

	pool *sync.Pool
}

func maxBuffers(bufSize uint64) uint64 {
	m, err := mem.VirtualMemory()
	if err != nil {
		panic(err)
	}

	availableMem := m.Available

	log.Debugf("amount of available memory: %v", availableMem/1024/1024)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	log.Debugf("amount of allocated memory: %v %v", ms.Sys/1024/1024, ms.Alloc/1024/1024)

	max := (availableMem + ms.Sys) / 2
	maxBuffers_ := max / bufSize
	log.Debugf("using up to %vMB buffers, now is %v", max, maxBuffers_)
	return maxBuffers_
}

func (pool *BufferPool) Init(size int) *BufferPool {
	pool.cond = sync.NewCond(&pool.mu)

	pool.pool = &sync.Pool{New: func() interface{} {
		return make([]byte, 0, size)
	}}

	return pool
}

func (pool *BufferPool) recomputeBufferLimit() {
	pool.computedMaxBuffers = maxBuffers(pool.bufSize)
}

func (pool *BufferPool) RequestMBuf(size uint64, block bool, blockSize int) (buf []byte) {
	pool.bufSize = size
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.totalBuffers%10 == 0 {
		pool.recomputeBufferLimit()
	}

	log.Debugf("requesting %v", size)

	for pool.computedMaxBuffers < 1 {
		if block {
			pool.MaybeGC()
			pool.recomputeBufferLimit()
			pool.cond.Wait()
		} else {
			return
		}
	}

	pool.totalBuffers++
	buf = make([]byte, 0, size)
	return
}

func (pool *BufferPool) MaybeGC() {
	debug.FreeOSMemory()
}

func (p *Page) SetCache() {
	tmp := p
	go func() {
		tmp.r.setCache(tmp.index, tmp.buffer, len(tmp.buffer))
		tmp.Free()
	}()
}

func (p *Page) ReadAt(buf []byte, offset uint64) (n int, err error) {
	if int(offset) > cap(p.buffer) {
		return 0, io.EOF
	}
	if int(offset) > len(p.buffer) {
		return 0, err
	}
	n = copy(buf, p.buffer[int(offset):])
	if n == 0 && p.end == true {
		return n, io.EOF
	}
	return
}

func (p *Page) WriteFrom(reader io.Reader) (n int, err error) {
	n, err = reader.Read(p.buffer[p.writeLength:cap(p.buffer)])
	p.lock.Lock()
	p.writeLength += n
	p.lock.Unlock()
	p.buffer = p.buffer[:p.writeLength]
	if err != nil {
		return n, err
	}
	log.Debugf("write from len %v and n %v", len(p.buffer), n)
	return
}

func (p *Page) Free() {
	p.bufferPool.mu.Lock()
	defer p.bufferPool.mu.Unlock()
	if p.buffer != nil {
		p.buffer = p.buffer[:0]
		p.bufferPool.pool.Put(p.buffer)
		p.bufferPool.cond.Signal()
	}
}

func (p *Page) Init(pool *BufferPool, size uint64, block bool, blockSize int) *Page {
	p.bufferPool = pool
	if size != 0 {
		p.buffer = p.bufferPool.RequestMBuf(size, block, blockSize)
		log.Debugf("init page %v blocksize %v and len %v", size, blockSize, len(p.buffer))
		if p.buffer == nil {
			return nil
		}
	}
	return p
}

type Buffer struct {
	mu     sync.Mutex
	cond   *sync.Cond
	offset uint64
	page   *Page
	reader io.ReadCloser
	err    error
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
		if b.page == nil {
			b.mu.Unlock()
			break
		}

		nread, err := b.page.WriteFrom(b.reader)
		if err != nil {
			if err == io.EOF {
				b.page.end = true
				_ = b.reader.Close()
				b.page.SetCache()
			}
			b.err = err
			b.mu.Unlock()
			break
		}

		if nread == 0 {
			b.page.end = true
			_ = b.reader.Close()
			b.page.SetCache()
			b.mu.Unlock()
			break
		}

		b.mu.Unlock()
		// if we get here we've read _something_, bounce this goroutine
		// to allow another one to read
		runtime.Gosched()
	}
}

func (b *Buffer) ReadAt(p []byte, offset uint64) (n int, err error) {
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
		n, err = b.page.ReadAt(p, offset)
		return n, err
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

	if b.page != nil {
		b.page.Free()
	}
	b.page = nil

	return
}
