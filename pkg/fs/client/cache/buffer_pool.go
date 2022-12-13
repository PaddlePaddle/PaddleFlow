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
	"math"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/shirou/gopsutil/v3/mem"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

// todo:: cacheBuf may be block, can use ringbuf
var cacheChan = make(chan *Page, 2000)
var cacheGoPool, _ = ants.NewPool(5)
var lastFreeOSMemoryMemPercent = float64(0)
var freeI int

func init() {
	var count = 0
	go func() {
		for page := range cacheChan {
			page_ := page
			count += 1
			_ = cacheGoPool.Submit(func() {
				page_.r.setCache(page_.index, page_.buffer, len(page_.buffer))
				if *page_.closed {
					page_.bufferPool.pool.Put(page_.buffer)
				}
				*page_.writeCacheReady = true
				page_ = nil
			})
		}
	}()
	go func() {
		for {
			freeMemory()
			time.Sleep(10 * time.Second)
		}
	}()
}

func freeMemory() {
	mp := utils.GetProcessMemPercent() / 100
	freeI += 1
	if mp > 0.3 {
		if math.Abs(lastFreeOSMemoryMemPercent-float64(mp)) > 0.1 || freeI >= 3 {
			debug.FreeOSMemory()
			time.Sleep((10 + time.Duration(rand.Intn(10))) * time.Second)
			lastFreeOSMemoryMemPercent = float64(mp)
			freeI = 0
		} else {
			freeI += 1
		}
	}
}

type Page struct {
	bufferPool      *BufferPool
	writeLength     int
	lock            sync.RWMutex
	buffer          []byte
	ready           bool
	index           int
	r               *rCache
	closed          *bool
	writeCacheReady *bool
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
		out := make([]byte, 0, size)
		return out
	}}

	return pool
}

func (pool *BufferPool) recomputeBufferLimit() {
	pool.computedMaxBuffers = maxBuffers(pool.bufSize)
}

func (pool *BufferPool) RequestMBuf(size uint64, block bool, blockSize int) []byte {
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
			return nil
		}
	}

	pool.totalBuffers++
	return pool.pool.Get().([]byte)
}

func (pool *BufferPool) MaybeGC() {
	debug.FreeOSMemory()
}

func (p *Page) setCache() {
	page := &Page{
		index:           p.index,
		buffer:          p.buffer,
		r:               p.r,
		bufferPool:      p.bufferPool,
		closed:          p.closed,
		writeCacheReady: p.writeCacheReady,
	}
	go func() {
		cacheChan <- page
	}()
}

func (p *Page) ReadAt(buf []byte, offset uint64) (n int, err error) {
	if int(offset) > cap(p.buffer) {
		return 0, io.EOF
	}
	if int(offset) > len(p.buffer) || int(offset) >= p.writeLength {
		if p.ready {
			return 0, io.EOF
		}
		// page not ready
		return 0, err
	}
	n = copy(buf, p.buffer[int(offset):p.writeLength])
	if p.ready && n == p.writeLength-int(offset) {
		return n, io.EOF
	}
	return
}

func (p *Page) WriteFrom(reader io.Reader) (n int, err error) {
	if p.writeLength >= cap(p.buffer) {
		return 0, err
	}
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
		if *p.writeCacheReady {
			p.bufferPool.pool.Put(p.buffer)
		}
		p.buffer = nil
		p.bufferPool.cond.Signal()
	}
	*p.closed = true
}

func (p *Page) Init(pool *BufferPool, size uint64, block bool, blockSize int) *Page {
	p.bufferPool = pool
	if size != 0 {
		p.buffer = p.bufferPool.RequestMBuf(size, block, blockSize)
		log.Debugf("init page %v blocksize %v and len %v", size, blockSize, len(p.buffer))
		if p.buffer == nil {
			return nil
		}
		cacheWrite := false
		closed := false
		p.closed = &closed
		p.writeCacheReady = &cacheWrite
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
				b.page.ready = true
				_ = b.reader.Close()
				b.page.setCache()
			}
			b.err = err
			b.mu.Unlock()
			break
		}

		if nread == 0 {
			b.page.ready = true
			_ = b.reader.Close()
			b.page.setCache()
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
