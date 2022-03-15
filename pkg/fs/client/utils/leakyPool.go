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

type LeakyBuf struct {
	bufSize  int // size of each buffer
	freeList chan []byte
}

// NewLeakyBuf creates a leaky buffer which can hold at most n buffer, each
// with bufSize bytes.
func NewLeakyBuf(bufSize, n int) *LeakyBuf {
	return &LeakyBuf{
		bufSize:  bufSize,
		freeList: make(chan []byte, n),
	}
}

// Get returns a buffer from the leaky buffer or create a new buffer.
func (lb *LeakyBuf) Get() (b []byte) {
	select {
	case b = <-lb.freeList:
	default:
		b = make([]byte, lb.bufSize)
	}
	return
}

// Put add the buffer into the free buffer pool for reuse. Panic if the buffer
// size is not the same with the leaky buffer's. This is intended to expose
// error usage of leaky buffer.
func (lb *LeakyBuf) Put(b []byte) {
	if len(b) != lb.bufSize {
		return
	}
	select {
	case lb.freeList <- b:
	default:
	}
	return
}
