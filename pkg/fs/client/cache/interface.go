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

	"paddleflow/pkg/fs/client/ufs"
)

type Reader interface {
	io.ReaderAt
}

type Writer interface {
	io.WriterAt
}

type Store interface {
	NewReader(name string, length int, flags uint32, ufs ufs.UnderFileStorage,
		buffers ReadBufferMap, bufferPool *BufferPool, seqReadAmount uint64) Reader
	NewWriter(name string, length int, ufsFh ufs.FileHandle) Writer
	InvalidateCache(name string, length int) error
}

type DataCache interface {
	load(key string) (ReadCloser, bool)
	save(key string, buf []byte)
	delete(key string)
	clean()
}

type ReadCloser interface {
	io.Reader
	io.ReaderAt
	io.Closer
}
