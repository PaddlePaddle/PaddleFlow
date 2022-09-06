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

package ufs

import (
	"bytes"
	"fmt"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
)

// todo:: not use base.filehandle
type FileHandler struct {
	fd FileHandle
}

func NewFileHandle(fd FileHandle) FileHandler {
	return FileHandler{fd: fd}
}

func (u *FileHandler) ReadAt(buf []byte, off int64) (int, error) {
	// todo:: replace fd read
	result, code := u.fd.Read(buf, off)
	if utils.IsError(syscall.Errno(code)) {
		log.Debugf("ufs read err %v", code)
		return 0, fmt.Errorf("ufs read err %v", code)
	}
	buffer, errBytes := result.Bytes(buf)
	if utils.IsError(syscall.Errno(errBytes)) {
		return 0, syscall.Errno(errBytes)
	}

	// s3存储没有填充b，因此需要判断填充b。local模式需要依赖len(b)填充读取字节长度。
	size := len(buffer)
	if size > len(buf) {
		size = len(buf)
	} else {
		buf = buf[0:size]
	}
	if bytes.Compare(buffer[0:size], buf) != 0 {
		buf = append(buf[:0], buffer[0:size]...)
	}
	return len(buf), nil
}

func (u *FileHandler) WriteAt(buf []byte, off int64) (int, error) {
	n, err := u.fd.Write(buf, off)
	if utils.IsError(syscall.Errno(err)) {
		return 0, syscall.Errno(err)
	}
	return int(n), nil
}
