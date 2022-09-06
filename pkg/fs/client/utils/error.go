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

import (
	"os"
	"strings"
	"syscall"
)

func IsError(err syscall.Errno) bool {
	if err == syscall.F_OK {
		return false
	}
	return true
}

func IfNotExist(err error) bool {
	err_ := ToSyscallErrno(err)
	if err_ == syscall.ENOENT || err_ == syscall.ENOTDIR {
		return true
	}
	return false
}

// err默认为syscall.Errno, 当不能转换时返回syscall.ENOTSUP
func ToSyscallErrno(err error) syscall.Errno {
	if err == nil {
		return syscall.F_OK
	}
	if errno, ok := err.(syscall.Errno); ok {
		return errno
	}

	if pathError, ok := err.(*os.PathError); ok {
		if pathError.Err == os.ErrNotExist {
			return syscall.ENOENT
		}
		if pathError.Err == os.ErrPermission {
			return syscall.EACCES
		}
	}

	if strings.Contains(err.Error(), "no such file or directory") {
		return syscall.ENOTDIR
	}

	if strings.Contains(err.Error(), "file does not exist") {
		return syscall.ENOENT
	}

	if strings.Contains(err.Error(), "file exists") {
		return syscall.EEXIST
	}

	if strings.Contains(err.Error(), "file already exists") {
		return syscall.EEXIST
	}

	if strings.Contains(err.Error(), "bad file descriptor") {
		return syscall.EBADF
	}

	if strings.Contains(err.Error(), "Operation unsupported") {
		return syscall.ENOSYS
	}
	return syscall.ENOTSUP
}
