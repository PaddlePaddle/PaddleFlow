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
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
)

func (fh *s3FileHandle) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	log.Debugf("S3 Allocate: fh.name[%s]", fh.name)
	if fh.writeTmpfile != nil {
		if fh.canWrite != nil {
			select {
			case <-fh.canWrite:
				break
			}
		}
		err := syscall.Fallocate(int(fh.writeTmpfile.Fd()), mode, int64(off), int64(size))
		return fuse.ToStatus(err)
	}
	return fuse.EBADF
}