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

package schema

import "path"

const (
	FSIDFormat      = "$(pfs.fs.id)"
	NameSpaceFormat = "$(namespace)"
	FSID            = "pfs.fs.id"
	PFSServer       = "pfs.server"
	PFSUserName     = "pfs.user.name"

	HostMntDir = "/data/paddleflow-fs/mnt"

	FsMetaDefault = "default"
	FsMetaMemory  = "mem"
	FsMetaLevelDB = "leveldb"
	FsMetaNutsDB  = "nutsdb"
)

func IsValidFsMetaDriver(metaDriver string) bool {
	switch metaDriver {
	case FsMetaDefault, FsMetaMemory, FsMetaLevelDB, FsMetaNutsDB:
		return true
	default:
		return false
	}
}

func DefaultCacheDir(fsID string) string {
	return path.Join(HostMntDir, fsID)
}
