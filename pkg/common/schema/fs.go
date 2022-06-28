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

import (
	"path"
	"strings"
)

const (
	PVNameTemplate  = "pfs-$(pfs.fs.id)-$(namespace)-pv"
	PVCNameTemplate = "pfs-$(pfs.fs.id)-pvc"
	FSIDFormat      = "$(pfs.fs.id)"
	NameSpaceFormat = "$(namespace)"

	PfsFsID    = "pfs.fs.id"
	PfsFsInfo  = "pfs.fs.info"
	PfsFsCache = "pfs.fs.cache"
	PfsServer  = "pfs.server"

	FusePodMntDir = "/home/paddleflow/mnt"

	FsMetaDefault = "default"
	FsMetaMemory  = "mem"
	FsMetaLevelDB = "leveldb"
	FsMetaNutsDB  = "nutsdb"

	FuseKeyFsInfo = "fs-info"
)

func IsValidFsMetaDriver(metaDriver string) bool {
	switch metaDriver {
	case FsMetaDefault, FsMetaMemory, FsMetaLevelDB, FsMetaNutsDB:
		return true
	default:
		return false
	}
}

func GetBindSource(fsID string) string {
	return path.Join(FusePodMntDir, fsID, "storage")
}

func ConcatenatePVName(namespace, fsID string) string {
	pvName := strings.Replace(PVNameTemplate, FSIDFormat, fsID, -1)
	pvName = strings.Replace(pvName, NameSpaceFormat, namespace, -1)
	return pvName
}

func ConcatenatePVCName(fsID string) string {
	return strings.Replace(PVCNameTemplate, FSIDFormat, fsID, -1)
}
