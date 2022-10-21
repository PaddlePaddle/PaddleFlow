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
	PFSTypeLocal = "local"

	PVNameTemplate  = "pfs-$(pfs.fs.id)-$(namespace)-pv"
	PVCNameTemplate = "pfs-$(pfs.fs.id)-pvc"
	FSIDFormat      = "$(pfs.fs.id)"
	NameSpaceFormat = "$(namespace)"

	PFSID        = "pfs.fs.id"
	PFSInfo      = "pfs.fs.info"
	PFSCache     = "pfs.fs.cache"
	PFSClusterID = "pfs.cluster.id"

	FusePodMntDir = "/home/paddleflow/mnt"

	FsMetaMemory = "mem"
	FsMetaDisk   = "disk"

	FuseKeyFsInfo = "fs-info"

	// mount pod label key
	KeyFsID     = "fsID"
	KeyCacheID  = "cacheID"
	KeyNodeName = "nodename"
	KeyUsedSize = "usedSize" // patched by cache-worker in mount pod

	// mount pod annotation key - patched by csi
	KeyCacheDir     = "cacheDir"
	KeyModifiedTime = "modifiedTime"
	KeyMountPrefix  = "mount-"

	EnvKeyMountPodName = "POD_NAME"
	EnvKeyNamespace    = "NAMESPACE"

	MountPodNamespace = "paddleflow"
)

func IsValidFsMetaDriver(metaDriver string) bool {
	switch metaDriver {
	case FsMetaDisk, FsMetaMemory:
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
