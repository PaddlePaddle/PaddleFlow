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

	PVNameTemplate  = "pfs-$(pfs.fs.id)-$(namespace)-$(pk)-$(host)-$(port)-pv"
	PVCNameTemplate = "pfs-$(pfs.fs.id)-$(pk)-$(host)-$(port)-pvc"
	FSIDFormat      = "$(pfs.fs.id)"
	PKFormat        = "$(pk)"
	HostFormat      = "$(host)"
	PortFormat      = "$(port)"
	NameSpaceFormat = "$(namespace)"

	PFSID           = "pfs.fs.id"
	PFSType         = "pfs.type"
	PFSMountOptions = "pfs.mount.options"
	PFSInfo         = "pfs.info"
	PFSAddress      = "pfs.mount.address"
	PFSCache        = "pfs.cache"
	PFSServer       = "pfs.server"
	PFSMountMethod  = "pfs.mount.method"
	PFSClusterID    = "pfs.cluster.id"

	FusePodMntDir = "/home/paddleflow/mnt"

	FsMetaMemory = "mem"
	FsMetaDisk   = "disk"

	FuseKeyFsInfo = "fs-info"

	LabelKeyFsID             = "fsID"
	LabelKeyCacheID          = "cacheID"
	LabelKeyNodeName         = "nodename"
	LabelKeyUsedSize         = "usedSize"
	AnnotationKeyCacheDir    = "cacheDir"
	AnnotationKeyMTime       = "modifiedTime"
	AnnotationKeyMountPrefix = "mount-"

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

func ConcatenatePVName(namespace, fsID, pk, host, port string) string {
	pvName := strings.Replace(PVNameTemplate, FSIDFormat, fsID, -1)
	pvName = strings.Replace(pvName, NameSpaceFormat, namespace, -1)
	pvName = strings.Replace(pvName, PKFormat, pk, -1)
	pvName = strings.Replace(pvName, HostFormat, host, -1)
	pvName = strings.Replace(pvName, PortFormat, port, -1)
	return pvName
}

func ConcatenatePVCName(fsID, pk, host, port string) string {
	pvcName := strings.Replace(PVCNameTemplate, FSIDFormat, fsID, -1)
	pvcName = strings.Replace(pvcName, PKFormat, pk, -1)
	pvcName = strings.Replace(pvcName, HostFormat, host, -1)
	pvcName = strings.Replace(pvcName, PortFormat, port, -1)
	return pvcName
}
