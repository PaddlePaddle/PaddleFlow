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

package controller

import (
	"fmt"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

const (
	PFSPrefix = "pfs-"
	PVCSuffix = "-pvc"
)

// volumeMountInfo is a struct that contains volume information and its mounts in the pod spec
type volumeMountInfo struct {
	// POD UID is the unique in time and space value for the Pod
	PodUID string

	// PFSID is the unique identifies for volume in paddle flow storage service
	PFSID string

	// VolumeName is the PV's name
	VolumeName string

	// ClaimName is the PVC's name
	ClaimName string

	// ReadOnly which setting in pod's volumes
	ReadOnly bool

	// A SubPath is a volumeMount with subPath param in the pod's spec
	SubPaths []SubPath
}

// The SubPath is a volumeMount with subPath param in the pod's spec
type SubPath struct {
	ReadOnly   bool
	SourcePath string
	TargetPath string
}

func getPodVolumeMounts(pod *v1.Pod) []volumeMountInfo {
	volumeMountMaps := make(map[string]volumeMountInfo)
	for _, v := range pod.Spec.Volumes {
		pvc := v.PersistentVolumeClaim
		if pvc == nil {
			continue
		}

		if !strings.HasSuffix(pvc.ClaimName, PVCSuffix) ||
			!strings.HasPrefix(pvc.ClaimName, PFSPrefix) {
			log.Tracef("pvc[%s] is not valid, skipping this volume", pvc.ClaimName)
			continue
		}

		volumeName := fmt.Sprintf("%s-%s-pv", strings.TrimSuffix(v.PersistentVolumeClaim.ClaimName, PVCSuffix),
			pod.Namespace)
		volumeMountMaps[v.Name] = volumeMountInfo{
			PodUID:     string(pod.UID),
			PFSID:      getPfsVolumeIDByPVCName(v.PersistentVolumeClaim.ClaimName),
			ClaimName:  v.PersistentVolumeClaim.ClaimName,
			VolumeName: volumeName,
			ReadOnly:   v.PersistentVolumeClaim.ReadOnly,
		}
	}

	for _, container := range pod.Spec.Containers {
		for i, mount := range container.VolumeMounts {
			volumeMount, ok := volumeMountMaps[mount.Name]
			if !ok {
				continue
			}

			volumeBindMountPath := utils.GetVolumeBindMountPathByPod(string(pod.UID), volumeMount.VolumeName)
			subpath := SubPath{
				ReadOnly:   mount.ReadOnly,
				SourcePath: utils.GetSubPathSourcePath(volumeBindMountPath, mount.SubPath),
				TargetPath: utils.GetSubPathTargetPath(string(pod.UID), volumeMount.VolumeName, container.Name, i),
			}
			volumeMount.SubPaths = append(volumeMount.SubPaths, subpath)
			volumeMountMaps[mount.Name] = volumeMount
		}
	}

	var volumeMounts []volumeMountInfo
	for _, v := range volumeMountMaps {
		volumeMounts = append(volumeMounts, v)
	}
	return volumeMounts
}

func getPfsVolumeIDByPVCName(claimName string) string {
	idRegexp := regexp.MustCompile(`^pfs-(.*)-pvc$`)
	ids := idRegexp.FindStringSubmatch(claimName)
	if len(ids) == 2 {
		return ids[1]
	}
	return ""
}
