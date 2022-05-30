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

package k8s

import (
	"fmt"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
)

const (
	PFSPrefix = "pfs-"
	PVCSuffix = "-pvc"
)

// VolumeMount is a struct that contains volume information and its mounts in the pod spec
type VolumeMount struct {
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
}

// GetVolumeMounts Get volume mounts information from the pod spec
func GetVolumeMounts(pod *v1.Pod) []VolumeMount {
	volumeMountMaps := make(map[string]VolumeMount)
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
		volumeMountMaps[v.Name] = VolumeMount{
			PodUID:     string(pod.UID),
			PFSID:      GetBmlVolumeIDByPVCName(v.PersistentVolumeClaim.ClaimName),
			ClaimName:  v.PersistentVolumeClaim.ClaimName,
			VolumeName: volumeName,
			ReadOnly:   v.PersistentVolumeClaim.ReadOnly,
		}
	}

	var volumeMounts []VolumeMount
	for _, v := range volumeMountMaps {
		volumeMounts = append(volumeMounts, v)
	}
	return volumeMounts
}

func GetBmlVolumeIDByPVCName(claimName string) string {
	idRegexp := regexp.MustCompile(`^pfs-(.*)-pvc$`)
	ids := idRegexp.FindStringSubmatch(claimName)
	if len(ids) == 2 {
		return ids[1]
	}
	return ""
}
