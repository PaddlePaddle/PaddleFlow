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

package location_awareness

import (
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	"math/rand"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

type CacheStats struct {
	FsID     string
	CacheDir string
	NodeName string
	UsedSize int
}

type PatchInfo struct {
	K8sClient    utils.Client
	Pod          *corev1.Pod
	PodCachePath string
}

func PatchCacheStatsLoop(cacheReport CacheStats, patchInfo PatchInfo) {
	var errStat error
	var usageStat *disk.UsageStat
	for {
		usageStat, errStat = disk.Usage(patchInfo.PodCachePath)
		if errStat != nil {
			log.Errorf("disk stat path[%s] and err[%v]", patchInfo.PodCachePath, errStat)
			time.Sleep(1 * time.Second)
			continue
		}
		cacheReport.UsedSize = int(usageStat.Used / 1024)

		str, err := json.Marshal(cacheReport)
		if err != nil {
			log.Errorf("failed marshal cache stats %+v, err: %v", cacheReport, err)
			continue
		}

		patchInfo.Pod.ObjectMeta.Annotations[schema.AnnotationKeyCache] = string(str)
		err = patchInfo.K8sClient.PatchPodAnnotation(patchInfo.Pod)
		if err != nil {
			log.Errorf("PatchPodAnnotation %+v err[%v]", patchInfo.Pod.ObjectMeta.Annotations, err)
		}
		select {
		case <-time.After(time.Duration(15+rand.Intn(10)) * time.Second):
		}
	}
}
