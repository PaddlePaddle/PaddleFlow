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
	"math/rand"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
	log "github.com/sirupsen/logrus"
	k8sCore "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func PatchCacheStatsLoop(k8sClient utils.Client, pod *k8sCore.Pod,
	fsID, cacheDir, nodname, podCachePath string) {
	var errStat error
	var usageStat *disk.UsageStat
	for {
		usageStat, errStat = disk.Usage(podCachePath)
		if errStat != nil {
			log.Errorf("disk stat path[%s] and err[%v]", podCachePath, errStat)
			time.Sleep(1 * time.Second)
			continue
		}

		cacheStats := model.CacheStats{
			FsID:     fsID,
			CacheDir: cacheDir,
			NodeName: nodname,
			UsedSize: int(usageStat.Used / 1024),
		}

		str, err := json.Marshal(cacheStats)
		if err != nil {
			log.Errorf("failed marshal cache stats %+v, err: %v", cacheStats, err)
			continue
		}

		pod.ObjectMeta.Annotations[schema.AnnotationKeyCache] = string(str)
		err = k8sClient.PatchPodAnnotation(pod)
		if err != nil {
			log.Errorf("PatchPodAnnotation %+v err[%v]", pod.ObjectMeta.Annotations, err)
		}
		select {
		case <-time.After(time.Duration(15+rand.Intn(10)) * time.Second):
		}
	}
}
