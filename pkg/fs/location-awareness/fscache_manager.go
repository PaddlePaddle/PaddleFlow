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
	"math/rand"
	"strconv"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
	log "github.com/sirupsen/logrus"
	k8sCore "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/mount"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

func PatchCacheStatsLoop(k8sClient utils.Client, pod *k8sCore.Pod, hasCachePath bool) {
	var errStat, err error
	var usageStat *disk.UsageStat
	var sizeUsed string = "0"
	for {
		if hasCachePath {
			usageStat, errStat = disk.Usage(mount.FusePodCachePath)
			if errStat != nil {
				log.Errorf("disk stat path[%s] and err[%v]", mount.FusePodCachePath, errStat)
				time.Sleep(1 * time.Second)
				continue
			}
			sizeUsed = strconv.Itoa(int(usageStat.Used / 1024))

			pod.ObjectMeta.Labels[schema.KeyUsedSize] = sizeUsed
			err = k8sClient.PatchPodLabel(pod)
			if err != nil {
				log.Errorf("PatchPodLabel %+v err[%v]", pod.Labels, err)
			}
		}

		select {
		case <-time.After(time.Duration(15+rand.Intn(10)) * time.Second):
		}
	}
}
