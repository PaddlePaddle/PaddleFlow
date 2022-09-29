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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func PatchCacheStatsLoop(k8sClient utils.Client, fsID, cacheDir, nodname, podNamespace, podName, podCachePath string) {
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

		cacheAnnotation := map[string]string{schema.AnnotationKeyCache: string(str)}
		err = patchAddAnnotation(k8sClient, podNamespace, podName, cacheAnnotation)
		if err != nil {
			log.Errorf("mount pod %s patchAddAnnotation err: %v", podName, err)
			continue
		}

		select {
		case <-time.After(time.Duration(15+rand.Intn(10)) * time.Second):
		}
	}
}

func patchAddAnnotation(k8sClient utils.Client, podNamespace, podName string, anno map[string]string) error {
	payload := []utils.PatchMapValue{{
		Op:    "add",
		Path:  "/metadata/annotations",
		Value: anno,
	}}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Errorf("patchAddAnnotation: parse pod[%s] anno json error: %v", podName, err)
		return err
	}
	if err := k8sClient.PatchPod(podNamespace, podName, payloadBytes); err != nil {
		log.Errorf("patchAddAnnotation: patch pod[%s] anno [%s] error: %v", podName, string(payloadBytes), err)
		return err
	}
	return nil
}
