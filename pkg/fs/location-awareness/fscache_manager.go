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
	"os/exec"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

func PatchCacheStatsLoop(k8sClient utils.Client, podNamespace, podName, podCachePath string) {
	var errStat error
	var sizeUsed int
	for {
		if podCachePath != "" {
			sizeUsed, errStat = diskUse(podCachePath)
			if errStat != nil {
				log.Errorf("disk stat path[%s] and err[%v]", podCachePath, errStat)
				time.Sleep(1 * time.Second)
				continue
			}
		}

		// TODO memory, cpu stats

		pod, err := k8sClient.GetPod(podNamespace, podName)
		if err != nil {
			log.Errorf("Can't get mount pod %s: %v", podName, err)
			continue
		}

		pod.ObjectMeta.Labels[schema.LabelKeyUsedSize] = strconv.Itoa(sizeUsed)
		err = k8sClient.PatchPodLabel(pod)
		if err != nil {
			log.Errorf("PatchPodLabel %+v err[%v]", pod.ObjectMeta.Labels, err)
		}

		select {
		case <-time.After(time.Duration(600+rand.Intn(10)) * time.Second):
		}
	}
}

func diskUse(path string) (int, error) {
	cmd := exec.Command("du", "-sm", path)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Errorf("du -sm err %v", err)
		return 0, err
	}

	a := string(output)
	d := strings.Split(a, "\t")
	usedSize, err := strconv.Atoi(d[0])
	if err != nil {
		log.Errorf("du -sm atoi err %v", err)
	}
	return usedSize, err
}
