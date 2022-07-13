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

package fs

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	k8sCore "k8s.io/api/core/v1"
	k8sMeta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
)

type MountPodController struct {
	MountPodExpire            time.Duration
	CleanMountPodIntervalTime time.Duration
}

var mountPodSync sync.Once
var mountPodController *MountPodController

func GetMountPodController(mountPodExpire, cleanMountPodIntervalTime time.Duration) *MountPodController {
	mountPodSync.Do(func() {
		controller := MountPodController{
			MountPodExpire:            mountPodExpire,
			CleanMountPodIntervalTime: cleanMountPodIntervalTime,
		}
		mountPodController = &controller
	})
	return mountPodController
}

func (m *MountPodController) CleanMountPodController(stopChan chan struct{}) {
	interval := m.CleanMountPodIntervalTime * time.Second
	for {
		if err := cleanMountPod(m.MountPodExpire); err != nil {
			log.Errorf("clean mount pod err: %v", err)
		}
		select {
		case <-stopChan:
			log.Info("mount pod controller stopped")
			return
		default:
			time.Sleep(interval)
		}
	}
}

func cleanMountPod(mountPodExpire time.Duration) error {
	// check k8s mount pods
	clusters, err := getClusterNamespaceMap()
	if err != nil {
		log.Errorf(fmt.Sprintf("clean mount pods getClusterNamespaceMap err: %v", err))
		return err
	}
	deleteMountPodsMap, err := listNotUsedAndExpireMountPods(clusters, mountPodExpire)
	if err != nil {
		log.Errorf(fmt.Sprintf("clean mount pods listNotUsedAndExpireMountPods err: %v", err))
		return err
	}
	log.Debugf("delete Mount pods map %v", deleteMountPodsMap)
	if err = deleteMountPods(deleteMountPodsMap); err != nil {
		log.Errorf(fmt.Sprintf("clean mount pods with err: %v", err))
		return err
	}
	if err = cleanFsCache(deleteMountPodsMap); err != nil {
		log.Errorf(fmt.Sprintf("clean fs cache with err: %v", err))
		return err
	}
	return nil
}

func listNotUsedAndExpireMountPods(clusterMaps map[*runtime.KubeRuntime][]string, mountPodExpire time.Duration) (map[*runtime.KubeRuntime][]k8sCore.Pod, error) {
	clusterPodMap := make(map[*runtime.KubeRuntime][]k8sCore.Pod)
	now := time.Now()
	for k8sRuntime, _ := range clusterMaps {
		k8sRuntime.Name()
		listOptions := k8sMeta.ListOptions{
			LabelSelector: fmt.Sprintf(csiconfig.PodTypeKey + "=" + csiconfig.PodMount),
		}
		pods, err := k8sRuntime.ListPods(schema.MountPodNamespace, listOptions)
		if err != nil {
			log.Errorf("list mount pods failed: %v", err)
			return nil, err
		}

		var needToDelete bool
		for _, pod := range pods.Items {
			needToDelete = true
			for key, _ := range pod.Annotations {
				if key != schema.AnnotationKeyMTime {
					needToDelete = false
					break
				} else {
					modifyTime, errParseTime := time.Parse("2006-01-02 15:04:05", pod.Annotations[key])
					if errParseTime != nil {
						log.Errorf("parse time err: %v", err)
						return nil, errParseTime
					}
					expireTime := modifyTime.Add(mountPodExpire)
					if expireTime.After(now) {
						needToDelete = false
					}
				}
			}
			if needToDelete {
				clusterPodMap[k8sRuntime] = append(clusterPodMap[k8sRuntime], pod)
			}
		}
	}
	return clusterPodMap, nil
}
