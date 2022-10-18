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
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	k8sCore "k8s.io/api/core/v1"
	k8sMeta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
)

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
	if err = cleanFSCache(deleteMountPodsMap); err != nil {
		log.Errorf(fmt.Sprintf("clean fs cache with err: %v", err))
		return err
	}
	return nil
}

func listNotUsedAndExpireMountPods(clusterMaps map[*runtime.KubeRuntime][]string, mountPodExpire time.Duration) (map[*runtime.KubeRuntime][]k8sCore.Pod, error) {
	clusterPodMap := make(map[*runtime.KubeRuntime][]k8sCore.Pod)
	for k8sRuntime, _ := range clusterMaps {
		listOptions := k8sMeta.ListOptions{
			LabelSelector: fmt.Sprintf(csiconfig.PodTypeKey + "=" + csiconfig.PodMount),
		}
		pods, err := k8sRuntime.ListPods(schema.MountPodNamespace, listOptions)
		if err != nil {
			log.Errorf("list mount pods failed: %v", err)
			return nil, err
		}

		for _, pod := range pods.Items {
			log.Debugf("list pod %+v", pod)
			if mountPodExpired(pod.Name, pod.Labels, mountPodExpire) {
				clusterPodMap[k8sRuntime] = append(clusterPodMap[k8sRuntime], pod)
			}
		}
	}
	return clusterPodMap, nil
}

func mountPodExpired(podName string, labels map[string]string, mountPodExpire time.Duration) bool {
	log.Debugf("check whether expired pod: %s", podName)
	for key, _ := range labels {
		if strings.HasPrefix(key, schema.KeyMountPrefix) {
			// is mounted by job pod
			return false
		}
	}

	i, errParseTime := strconv.ParseInt(labels[schema.KeyModifiedTime], 10, 64)
	if errParseTime != nil {
		log.Errorf("mount pod[%s] parse time from label %s err: %v", podName, labels[schema.KeyModifiedTime], errParseTime)
		return false
	}
	modifyTime := time.Unix(i, 0)

	expireTime := modifyTime.Add(mountPodExpire)
	log.Debugf("time fs modifyTime %v and expireTime %v and now %v", modifyTime, expireTime, time.Now())
	if expireTime.After(time.Now()) {
		return false
	} else {
		log.Debugf("needToDelete mount pod %v", podName)
		return true
	}
}
