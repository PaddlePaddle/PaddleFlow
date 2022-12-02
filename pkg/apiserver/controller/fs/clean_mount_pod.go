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
	"time"

	log "github.com/sirupsen/logrus"
	k8sCore "k8s.io/api/core/v1"
	k8sMeta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

func cleanMountPod(expireDuration time.Duration) error {
	// check k8s mount pods
	clusters, err := storage.Cluster.ListCluster(0, 0, nil, "")
	if err != nil {
		err := fmt.Errorf("list clusters err: %v", err)
		log.Errorf("getClusterNamespaceMap failed: %v", err)
		return err
	}
	if len(clusters) == 0 {
		return nil
	}
	podCleanMap := make(map[*runtime.KubeRuntime][]k8sCore.Pod, 0)
	for _, cluster := range clusters {
		runtimePtr, podsToClean, err := expiredMountedPodsSingleCluster(cluster, expireDuration)
		if err != nil {
			log.Errorf("expiredMountedPodsSingleCluster[%s] failed: %v", cluster.ID, err)
			return err
		}
		if len(podsToClean) > 0 {
			podCleanMap[runtimePtr] = podsToClean
		}
	}
	log.Infof("clean expired mount pods: %+v", podCleanMap)
	if len(podCleanMap) == 0 {
		return nil
	}
	if err = deleteMountPods(podCleanMap); err != nil {
		log.Errorf(fmt.Sprintf("clean mount pods with err: %v", err))
		return err
	}
	if err = cleanFSCache(podCleanMap); err != nil {
		log.Errorf(fmt.Sprintf("clean fs cache with err: %v", err))
		return err
	}
	return nil
}

func expiredMountedPodsSingleCluster(cluster model.ClusterInfo, expireDuration time.Duration) (*runtime.KubeRuntime, []k8sCore.Pod, error) {
	if cluster.ClusterType != schema.KubernetesType {
		log.Debugf("cluster[%s] type: %s, no need to investigate", cluster.Name, cluster.ClusterType)
		return nil, nil, nil
	}
	runtimeSvc, err := runtime.GetOrCreateRuntime(cluster)
	if err != nil {
		err := fmt.Errorf("cluster[%s] GetOrCreateRuntime err: %v", cluster.Name, err)
		log.Errorf(err.Error())
		return nil, nil, err
	}
	k8sRuntime := runtimeSvc.(*runtime.KubeRuntime)
	// label indicating a mount pod
	label := csiconfig.PodTypeKey + "=" + csiconfig.PodMount
	listOptions := k8sMeta.ListOptions{
		LabelSelector: label,
	}
	pods, err := k8sRuntime.ListPods(schema.MountPodNamespace, listOptions)
	if err != nil {
		log.Errorf("list mount pods failed: %v", err)
		return nil, nil, err
	}
	podsToClean := make([]k8sCore.Pod, 0)
	for _, po := range pods.Items {
		if checkMountPodMounted(po) {
			continue
		}
		expired, err := checkMountPodExpired(po, expireDuration)
		if err != nil {
			log.Errorf("checkMountPodExpired[%s] failed: %v", po.Name, err)
			return nil, nil, err
		}
		if expired {
			podsToClean = append(podsToClean, po)
		}
	}
	if len(podsToClean) > 0 {
		log.Debugf("cluster[%s] has expired mount pods to clean: %v", cluster.ID, podsToClean)
		return k8sRuntime, podsToClean, nil
	} else {
		log.Debugf("cluster[%s] has no expired mount pods to clean", cluster.ID)
		return nil, nil, nil
	}

}

func checkMountPodExpired(po k8sCore.Pod, expireDuration time.Duration) (bool, error) {
	modifiedTimeStr := po.Annotations[schema.AnnotationKeyMTime]
	modifyTime, errParseTime := time.Parse(model.TimeFormat, modifiedTimeStr)
	if errParseTime != nil {
		errRet := fmt.Errorf("checkMountPodExpired: pod [%s] parse time str [%s] err: %v", po.Name, modifiedTimeStr, errParseTime)
		log.Errorf(errRet.Error())
		return false, errRet
	}
	now_ := time.Now().Format(model.TimeFormat)
	now, _ := time.Parse(model.TimeFormat, now_)
	log.Infof("modifyTime %v, expireDuration %v and nowTime %v", modifyTime, expireDuration, now)
	if modifyTime.Add(expireDuration).Before(now) {
		log.Infof("pod %s expired", po.Name)
		return true, nil
	} else {
		return false, nil
	}
}
