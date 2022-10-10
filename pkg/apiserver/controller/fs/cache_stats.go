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

	log "github.com/sirupsen/logrus"
	k8sCore "k8s.io/api/core/v1"
	k8sMeta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

func scrapeCacheStats() error {
	crm, err := getClusterRuntimeMap()
	if err != nil {
		log.Errorf(fmt.Sprintf("scrapeCacheStats getClusterRuntimeMap err: %v", err))
		return err
	}

	for clusterID, k8sRuntime := range crm {
		go updateMountPodCacheStats(clusterID, k8sRuntime)
	}
	return err
}

func getClusterRuntimeMap() (map[string]*runtime.KubeRuntime, error) {
	crm := make(map[string]*runtime.KubeRuntime)
	clusters, err := storage.Cluster.ListCluster(0, 0, nil, "")
	if err != nil {
		err := fmt.Errorf("getClusterRuntimeMap list clusters err: %v", err)
		log.Errorf(err.Error())
		return nil, err
	}
	for _, cluster := range clusters {
		if cluster.ClusterType != schema.KubernetesType {
			log.Debugf("cluster[%s] type: %s, no need to delete pv pvc", cluster.Name, cluster.ClusterType)
			continue
		}
		runtimeSvc, err := runtime.GetOrCreateRuntime(cluster)
		if err != nil {
			log.Errorf("getClusterRuntimeMap: cluster[%s] GetOrCreateRuntime err: %v", cluster.Name, err)
			continue
		}
		crm[cluster.ID] = runtimeSvc.(*runtime.KubeRuntime)
	}
	return crm, nil
}

func updateMountPodCacheStats(clusterID string, k8sRuntime *runtime.KubeRuntime) error {
	// label indicating a mount pod
	label := csiconfig.PodTypeKey + "=" + csiconfig.PodMount
	// label indicating using cache
	label += "," + schema.LabelKeyCacheID
	listOptions := k8sMeta.ListOptions{
		LabelSelector: label,
		FieldSelector: "status.phase=Running",
	}
	pods, err := k8sRuntime.ListPods(schema.MountPodNamespace, listOptions)
	if err != nil {
		errRet := fmt.Errorf("list mount pods failed in cluster[%s]: %v", clusterID, err)
		log.Errorf(errRet.Error())
		return errRet
	}

	for _, pod := range pods.Items {
		if err = syncCacheFromMountPod(&pod, clusterID); err != nil {
			log.Errorf("syncCacheFromMountPod[%s] in cluster[%s] failed: %v", pod.Name, clusterID, err)
		}
	}
	return nil
}

func syncCacheFromMountPod(pod *k8sCore.Pod, clusterID string) error {
	fsCache := &model.FSCache{ClusterID: clusterID}
	for k, v := range pod.Labels {
		if k == schema.LabelKeyUsedSize {
			usedSize, err := strconv.Atoi(k)
			if err != nil {
				errRet := fmt.Errorf("mount pod[%s] used size %s failed to convert to int err: %v", pod.Name, v, err)
				log.Errorf(errRet.Error())
				return errRet
			}
			fsCache.UsedSize = usedSize
		}
		if k == schema.LabelKeyFsID {
			fsCache.FsID = v
		}
		if k == schema.LabelKeyNodeName {
			fsCache.NodeName = v
		}
		if k == schema.LabelKeyCacheID {
			fsCache.CacheID = v
		}
	}

	for k, v := range pod.Annotations {
		if k == schema.AnnotationKeyCacheDir {
			fsCache.CacheDir = v
			break
		}
	}

	if fsCache.FsID == "" ||
		fsCache.CacheID == "" ||
		fsCache.CacheDir == "" ||
		fsCache.NodeName == "" {
		errRet := fmt.Errorf("mount pod[%s] cache stats %+v is not valid", pod.Name, fsCache)
		log.Errorf(errRet.Error())
		return errRet
	}

	if err := addOrUpdateFSCache(fsCache); err != nil {
		errRet := fmt.Errorf("addOrUpdateFSCache[%+v] for pod[%s] failed: %v", *fsCache, pod.Name, err)
		log.Errorf(errRet.Error())
		return errRet
	}
	return nil
}

func addOrUpdateFSCache(fsCache *model.FSCache) error {
	n, err := storage.FsCache.Update(fsCache)
	if err != nil {
		log.Errorf("update fsCache[%+v] err:%v", *fsCache, err)
		return err
	}
	if n == 0 {
		err = storage.FsCache.Add(fsCache)
	}
	if err != nil {
		log.Errorf("add fsCache[%+v] err:%v", *fsCache, err)
		return err
	}
	return nil
}
