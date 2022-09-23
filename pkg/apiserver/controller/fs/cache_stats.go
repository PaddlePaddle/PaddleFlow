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
	"encoding/json"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	k8sMeta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

func ScrapeCacheStatsLoop(scrapeCacheInterval time.Duration) {
	for {
		if err := scrapeCacheStats(); err != nil {
			log.Errorf("scrapeCacheStats err: %v", err)
		}
		time.Sleep(scrapeCacheInterval)
	}
}

func scrapeCacheStats() error {
	crm, err := getClusterRuntimeMap()
	if err != nil {
		log.Errorf(fmt.Sprintf("scrapeCacheStats getClusterRuntimeMap err: %v", err))
		return err
	}
	return updateMountPodsCacheStats(crm)
}

func addOrUpdateFsCache(fsCache *model.FSCache) error {
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

func updateMountPodsCacheStats(crm map[string]*runtime.KubeRuntime) error {
	for clusterID, k8sRuntime := range crm {
		listOptions := k8sMeta.ListOptions{
			LabelSelector: fmt.Sprintf(csiconfig.PodTypeKey + "=" + csiconfig.PodMount),
		}
		pods, err := k8sRuntime.ListPods(schema.MountPodNamespace, listOptions)
		if err != nil {
			log.Errorf("list mount pods failed: %v", err)
			return err
		}

		for _, pod := range pods.Items {
			for k, v := range pod.Annotations {
				if k == schema.AnnotationKeyCache {
					log.Debugf("mount pod %s has cache stats: %s", pod.Name, v)
					var stats model.CacheStats
					if err = json.Unmarshal([]byte(v), &stats); err != nil {
						errRet := fmt.Errorf("unmarshal cache stats %s from pod[%s] failed: %v", v, pod.Name, err)
						log.Errorf(errRet.Error())
						return errRet
					}

					fsCache := &model.FSCache{
						FsID:      stats.FsID,
						CacheDir:  stats.CacheDir,
						NodeName:  stats.NodeName,
						UsedSize:  stats.UsedSize,
						ClusterID: clusterID,
					}
					if err = addOrUpdateFsCache(fsCache); err != nil {
						errRet := fmt.Errorf("addOrUpdateFsCache[%+v] failed: %v", *fsCache, err)
						log.Errorf(errRet.Error())
						return errRet
					}
					break
				}
			}
		}
	}
	return nil
}

func getClusterRuntimeMap() (map[string]*runtime.KubeRuntime, error) {
	crm := make(map[string]*runtime.KubeRuntime)
	clusters, err := storage.Cluster.ListCluster(0, 0, nil, "")
	if err != nil {
		err := fmt.Errorf("list clusters err: %v", err)
		log.Errorf("getClusterNamespaceMap failed: %v", err)
		return nil, err
	}
	for _, cluster := range clusters {
		if cluster.ClusterType != schema.KubernetesType {
			log.Debugf("cluster[%s] type: %s, no need to delete pv pvc", cluster.Name, cluster.ClusterType)
			continue
		}
		runtimeSvc, err := runtime.GetOrCreateRuntime(cluster)
		if err != nil {
			err := fmt.Errorf("getClusterNamespaceMap: cluster[%s] GetOrCreateRuntime err: %v", cluster.Name, err)
			log.Errorf(err.Error())
			return nil, err
		}
		crm[cluster.ID] = runtimeSvc.(*runtime.KubeRuntime)
	}
	return crm, nil
}
