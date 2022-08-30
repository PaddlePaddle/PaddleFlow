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
	"fmt"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	fsLocationAwarenessKey    = "kubernetes.io/hostname"
	fsLocationAwarenessWeight = 100
)

func FsNodeAffinity(fsIDs []string) (*corev1.Affinity, error) {
	exp := make([]corev1.NodeSelectorRequirement, 0)
	// existing cache location
	nodes, err := storage.FsCache.ListNodes(fsIDs)
	if err != nil {
		err := fmt.Errorf("FsNodeAffinity %v ListNodes err:%v", fsIDs, err)
		log.Errorf(err.Error())
		return nil, err
	}
	if len(nodes) > 0 {
		a := corev1.NodeSelectorRequirement{
			Key:      fsLocationAwarenessKey,
			Operator: corev1.NodeSelectorOpIn,
			Values:   nodes,
		}
		exp = append(exp, a)
	}

	// node label
	cacheConfs, err := storage.Filesystem.ListFSCacheConfig(fsIDs)
	if err != nil {
		err := fmt.Errorf("FsNodeAffinity %v ListFSCacheConfig err:%v", fsIDs, err)
		log.Errorf(err.Error())
		return nil, err
	}

	for _, conf := range cacheConfs {
		for affKey, affValue := range conf.NodeAffinityMap {
			if len(affValue) > 0 {
				a := corev1.NodeSelectorRequirement{
					Key:      affKey,
					Operator: corev1.NodeSelectorOpIn,
					Values:   affValue,
				}
				exp = append(exp, a)
			}
		}
	}

	if len(exp) == 0 {
		err := fmt.Errorf("fsIDs: %v have no node affinity", fsIDs)
		log.Warnf(err.Error())
		return nil, err
	}

	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{
				{
					Weight: fsLocationAwarenessWeight,
					Preference: corev1.NodeSelectorTerm{
						MatchExpressions: exp,
					},
				},
			},
		},
	}, nil
}
