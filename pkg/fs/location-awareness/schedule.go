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

// FsNodeAffinity if no node affinity, return nil, nil
func FsNodeAffinity(fsIDs []string) (*corev1.Affinity, error) {
	nodeAffinity := &corev1.NodeAffinity{}
	// cached node preferred
	preferred := make([]corev1.PreferredSchedulingTerm, 0)
	nodes, err := storage.FsCache.ListNodes(fsIDs)
	if err != nil {
		err := fmt.Errorf("FsNodeAffinity %v ListNodes err:%v", fsIDs, err)
		log.Errorf(err.Error())
		return nil, err
	}
	if len(nodes) > 0 {
		matchExpression := corev1.NodeSelectorRequirement{
			Key:      fsLocationAwarenessKey,
			Operator: corev1.NodeSelectorOpIn,
			Values:   nodes,
		}
		preferredSchedulingTerm := corev1.PreferredSchedulingTerm{
			Weight: fsLocationAwarenessWeight,
			Preference: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{matchExpression},
			},
		}
		preferred = append(preferred, preferredSchedulingTerm)
	}

	// user set affinity
	cacheConfs, err := storage.Filesystem.ListFSCacheConfig(fsIDs)
	if err != nil {
		err := fmt.Errorf("FsNodeAffinity %v ListFSCacheConfig err:%v", fsIDs, err)
		log.Errorf(err.Error())
		return nil, err
	}
	required := make([]corev1.NodeSelectorTerm, 0)
	for _, conf := range cacheConfs {
		preferredTerms := conf.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
		if len(preferredTerms) > 0 {
			preferred = append(preferred, preferredTerms...)
		}
		requiredTerms := conf.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if requiredTerms != nil && len(requiredTerms.NodeSelectorTerms) > 0 {
			required = append(required, requiredTerms.NodeSelectorTerms...)
		}
	}

	if len(required) == 0 && len(preferred) == 0 {
		log.Warnf("FsNodeAffinity %v has no node affinity", fsIDs)
		return nil, nil
	}

	if len(required) > 0 {
		nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{NodeSelectorTerms: required}
	}
	if len(preferred) > 0 {
		nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = preferred
	}
	return &corev1.Affinity{NodeAffinity: nodeAffinity}, nil
}
