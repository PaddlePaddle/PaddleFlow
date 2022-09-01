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
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestFsNodeAffinity(t *testing.T) {
	driver.InitMockDB()
	// mock cache recods
	fsID1, fsID2, cacheDir1, cacheDir2, nodeName1, nodeName2, clusterID :=
		"fs-root-1", "fs-root-2", "/mnt/fs-root-1/storage", "/mnt/fs-root-2/storage", "node1", "node2", ""
	cache := &model.FSCache{
		FsID:      fsID1,
		CacheDir:  cacheDir1,
		NodeName:  nodeName1,
		ClusterID: clusterID,
	}
	err := storage.FsCache.Add(cache)
	assert.Nil(t, err)

	cache = &model.FSCache{
		FsID:      fsID1,
		CacheDir:  cacheDir1,
		NodeName:  nodeName2,
		ClusterID: clusterID,
	}
	err = storage.FsCache.Add(cache)
	assert.Nil(t, err)

	cache = &model.FSCache{
		FsID:      fsID2,
		CacheDir:  cacheDir2,
		NodeName:  nodeName1,
		ClusterID: clusterID,
	}
	err = storage.FsCache.Add(cache)
	assert.Nil(t, err)

	fsIDs := []string{fsID1, fsID2, "fs-non-exist"}
	affinity, err := FsNodeAffinity(fsIDs)
	assert.Nil(t, err)
	pref := affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	assert.Equal(t, 1, len(pref))
	exp := pref[0].Preference.MatchExpressions
	assert.Equal(t, 1, len(exp))
	assert.Equal(t, 2, len(exp[0].Values))

	cacheConf := &model.FSCacheConfig{
		FsID:         fsID1,
		NodeAffinity: nodeAffinity(),
	}
	err = storage.Filesystem.CreateFSCacheConfig(cacheConf)
	assert.Nil(t, err)

	affinity, err = FsNodeAffinity(fsIDs)
	assert.Nil(t, err)
	pref = affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	assert.Equal(t, 2, len(pref))
	exp = pref[1].Preference.MatchExpressions
	assert.Equal(t, 1, len(exp))
	assert.Equal(t, 2, len(exp[0].Values))
	required := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	assert.Equal(t, 1, len(required))
	exp = pref[0].Preference.MatchExpressions
	assert.Equal(t, 1, len(exp))
	assert.Equal(t, 2, len(exp[0].Values))
	aff := nodeAffinity()
	fmt.Printf("%+v", aff)
}

func nodeAffinity() v1.NodeAffinity {
	nodeSelectorRequirement := v1.NodeSelectorRequirement{
		Key:      "MatchFields",
		Operator: v1.NodeSelectorOpIn,
		Values:   []string{"giraffe", "penguin"},
	}
	nodeSelectorTerm := v1.NodeSelectorTerm{
		MatchExpressions: []v1.NodeSelectorRequirement{nodeSelectorRequirement},
		MatchFields:      []v1.NodeSelectorRequirement{nodeSelectorRequirement},
	}
	nodeAffinity := v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{nodeSelectorTerm},
		},
		PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
			{
				Weight:     fsLocationAwarenessWeight,
				Preference: nodeSelectorTerm,
			},
		},
	}
	return nodeAffinity
}
