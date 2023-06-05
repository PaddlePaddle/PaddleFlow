/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func TestResourcePoolStore_Op(t *testing.T) {
	resources, err := resources.NewResourceFromMap(map[string]string{
		"cpu":            "100",
		"memory":         "200Gi",
		"nvidia.com/gpu": "120",
	})
	assert.Equal(t, nil, err)

	initMockDB()
	err = Cluster.CreateCluster(&model.ClusterInfo{
		Name:   "test_cluster",
		Status: model.ClusterStatusOnLine,
	})
	rp := &model.ResourcePool{
		Name:           "test-rp-name",
		Namespace:      "default",
		TotalResources: model.Resource(*resources),
	}
	t.Run("test resource pool op", func(t *testing.T) {
		err = ResourcePool.Create(rp)
		assert.Equal(t, nil, err)

		// get resource pool
		newRP, err := ResourcePool.Get(rp.Name)
		assert.Equal(t, nil, err)
		assert.Equal(t, rp.TotalResources, newRP.TotalResources)
		t.Logf("get resource pool: %v", newRP)

		// list resource pool
		rps, err := ResourcePool.List(0, 100, map[string]string{})
		assert.Equal(t, nil, err)
		t.Logf("list resource pool: %v", rps)

		// update resource pool
		rp.Namespace = "test-ns"
		err = ResourcePool.Update(rp)
		assert.Equal(t, nil, err)
		newRP, err = ResourcePool.Get(rp.Name)
		assert.Equal(t, nil, err)
		t.Logf("update resource pool: %v", newRP)

		// delete resource pool
		err = ResourcePool.Delete(rp.Name)
		assert.Equal(t, nil, err)
	})
}
