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

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func TestPodCache(t *testing.T) {
	initMockCache()
	mockNodeID := "test-node-id"
	mockPodID := "test-pod-id"

	err := PodCache.AddPod(&model.PodInfo{
		ID:     mockPodID,
		Name:   "test-pod-name",
		NodeID: mockNodeID,
		Status: "Running",
	})
	assert.Equal(t, nil, err)

	err = PodCache.UpdatePod(mockPodID, &model.PodInfo{Status: "Terminating"})
	assert.Equal(t, nil, err)

	err = PodCache.DeletePod(mockPodID)
	assert.Equal(t, nil, err)
}
