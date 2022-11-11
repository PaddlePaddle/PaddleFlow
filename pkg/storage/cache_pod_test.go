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

const (
	MockClusterName = "fakeClusterName"
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
		Labels: map[string]string{
			"xxx/queue-name": "test-queue",
		},
		Resources: map[string]int64{
			"cpu":            1000,
			"memory":         1 * 1024 * 1024 * 1024,
			"nvidia.com/gpu": 1,
		},
	})
	assert.Equal(t, nil, err)

	updatedPod1 := &model.PodInfo{
		Status: "Terminating",
		Labels: map[string]string{
			"xxx/queue-name": "default-queue",
		},
	}
	err = PodCache.UpdatePod(mockPodID, updatedPod1)
	assert.Equal(t, nil, err)

	updatedPod2 := &model.PodInfo{
		Resources: map[string]int64{
			"cpu":            2000,
			"memory":         2 * 1024 * 1024 * 1024,
			"nvidia.com/gpu": 1,
		},
	}
	err = PodCache.UpdatePodResources(mockPodID, updatedPod2)
	assert.Equal(t, nil, err)

	err = PodCache.DeletePod(mockPodID)
	assert.Equal(t, nil, err)
}

func TestPodResourceCache(t *testing.T) {
	initMockCache()
	mockNodeID := "test-node-id"
	mockNodeName := "test-node-name"
	mockPodID := "test-pod-id"

	err := ResourceCache.AddResource(&model.ResourceInfo{
		PodID:    mockPodID,
		NodeID:   mockNodeID,
		NodeName: mockNodeName,
		Name:     "cpu",
		Value:    1000,
	})
	assert.Equal(t, nil, err)

	err = ResourceCache.UpdateResource(mockPodID, "cpu", &model.ResourceInfo{Value: 2000})
	assert.Equal(t, nil, err)
}

func TestResourceList(t *testing.T) {
	initMockCache()

	type args struct {
		ClusterNameList []string
		labels          string
		labelType       string
		pageNo          int
		pageSize        int
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "nil request",
			args:    args{},
			wantErr: false,
		},
		{
			name: "all clusters",
			args: args{
				ClusterNameList: []string{MockClusterName},
				labels:          "",
				labelType:       "",
				pageNo:          1,
				pageSize:        2,
			},
			wantErr: false,
		},
		{
			name: "filter by node label",
			args: args{
				ClusterNameList: []string{MockClusterName},
				labels:          "a=b",
				labelType:       model.ObjectTypeNode,
				pageNo:          1,
				pageSize:        1,
			},
			wantErr: false,
		},
		{
			name: "filter by wrong label",
			args: args{
				ClusterNameList: []string{MockClusterName},
				labels:          "a=b",
				labelType:       "fake label",
				pageNo:          1,
				pageSize:        1,
			},
			wantErr: true,
		},
		{
			name: "filter by pod label",
			args: args{
				ClusterNameList: []string{MockClusterName},
				labels:          "a=b",
				labelType:       model.ObjectTypePod,
				pageNo:          1,
				pageSize:        1,
			},
			wantErr: false,
		},
	}
	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("no.%d name=%s args=[%#v], wantError=%v", index, tt.name, tt.args, tt.wantErr)
			response, err := ResourceCache.ListResouces(tt.args.ClusterNameList,
				tt.args.labels,
				tt.args.labelType)
			t.Logf("case[%s] list cluster quota by labels, response=%+v", tt.name, response)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				t.Logf("response: %+v", response)
			}
		})
	}

}
