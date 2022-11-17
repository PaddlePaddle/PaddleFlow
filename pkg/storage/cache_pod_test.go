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
		Status: int(model.TaskRunning),
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
		Status: int(model.TaskTerminating),
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

func TestPodResourceCache_ListNodeResources(t *testing.T) {
	initMockCache()

	testCases := []struct {
		name     string
		nodeIDs  []string
		podInfos []model.PodInfo
		count    int
		err      error
	}{
		{
			name:    "2 nodes",
			nodeIDs: []string{"node-instance-1", "node-instance-2"},
			podInfos: []model.PodInfo{
				{
					ID:       "test-pod-1",
					Name:     "test-pod-n-1",
					NodeID:   "node-instance-1",
					NodeName: "instance-1",
					Status:   int(model.TaskCreating),
					Resources: map[string]int64{
						"cpu":            10000,
						"memory":         3618635776,
						"nvidia.com/gpu": 4,
					},
				},
				{
					ID:       "test-pod-2",
					Name:     "test-pod-n-2",
					NodeID:   "node-instance-2",
					NodeName: "instance-2",
					Status:   int(model.TaskRunning),
					Resources: map[string]int64{
						"cpu":            4000,
						"memory":         1073741824,
						"nvidia.com/gpu": 1,
					},
				},
				{
					ID:       "test-pod-3",
					Name:     "test-pod-n-3",
					NodeID:   "node-instance-1",
					NodeName: "instance-1",
					Status:   int(model.TaskTerminating),
					Resources: map[string]int64{
						"cpu":            20000,
						"memory":         3618635776,
						"nvidia.com/gpu": 4,
					},
				},
			},
			count: 6,
			err:   nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			var rs []model.ResourceInfo
			for idx := range tc.podInfos {
				err = PodCache.AddPod(&tc.podInfos[idx])
				assert.Equal(t, nil, err)
			}
			rs, err = ResourceCache.ListNodeResources(tc.nodeIDs)
			assert.Equal(t, tc.err, err)
			assert.Equal(t, tc.count, len(rs))
		})
	}

}
