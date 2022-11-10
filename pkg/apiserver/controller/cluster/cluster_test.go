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

package cluster

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	MockRootUser    = "root"
	MockNonRootUser = "notroot"
	MockClusterName = "testCn"
)

func TestCreateCluster(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	createClusterReq := CreateClusterRequest{
		Name: MockClusterName,
	}

	createClusterReq.ID = ""
	createClusterReq.Description = ""
	resp, err := CreateCluster(ctx, &createClusterReq)
	assert.Equal(t, "Endpoint should not be empty", err.Error())

	createClusterReq.Endpoint = "127.0.0.1"
	createClusterReq.Source = ""
	createClusterReq.ClusterType = schema.KubernetesType
	createClusterReq.Version = "1.16"
	createClusterReq.Status = model.ClusterStatusOnLine
	createClusterReq.Credential = ""
	createClusterReq.Setting = ""
	createClusterReq.NamespaceList = []string{"n1", "n2"}
	// case1 test CreateCluster
	resp, err = CreateCluster(ctx, &createClusterReq)
	assert.NotNil(t, err)
	err = nil

	// case2 client init success
	rts := &runtime.KubeRuntime{}
	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "Init", func() error {
		return nil
	})
	defer p2.Reset()
	resp, err = CreateCluster(ctx, &createClusterReq)

	assert.Equal(t, createClusterReq.Status, resp.Status)
	t.Logf("resp=%v", resp)
}

func TestGetCluster(t *testing.T) {
	TestCreateCluster(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}

	// test get clusterInfo
	resp, err := GetCluster(ctx, MockClusterName)
	assert.Nil(t, err)
	// expect status changes from online to offline
	assert.Equal(t, MockClusterName, resp.Name)
	t.Logf("resp=%v", resp)
}

func TestUpdateCluster(t *testing.T) {
	TestCreateCluster(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}
	updateClusterRequest := UpdateClusterRequest{
		ClusterCommonInfo: ClusterCommonInfo{
			ClusterType: schema.LocalType,
		},
	}

	// test update clusterInfo
	resp, err := UpdateCluster(ctx, MockClusterName, &updateClusterRequest)
	assert.Nil(t, err)
	// expect status changes from online to offline
	assert.Equal(t, MockClusterName, resp.Name)
	t.Logf("resp=%v", resp)
}

func TestListCluster(t *testing.T) {
	TestCreateCluster(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}
	clusterNameList := []string{MockClusterName, "fake"}
	resp, err := ListCluster(ctx, "", 0, clusterNameList, "")
	assert.Nil(t, err)
	assert.NotZero(t, len(resp.ClusterList))
	// expect status changes from online to offline
	for _, cluster := range resp.ClusterList {
		assert.NotEmpty(t, cluster.Name)
		t.Logf("cluster=%v", cluster)
	}

}

func TestDeleteCluster(t *testing.T) {
	TestCreateCluster(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}
	err := DeleteCluster(ctx, MockClusterName)
	assert.Nil(t, err)
}

func TestListClusterQuotaByLabels(t *testing.T) {
	driver.InitMockCache()
	createMockClusters(t)
	type args struct {
		ctx *logger.RequestContext
		req ListClusterByLabelRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "nil request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockRootUser,
				},
				req: ListClusterByLabelRequest{},
			},
			wantErr: false,
		},
		{
			name: "non root query",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockNonRootUser,
				},
				req: ListClusterByLabelRequest{},
			},
			wantErr: true,
		},
		{
			name: "all clusters",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockRootUser,
				},
				req: ListClusterByLabelRequest{
					ClusterNameList: []string{MockClusterName},
					labels:          "",
					labelType:       "",
					pageNo:          1,
					pageSize:        2,
				},
			},
			wantErr: false,
		},
		{
			name: "filter by node label",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockRootUser,
				},
				req: ListClusterByLabelRequest{
					ClusterNameList: []string{MockClusterName},
					labels:          "a=b",
					labelType:       model.ObjectTypeNode,
					pageNo:          1,
					pageSize:        1,
				},
			},
			wantErr: false,
		},
	}
	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("no.%d name=%s args=[%#v], wantError=%v", index, tt.name, tt.args, tt.wantErr)
			response, err := ListClusterQuotaByLabels(tt.args.ctx, tt.args.req)
			t.Logf("case[%s] list cluster quota by labels, response=%+v", tt.name, response)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				obj, err := json.Marshal(response)
				assert.NoError(t, err)
				fmt.Println(string(obj))
				//t.Logf("response: %s", string(obj))
			}
		})
	}
}

func createMockClusters(t *testing.T) {
	mockNodeNum := 3
	mockPodNum := 3
	mockPodID := "mockPodID"
	mockNodeID := "mockNodeID"
	for i := 0; i < mockNodeNum; i++ {
		randNodeID := uuid.GenerateIDWithLength("mock", 5)
		randNodeName := fmt.Sprintf("%s-%s", mockNodeID, randNodeID)
		nodeInfo := &model.NodeInfo{
			ID:          randNodeID,
			Name:        randNodeName,
			ClusterID:   MockClusterName,
			ClusterName: MockClusterName,
			Status:      "Ready",
			Capacity: map[string]string{
				"cpu":    "20",
				"memory": "20Gi",
			},
		}
		if i%2 == 0 {
			nodeInfo.Labels = map[string]string{
				"a": "b",
			}
		}
		err := storage.NodeCache.AddNode(nodeInfo)
		assert.NoError(t, err)
		for j := 0; j < mockPodNum; j++ {
			randStr := uuid.GenerateIDWithLength("mock", 5)
			podName := fmt.Sprintf("%s-%s", mockPodID, randStr)
			resourceInfo := model.ResourceInfo{
				PodID:    podName,
				NodeID:   randNodeID,
				NodeName: randNodeName,
				Name:     "cpu",
				Value:    1000,
			}
			err = storage.ResourceCache.AddResource(&resourceInfo)
			assert.NoError(t, err)
			resourceInfo2 := model.ResourceInfo{
				PodID:    podName,
				NodeID:   randNodeID,
				NodeName: randNodeName,
				Name:     "memory",
				Value:    1024,
			}
			err = storage.ResourceCache.AddResource(&resourceInfo2)
			assert.NoError(t, err)
		}
	}

}
