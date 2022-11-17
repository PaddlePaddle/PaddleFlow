package cluster

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestListClusterQuotaByLabels(t *testing.T) {
	driver.InitCache("DEBUG")
	createMockClusters(t, 10, 10, 10)
	type args struct {
		ctx *logger.RequestContext
		req ListClusterResourcesRequest
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
				req: ListClusterResourcesRequest{
					PageSize: 2,
					PageNo:   1,
				},
			},
			wantErr: false,
		},
		{
			name: "non root query",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockNonRootUser,
				},
				req: ListClusterResourcesRequest{
					PageNo:   1,
					PageSize: 2,
				},
			},
			wantErr: true,
		},
		{
			name: "all clusters",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockRootUser,
				},
				req: ListClusterResourcesRequest{
					ClusterNameList: []string{fmt.Sprintf("%s1", MockClusterName)},
					Labels:          "",
					LabelType:       "",
					PageNo:          1,
					PageSize:        2,
				},
			},
			wantErr: false,
		},
		{
			name: "all clusters with wrong PageNo",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockRootUser,
				},
				req: ListClusterResourcesRequest{
					ClusterNameList: []string{fmt.Sprintf("%s1", MockClusterName)},
					Labels:          "",
					LabelType:       "",
					PageNo:          1000,
					PageSize:        2,
				},
			},
			wantErr: true,
		},
		{
			name: "all clusters with large PageSize",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockRootUser,
				},
				req: ListClusterResourcesRequest{
					ClusterNameList: []string{fmt.Sprintf("%s1", MockClusterName),
						fmt.Sprintf("%s2", MockClusterName),
						fmt.Sprintf("%s3", MockClusterName)},
					Labels:    "",
					LabelType: "",
					PageNo:    1,
					PageSize:  1000,
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
				req: ListClusterResourcesRequest{
					ClusterNameList: []string{},
					Labels:          "a=b",
					LabelType:       model.ObjectTypeNode,
					PageNo:          1,
					PageSize:        1,
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
				t.Logf("response: %s", string(obj))
			}
		})
	}
}

func createMockClusters(t *testing.T, mockClusterNum, mockNodeNum, mockPodNum int) {
	for i := 0; i < mockClusterNum; i++ {
		mockClusterID := fmt.Sprintf("%s%d", MockClusterName, i)
		createMockNode(t, mockClusterID, mockNodeNum, mockPodNum)
	}
}

func createMockNode(t *testing.T, clusterName string, mockNodeNum, mockPodNum int) {
	mockNodeID := uuid.GenerateIDWithLength("nodeID", 5)
	for i := 0; i < mockNodeNum; i++ {
		randNodeID := uuid.GenerateIDWithLength("mock", 5)
		randNodeName := fmt.Sprintf("%s-%s", mockNodeID, randNodeID)
		nodeInfo := &model.NodeInfo{
			ID:          randNodeID,
			Name:        randNodeName,
			ClusterID:   clusterName,
			ClusterName: clusterName,
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
			createMockPodInfo(t, randNodeID, randNodeName)
		}
	}

}

func createMockPodInfo(t *testing.T, randNodeID, randNodeName string) {
	mockPodID := uuid.GenerateIDWithLength("podID", 5)
	randStr := uuid.GenerateIDWithLength("mock", 5)
	podName := fmt.Sprintf("%s-%s", mockPodID, randStr)
	resourceInfo := model.ResourceInfo{
		PodID:    podName,
		NodeID:   randNodeID,
		NodeName: randNodeName,
		Name:     "cpu",
		Value:    1000,
	}
	err := storage.ResourceCache.AddResource(&resourceInfo)
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
