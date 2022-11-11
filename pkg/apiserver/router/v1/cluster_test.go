package v1

import (
	"fmt"
	"testing"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/cluster"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestListClusterQuotaV2(t *testing.T) {
	router, routerNonRoot, baseURL := MockInitJob(t)
	driver.InitCache("debug")
	createMockClusters(t, 10, 10, 10)
	type args struct {
		ctx    *logger.RequestContext
		req    interface{}
		router *chi.Mux
	}
	ctx := &logger.RequestContext{UserName: "testusername"}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "empty request",
			args: args{
				ctx:    ctx,
				router: router,
				req:    &cluster.ListClusterResourcesRequest{},
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "wrong request",
			args: args{
				ctx:    ctx,
				router: router,
				req:    "abc",
			},
			wantErr:      false,
			responseCode: 400,
		},
		{
			name: "routerNonRoot",
			args: args{
				ctx:    ctx,
				router: routerNonRoot,
				req: &cluster.ListClusterResourcesRequest{
					PageSize: 1,
					PageNo:   1,
				},
			},
			wantErr:      false,
			responseCode: 403,
		},
		{
			name: "PageSize and PageNo",
			args: args{
				ctx:    ctx,
				router: router,
				req: &cluster.ListClusterResourcesRequest{
					PageSize: 1,
					PageNo:   1,
				},
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "normal",
			args: args{
				ctx:    ctx,
				router: router,
				req: &cluster.ListClusterResourcesRequest{
					PageSize:        1,
					PageNo:          1,
					ClusterNameList: []string{},
					Labels:          "a=b",
					LabelType:       model.ObjectTypeNode,
				},
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "wrong labelType",
			args: args{
				ctx:    ctx,
				router: router,
				req: &cluster.ListClusterResourcesRequest{
					PageSize:        1,
					PageNo:          1,
					ClusterNameList: []string{},
					Labels:          "a=b",
				},
			},
			wantErr:      false,
			responseCode: 400,
		},
		{
			name: "normal",
			args: args{
				ctx:    ctx,
				router: router,
				req: &cluster.ListClusterResourcesRequest{
					PageSize:        1,
					PageNo:          1,
					ClusterNameList: []string{},
					Labels:          "a=b",
					LabelType:       model.ObjectTypePod,
				},
			},
			wantErr:      false,
			responseCode: 200,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := PerformPostRequest(tt.args.router, baseURL+"/cluster/resource", tt.args.req)
			assert.NoError(t, err)
			t.Logf("list cluster %v", res)
			if tt.wantErr {
				assert.NotEqual(t, res.Code, 200)
			} else {
				t.Logf("res: %v", res)
				assert.Equal(t, tt.responseCode, res.Code)
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
