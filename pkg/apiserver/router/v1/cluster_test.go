package v1

import (
	"fmt"
	"net/url"
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
					Labels:          "a!=b",
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

func TestListClusterNodeInfo(t *testing.T) {
	router, routerNonRoot, baseURL := MockInitJob(t)
	driver.InitCache("debug")
	createMockClusters(t, 10, 10, 10)
	type args struct {
		ctx         *logger.RequestContext
		PageSize    string
		PageNo      string
		ClusterName string
		router      *chi.Mux
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
				ctx:         ctx,
				router:      router,
				ClusterName: "",
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "routerNonRoot",
			args: args{
				ctx:         ctx,
				router:      routerNonRoot,
				ClusterName: "testCn1",
			},
			wantErr:      false,
			responseCode: 403,
		},
		{
			name: "PageSize and PageNo",
			args: args{
				ctx:         ctx,
				router:      router,
				PageNo:      "1",
				PageSize:    "1",
				ClusterName: "testCn1",
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "No PageSize",
			args: args{
				ctx:         ctx,
				router:      router,
				PageNo:      "1",
				ClusterName: "testCn1",
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "No PageNo",
			args: args{
				ctx:         ctx,
				router:      router,
				PageSize:    "10",
				ClusterName: "testCn1",
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "Wrong PageSize 1",
			args: args{
				ctx:         ctx,
				router:      router,
				PageNo:      "1",
				PageSize:    "500",
				ClusterName: "testCn1",
			},
			wantErr:      true,
			responseCode: 200,
		},
		{
			name: "Wrong PageSize 2",
			args: args{
				ctx:         ctx,
				router:      router,
				PageNo:      "1",
				PageSize:    "abc",
				ClusterName: "testCn1",
			},
			wantErr:      true,
			responseCode: 200,
		},
		{
			name: "Wrong PageNo",
			args: args{
				ctx:         ctx,
				router:      router,
				PageNo:      "abc",
				PageSize:    "100",
				ClusterName: "testCn1",
			},
			wantErr:      true,
			responseCode: 200,
		},
		{
			name: "normal",
			args: args{
				ctx:         ctx,
				router:      router,
				PageNo:      "1",
				PageSize:    "10",
				ClusterName: "testCn1",
			},
			wantErr:      false,
			responseCode: 200,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := url.Values{}
			params.Set("pageNo", tt.args.PageNo)
			params.Set("pageSize", tt.args.PageSize)
			t.Logf("request url: %v", fmt.Sprintf(baseURL+"/cluster/%s/nodeInfos?%v", tt.args.ClusterName, params.Encode()))
			res, err := PerformGetRequest(tt.args.router, fmt.Sprintf(baseURL+"/cluster/%s/nodeInfos?%s", tt.args.ClusterName, params.Encode()))
			assert.NoError(t, err)
			t.Logf("list node %v", res)
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
