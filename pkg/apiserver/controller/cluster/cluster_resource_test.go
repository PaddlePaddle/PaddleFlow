package cluster

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

var (
	nodeLabelsList = []map[string]string{
		{
			v1beta1.QuotaLabelKey: "public",
		},
		{
			v1beta1.QuotaLabelKey: "test-q1",
		},
	}
	clusterNames = []string{"test-1", "test-2", "test-3", "test-4", "test-5", "test-6", "test-7", "test-8"}
	capacities   = []map[string]string{
		{
			"cpu":            "56",
			"memory":         "256Gi",
			"nvidia.com/gpu": "8",
		},
		{
			"cpu":            "64",
			"memory":         "512Gi",
			"nvidia.com/gpu": "8",
		},
		{
			"cpu":            "80",
			"memory":         "512Gi",
			"nvidia.com/gpu": "8",
		},
		{
			"cpu":            "96",
			"memory":         "768Gi",
			"nvidia.com/gpu": "8",
		},
	}
	rList = []map[string]int64{
		{
			"cpu":    1000,
			"memory": 1 * 1024 * 1024 * 1024,
		},
		{
			"cpu":    2000,
			"memory": 4 * 1024 * 1024 * 1024,
		},
		{
			"cpu":    4000,
			"memory": 8 * 1024 * 1024 * 1024,
		},
		{
			"cpu":            8000,
			"memory":         16 * 1024 * 1024 * 1024,
			"nvidia.com/gpu": 1,
		},
	}
	rNames = []string{"cpu", "memory"}
)

func fakeNodeResourceInfo(count int, clusterNameList []string, rNames []string, capacities []map[string]string) ([]model.NodeInfo, []model.ResourceInfo) {
	var nodeInfoList []model.NodeInfo
	var resourceList []model.ResourceInfo
	clusterLen := len(clusterNameList)
	capLen := len(capacities)

	rand.Seed(time.Now().Unix())
	for idx := 0; idx < count; idx++ {
		clusterName := clusterNameList[rand.Intn(clusterLen)]
		nodeName := fmt.Sprintf("instance-%d", idx)
		nodeID := fmt.Sprintf("node-%d", idx)
		nodeInfoList = append(nodeInfoList,
			model.NodeInfo{
				ID:          nodeID,
				Name:        nodeName,
				ClusterID:   fmt.Sprintf("cluster-%s", clusterName),
				ClusterName: clusterName,
				Status:      "Ready",
				Capacity:    capacities[rand.Intn(capLen)],
			})

		for _, rName := range rNames {
			resourceList = append(resourceList, model.ResourceInfo{
				Name:     rName,
				Value:    int64(rand.Intn(10000)),
				NodeName: nodeName,
				NodeID:   nodeID,
			})
		}
		if rand.Intn(100) < 2 {
			resourceList = append(resourceList, model.ResourceInfo{
				Name:     "nvidia.com/gpu",
				Value:    1,
				NodeName: nodeName,
				NodeID:   nodeID,
			})
		}
	}
	return nodeInfoList, resourceList
}

func createFakePodInfo(count int, nodeID, nodeName string, reList []map[string]int64) error {
	statusList := []model.TaskAllocateStatus{
		model.TaskCreating,
		model.TaskRunning,
		model.TaskTerminating,
	}
	statusLen := len(statusList)
	reLen := len(reList)

	var err error
	rand.Seed(time.Now().Unix())
	for idx := 0; idx < count; idx++ {
		pInfo := &model.PodInfo{
			ID:        uuid.GenerateIDWithLength("", 32),
			Name:      fmt.Sprintf("pod-%s-%d", nodeName, idx),
			Namespace: "default",
			NodeName:  nodeName,
			NodeID:    nodeID,
			Labels:    map[string]string{"paddleflow/pod-name": fmt.Sprintf("pod-%s-%d", nodeName, idx)},
			Status:    int(statusList[rand.Intn(statusLen)]),
			Resources: reList[rand.Intn(reLen)],
		}
		err = storage.PodCache.AddPod(pInfo)
		if err != nil {
			break
		}
	}
	return err
}

func createFakeNodeInfo(nodeCount, podCount int, clusterList []string, rList []map[string]int64) error {
	clusterLen := len(clusterList)
	capaLen := len(capacities)
	nodeLabelLen := len(nodeLabelsList)

	var err error
	rand.Seed(time.Now().Unix())
	for idx := 0; idx < nodeCount; idx++ {
		nodeID := uuid.GenerateIDWithLength("node", 16)
		nodeName := fmt.Sprintf("instance-%d", idx)
		clusterName := clusterList[rand.Intn(clusterLen)]
		labels := nodeLabelsList[rand.Intn(nodeLabelLen)]
		info := &model.NodeInfo{
			ID:          nodeID,
			Name:        nodeName,
			ClusterID:   fmt.Sprintf("cluster-%s", clusterName),
			ClusterName: clusterName,
			Status:      "Ready",
			Capacity:    capacities[rand.Intn(capaLen)],
			Labels:      labels,
		}
		err = storage.NodeCache.AddNode(info)
		if err != nil {
			break
		}
		err = createFakePodInfo(podCount, nodeID, nodeName, rList)
	}
	return err
}

func TestListClusterResources(t *testing.T) {
	ctx := &logger.RequestContext{
		UserName: MockRootUser,
	}
	testCases := []struct {
		name string
		req  ListClusterResourcesRequest
		err  error
	}{
		{
			name: "list all clusters",
			req: ListClusterResourcesRequest{
				PageSize: 500,
				PageNo:   1,
			},
			err: nil,
		},
		{
			name: "list resources by physical queue",
			req: ListClusterResourcesRequest{
				PageSize:  500,
				PageNo:    1,
				QueueName: "test-q1",
			},
			err: nil,
		},
		{
			name: "list resources by logic queue",
			req: ListClusterResourcesRequest{
				PageSize:  500,
				PageNo:    1,
				QueueName: "test-q-public",
			},
			err: nil,
		},
	}

	driver.InitMockDB()
	err := storage.Cluster.CreateCluster(&model.ClusterInfo{
		Model: model.Model{
			ID: MockClusterName,
		},
		Name: MockClusterName,
	})
	assert.Equal(t, nil, err)
	err = storage.Queue.CreateQueue(&model.Queue{
		Name:        "test-q1",
		Namespace:   "default",
		ClusterId:   MockClusterName,
		ClusterName: MockClusterName,
		Location: map[string]string{
			v1beta1.QuotaTypeKey: v1beta1.QuotaTypePhysical,
		},
	})
	assert.Equal(t, nil, err)
	err = storage.Queue.CreateQueue(&model.Queue{
		Name:        "test-q-public",
		Namespace:   "default",
		ClusterId:   MockClusterName,
		ClusterName: MockClusterName,
		Location: map[string]string{
			v1beta1.QuotaTypeKey: v1beta1.QuotaTypeLogical,
		},
	})
	assert.Equal(t, nil, err)

	err = driver.InitCache("DEBUG")
	assert.Equal(t, nil, err)
	var nodeCount = 500
	var podCount = 20
	err = createFakeNodeInfo(nodeCount, podCount, []string{MockClusterName, "cn1"}, rList)
	assert.Equal(t, nil, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := ListClusterResources(ctx, tc.req)
			assert.Equal(t, tc.err, err)
			resJSON, err := json.Marshal(res)
			if err == nil {
				t.Logf("cluster resources: %s", string(resJSON))
			}
		})
	}
}

func TestListClusterNodeInfos(t *testing.T) {
	ctx := &logger.RequestContext{
		UserName: MockRootUser,
	}
	testCases := []struct {
		name      string
		req       ListClusterResourcesRequest
		namespace string
		err       error
	}{
		{
			name: "list nodes by physical queue",
			req: ListClusterResourcesRequest{
				PageSize:  500,
				PageNo:    1,
				QueueName: "test-q1",
			},
			namespace: "default",
			err:       nil,
		},
		{
			name: "list nodes by logic queue",
			req: ListClusterResourcesRequest{
				PageSize:  500,
				PageNo:    1,
				QueueName: "test-q-public",
			},
			namespace: "default",
			err:       nil,
		},
	}
	driver.InitMockDB()
	err := storage.Cluster.CreateCluster(&model.ClusterInfo{
		Model: model.Model{
			ID: MockClusterName,
		},
		Name: MockClusterName,
	})
	assert.Equal(t, nil, err)
	err = storage.Queue.CreateQueue(&model.Queue{
		Name:        "test-q1",
		Namespace:   "default",
		ClusterId:   MockClusterName,
		ClusterName: MockClusterName,
		Location: map[string]string{
			v1beta1.QuotaTypeKey: v1beta1.QuotaTypePhysical,
		},
	})

	assert.Equal(t, nil, err)
	err = storage.Queue.CreateQueue(&model.Queue{
		Name:        "test-q-public",
		Namespace:   "default",
		ClusterId:   MockClusterName,
		ClusterName: MockClusterName,
		Location: map[string]string{
			v1beta1.QuotaTypeKey: v1beta1.QuotaTypeLogical,
		},
	})
	assert.Equal(t, nil, err)

	err = driver.InitCache("DEBUG")
	assert.Equal(t, nil, err)
	var nodeCount = 5
	var podCount = 20
	err = createFakeNodeInfo(nodeCount, podCount, []string{MockClusterName, "cn1"}, rList)
	assert.Equal(t, nil, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			count, res, err := ListClusterNodeInfos(ctx, tc.req, tc.namespace)
			assert.Equal(t, tc.err, err)
			resJSON, err := json.Marshal(res)
			if err == nil {
				t.Logf("node infos: %s", string(resJSON))
			}
			assert.Equal(t, int(count), nodeCount)
		})
	}
}

func TestConstructNodeResponses(t *testing.T) {
	mockClusterID := "cluster-test"
	testCases := []struct {
		name   string
		nodes  []model.NodeInfo
		used   []model.ResourceInfo
		pods   []model.PodInfo
		labels []model.LabelInfo
		err    error
	}{
		{
			name: "2 nodes",
			nodes: []model.NodeInfo{
				{
					ID:          "node-instance-1",
					Name:        "instance-1",
					IP:          "10.0.0.1",
					ClusterID:   mockClusterID,
					ClusterName: mockClusterID,
					Status:      "Ready",
					Capacity: map[string]string{
						"cpu":                   "20",
						"memory":                "100Gi",
						"nvidia.com/gpu":        "8",
						"baidu.com/cgpu":        "8",
						"baidu.com/cgpu_core":   "800",
						"baidu.com/cgpu_memory": "1600",
					},
				},
				{
					ID:          "node-instance-2",
					Name:        "instance-2",
					IP:          "10.0.0.2",
					ClusterID:   mockClusterID,
					ClusterName: mockClusterID,
					Status:      "Ready",
					Capacity: map[string]string{
						"cpu":                   "30",
						"memory":                "100Gi",
						"nvidia.com/gpu":        "8",
						"baidu.com/cgpu":        "8",
						"baidu.com/cgpu_core":   "800",
						"baidu.com/cgpu_memory": "1600",
					},
				},
			},
			used: []model.ResourceInfo{
				{
					Name:     "cpu",
					NodeName: "instance-2",
					NodeID:   "node-instance-2",
					Value:    2795,
				},
				{
					Name:     "memory",
					NodeName: "instance-2",
					NodeID:   "node-instance-2",
					Value:    10737418240,
				},
				{
					Name:     "cpu",
					NodeName: "instance-1",
					NodeID:   "node-instance-1",
					Value:    10,
				},
				{
					Name:     "memory",
					NodeName: "instance-1",
					NodeID:   "node-instance-1",
					Value:    1073741824,
				},
				{
					Name:     "nvidia.com/gpu",
					NodeName: "instance-1",
					NodeID:   "node-instance-1",
					Value:    4,
				},
				{
					Name:  "cpu",
					PodID: "pod-instance-1",
					Value: 200,
				},
				{
					Name:  "cpu",
					PodID: "pod-instance-2",
					Value: 20,
				},
			},
			pods: []model.PodInfo{
				{
					Name:     "pod-instance-2",
					NodeName: "instance-2",
					NodeID:   "node-instance-2",
					ID:       "pod-instance-2",
					Status:   1,
					Resources: map[string]int64{
						"memory":                  741824,
						"cpu":                     200,
						"baidu.com/p40_cgpu_core": 100,
					},
				},
				{
					Name:     "pod-instance-1",
					NodeName: "instance-1",
					NodeID:   "node-instance-1",
					ID:       "pod-instance-1",
					Status:   1,
					Resources: map[string]int64{
						"memory":         1824,
						"cpu":            20,
						"nvidia.com/gpu": 2,
					},
				},
			},
			labels: []model.LabelInfo{
				{
					Name:       "jobName",
					Value:      "job-0bb64357dbedcbdf",
					ObjectID:   "pod-instance-1",
					ObjectType: model.ObjectTypePod,
				},
				{
					Name:       "jobName",
					Value:      "job-0bb64357dbedcbdf",
					ObjectID:   "pod-instance-2",
					ObjectType: model.ObjectTypePod,
				},
			},
			err: nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ConstructNodeResponses(tc.nodes, tc.used, tc.pods, tc.labels)
			assert.Equal(t, tc.err, err)
			data, err := json.Marshal(result)
			assert.Equal(t, nil, err)
			t.Logf("construct node resources: %v", string(data))
		})
	}
}

func TestConstructClusterResources(t *testing.T) {
	mockClusterID := "cluster-test"
	testCases := []struct {
		name      string
		nodes     []model.NodeInfo
		resources []model.ResourceInfo
		err       error
	}{
		{
			name: "2 nodes",
			nodes: []model.NodeInfo{
				{
					ID:          "node-instance-1",
					Name:        "instance-1",
					ClusterID:   mockClusterID,
					ClusterName: mockClusterID,
					Status:      "Ready",
					Capacity: map[string]string{
						"cpu":                   "20",
						"memory":                "100Gi",
						"nvidia.com/gpu":        "8",
						"baidu.com/cgpu":        "8",
						"baidu.com/cgpu_core":   "800",
						"baidu.com/cgpu_memory": "1600",
					},
				},
				{
					ID:          "node-instance-2",
					Name:        "instance-2",
					ClusterID:   mockClusterID,
					ClusterName: mockClusterID,
					Status:      "Ready",
					Capacity: map[string]string{
						"cpu":                   "30",
						"memory":                "100Gi",
						"nvidia.com/gpu":        "8",
						"baidu.com/cgpu":        "8",
						"baidu.com/cgpu_core":   "800",
						"baidu.com/cgpu_memory": "1600",
					},
				},
			},
			resources: []model.ResourceInfo{
				{
					Name:     "cpu",
					NodeName: "instance-2",
					NodeID:   "node-instance-2",
					Value:    2795,
				},
				{
					Name:     "memory",
					NodeName: "instance-2",
					NodeID:   "node-instance-2",
					Value:    10737418240,
				},
				{
					Name:     "baidu.com/p40_cgpu",
					NodeName: "instance-2",
					NodeID:   "node-instance-2",
					Value:    5,
				},
				{
					Name:     "baidu.com/p40_cgpu_core",
					NodeName: "instance-2",
					NodeID:   "node-instance-2",
					Value:    300,
				},
				{
					Name:     "gpuDeviceIDX",
					NodeName: "instance-2",
					NodeID:   "node-instance-2",
					Value:    73,
				},
				{
					Name:     "cpu",
					NodeName: "instance-1",
					NodeID:   "node-instance-1",
					Value:    10000,
				},
				{
					Name:     "memory",
					NodeName: "instance-1",
					NodeID:   "node-instance-1",
					Value:    1073741824,
				},
				{
					Name:     "nvidia.com/gpu",
					NodeName: "instance-1",
					NodeID:   "node-instance-1",
					Value:    4,
				},
			},
			err: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ConstructClusterResources(tc.nodes, tc.resources, "")
			assert.Equal(t, tc.err, err)
			data, err := json.Marshal(result)
			assert.Equal(t, nil, err)
			t.Logf("construct cluster resources: %v", string(data))
		})
	}
}

func BenchmarkListClusterResources(b *testing.B) {
	ctx := &logger.RequestContext{
		UserName: MockRootUser,
	}
	err := driver.InitCache("INFO")
	assert.Equal(b, nil, err)
	err = createFakeNodeInfo(10000, 25, clusterNames, rList)
	assert.Equal(b, nil, err)

	b.Run("test list 1k node", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = ListClusterResources(ctx, ListClusterResourcesRequest{PageNo: 1, PageSize: 1000})
			assert.Equal(b, nil, err)
		}
	})

	b.Run("test list 7k node", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = ListClusterResources(ctx, ListClusterResourcesRequest{PageNo: 1, PageSize: 7000})
			assert.Equal(b, nil, err)
		}
	})

	b.Run("test list 1w node", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = ListClusterResources(ctx, ListClusterResourcesRequest{PageNo: 1, PageSize: 10000})
			assert.Equal(b, nil, err)
		}
	})
}

func BenchmarkConstructClusterResources(b *testing.B) {
	nodes1k, resources1k := fakeNodeResourceInfo(1000, clusterNames, rNames, capacities)
	nodes5k, resources5k := fakeNodeResourceInfo(5000, clusterNames, rNames, capacities)
	nodes7k, resources7k := fakeNodeResourceInfo(7000, clusterNames, rNames, capacities)
	nodes1w, resources1w := fakeNodeResourceInfo(10000, clusterNames, rNames, capacities)
	nodes10w, resources10w := fakeNodeResourceInfo(100000, clusterNames, rNames, capacities)

	b.Run("test 1k node", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := ConstructClusterResources(nodes1k, resources1k, "")
			assert.Equal(b, nil, err)
		}
	})

	b.Run("test 5k node", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := ConstructClusterResources(nodes5k, resources5k, "")
			assert.Equal(b, nil, err)
		}
	})

	b.Run("test 7k node", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := ConstructClusterResources(nodes7k, resources7k, "")
			assert.Equal(b, nil, err)
		}
	})

	b.Run("test 1w node", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := ConstructClusterResources(nodes1w, resources1w, "")
			assert.Equal(b, nil, err)
		}
	})

	b.Run("test 10w node", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := ConstructClusterResources(nodes10w, resources10w, "")
			assert.Equal(b, nil, err)
		}
	})
}
