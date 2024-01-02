package v1

import (
	"database/sql"
	"fmt"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/statistics"
	"github.com/go-chi/chi"
	"testing"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
	"github.com/stretchr/testify/assert"
)

//const (
//	MockQueueName   = "default-queue"
//	MockQueueID     = "default-queue"
//	MockClusterName = "default-cluster"
//	MockClusterID   = "default-cluster"
//	MockRootUser    = "root"
//)

// test for func GetCardTimeByQueueName
func TestGetCardTimeByQueueName(t *testing.T) {
	maxRes, err := resources.NewResourceFromMap(map[string]string{
		resources.ResCPU:    "10",
		resources.ResMemory: "20Gi",
		"nvidia.com/gpu":    "500",
	})
	assert.Equal(t, nil, err)

	driver.InitMockDB()
	mockCluster := model.ClusterInfo{
		Model: model.Model{
			ID: MockClusterID,
		},
		Name: MockClusterName,
	}
	mockQueue1 := model.Queue{
		Name: MockQueueName + "1",
		Model: model.Model{
			ID: MockQueueID + "1",
		},
		Namespace:        "paddleflow",
		ClusterId:        MockClusterID,
		ClusterName:      MockClusterName,
		QuotaType:        schema.TypeVolcanoCapabilityQuota,
		MaxResources:     maxRes,
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueOpen,
	}
	mockQueue2 := model.Queue{
		Name: MockQueueName + "2",
		Model: model.Model{
			ID: MockQueueID + "2",
		},
		Namespace:        "paddleflow",
		ClusterId:        MockClusterID,
		ClusterName:      MockClusterName,
		QuotaType:        schema.TypeVolcanoCapabilityQuota,
		MaxResources:     maxRes,
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueOpen,
	}
	mockJob1 := model.Job{
		ID:       "MockJobID",
		Name:     "MockJobName",
		UserName: MockRootUser,
		QueueID:  MockQueueID,
		ActivatedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 2, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		FinishedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 2, 12, 0, 0, 0, time.UTC),
			Valid: true,
		},
		Members: []schema.Member{
			{
				Replicas: 1,
				Conf: schema.Conf{
					Flavour: schema.Flavour{
						ResourceInfo: schema.ResourceInfo{
							ScalarResources: map[schema.ResourceName]string{
								"nvidia.com/gpu": "1",
							},
						},
					},
				},
			},
		},
	}
	mockJob2 := model.Job{
		ID:       "MockJobID2",
		Name:     "MockJobName2",
		UserName: MockRootUser,
		QueueID:  MockQueueID + "2",
		ActivatedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 2, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		FinishedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 3, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		Members: []schema.Member{
			{
				Replicas: 1,
				Conf: schema.Conf{
					Flavour: schema.Flavour{
						ResourceInfo: schema.ResourceInfo{
							ScalarResources: map[schema.ResourceName]string{
								"nvidia.com/gpu": "2",
							},
						},
					},
				},
			},
		},
	}
	mockJob3 := model.Job{
		ID:       "MockJobID3",
		Name:     "MockJobName3",
		UserName: MockRootUser,
		QueueID:  MockQueueID,
		ActivatedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 2, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		FinishedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 3, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		Members: []schema.Member{
			{
				Replicas: 1,
				Conf: schema.Conf{
					Flavour: schema.Flavour{
						ResourceInfo: schema.ResourceInfo{
							ScalarResources: map[schema.ResourceName]string{
								"cpu": "2",
							},
						},
					},
				},
			},
		},
	}
	mockJob4 := model.Job{
		ID:       "MockJobID4",
		Name:     "MockJobName4",
		UserName: MockRootUser,
		QueueID:  MockQueueID,
		ActivatedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		FinishedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 7, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		Members: []schema.Member{
			{
				Replicas: 1,
				Conf: schema.Conf{
					Flavour: schema.Flavour{
						ResourceInfo: schema.ResourceInfo{
							ScalarResources: map[schema.ResourceName]string{
								"nvidia.com/gpu": "2",
							},
						},
					},
				},
			},
		},
	}

	type args struct {
		ctx       *logger.RequestContext
		queueName string
		startTime string
		endTime   string
		router    *chi.Mux
	}

	router, routerNonRoot, baseURL := MockInitJob(t)
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
				ctx:       ctx,
				router:    router,
				queueName: MockQueueName,
				startTime: "2023-03-02 06:00:00",
				endTime:   "2023-03-02 08:00:00",
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "empty request",
			args: args{
				ctx:       ctx,
				router:    routerNonRoot,
				queueName: MockQueueName,
				startTime: "2023-03-02 06:00:00",
				endTime:   "2023-03-02 08:00:00",
			},
			wantErr:      false,
			responseCode: 200,
		},
	}

	//ctx := &logger.RequestContext{UserName: MockRootUser}
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.IsSingleCluster = true
	storage.Cluster.CreateCluster(&mockCluster)
	storage.Queue.CreateQueue(&mockQueue1)
	storage.Queue.CreateQueue(&mockQueue2)
	storage.Job.CreateJob(&mockJob1)
	storage.Job.CreateJob(&mockJob2)
	storage.Job.CreateJob(&mockJob3)
	storage.Job.CreateJob(&mockJob4)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//params := url.Values{}
			//t.Logf("request url: %v", fmt.Sprintf(baseURL+"/cluster/%s/nodeInfos?%v", tt.args.queueName, params.Encode()))
			//res,err := getCardTimeDetail(tt.args.ctx, tt.args.queueName, tt.args.startTimeStr, tt.args.endTimeStr)
			res, err := PerformGetRequest(tt.args.router, fmt.Sprintf(baseURL+"/statistics/cardTime/%s?startTime=%s&endTime=%s", tt.args.queueName, tt.args.startTime, tt.args.endTime))
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
func TestGetCardTimeBatch(t *testing.T) {
	maxRes, err := resources.NewResourceFromMap(map[string]string{
		resources.ResCPU:    "10",
		resources.ResMemory: "20Gi",
		"nvidia.com/gpu":    "500",
	})
	assert.Equal(t, nil, err)

	driver.InitMockDB()
	mockCluster := model.ClusterInfo{
		Model: model.Model{
			ID: MockClusterID,
		},
		Name: MockClusterName,
	}
	mockQueue1 := model.Queue{
		Name: MockQueueName + "1",
		Model: model.Model{
			ID: MockQueueID + "1",
		},
		Namespace:        "paddleflow",
		ClusterId:        MockClusterID,
		ClusterName:      MockClusterName,
		QuotaType:        schema.TypeVolcanoCapabilityQuota,
		MaxResources:     maxRes,
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueOpen,
	}
	mockQueue2 := model.Queue{
		Name: MockQueueName + "2",
		Model: model.Model{
			ID: MockQueueID + "2",
		},
		Namespace:        "paddleflow",
		ClusterId:        MockClusterID,
		ClusterName:      MockClusterName,
		QuotaType:        schema.TypeVolcanoCapabilityQuota,
		MaxResources:     maxRes,
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueOpen,
	}
	mockJob1 := model.Job{
		ID:       "MockJobID",
		Name:     "MockJobName",
		UserName: MockRootUser,
		QueueID:  MockQueueID,
		ActivatedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 2, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		FinishedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 2, 12, 0, 0, 0, time.UTC),
			Valid: true,
		},
		Members: []schema.Member{
			{
				Replicas: 1,
				Conf: schema.Conf{
					Flavour: schema.Flavour{
						ResourceInfo: schema.ResourceInfo{
							ScalarResources: map[schema.ResourceName]string{
								"nvidia.com/gpu": "1",
							},
						},
					},
				},
			},
		},
	}
	mockJob2 := model.Job{
		ID:       "MockJobID2",
		Name:     "MockJobName2",
		UserName: MockRootUser,
		QueueID:  MockQueueID + "2",
		ActivatedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 2, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		FinishedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 3, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		Members: []schema.Member{
			{
				Replicas: 1,
				Conf: schema.Conf{
					Flavour: schema.Flavour{
						ResourceInfo: schema.ResourceInfo{
							ScalarResources: map[schema.ResourceName]string{
								"nvidia.com/gpu": "2",
							},
						},
					},
				},
			},
		},
	}
	mockJob3 := model.Job{
		ID:       "MockJobID3",
		Name:     "MockJobName3",
		UserName: MockRootUser,
		QueueID:  MockQueueID,
		ActivatedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 2, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		FinishedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 3, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		Members: []schema.Member{
			{
				Replicas: 1,
				Conf: schema.Conf{
					Flavour: schema.Flavour{
						ResourceInfo: schema.ResourceInfo{
							ScalarResources: map[schema.ResourceName]string{
								"cpu": "2",
							},
						},
					},
				},
			},
		},
	}
	mockJob4 := model.Job{
		ID:       "MockJobID4",
		Name:     "MockJobName4",
		UserName: MockRootUser,
		QueueID:  MockQueueID,
		ActivatedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		FinishedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 7, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		Members: []schema.Member{
			{
				Replicas: 1,
				Conf: schema.Conf{
					Flavour: schema.Flavour{
						ResourceInfo: schema.ResourceInfo{
							ScalarResources: map[schema.ResourceName]string{
								"nvidia.com/gpu": "2",
							},
						},
					},
				},
			},
		},
	}

	type args struct {
		ctx    *logger.RequestContext
		req    *statistics.GetCardTimeBatchRequest
		router *chi.Mux
	}

	router, _, baseURL := MockInitJob(t)
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
				req: &statistics.GetCardTimeBatchRequest{
					QueueNames: []string{MockQueueName},
					StartTime:  "2023-03-02 06:00:00",
					EndTime:    "2023-03-02 08:00:00",
				},
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "empty request",
			args: args{
				ctx:    ctx,
				router: router,
				req: &statistics.GetCardTimeBatchRequest{
					QueueNames: []string{MockQueueName},
					StartTime:  "2023-03-01 06:00:00",
					EndTime:    "2023-03-07 08:00:00",
				},
			},
			wantErr:      false,
			responseCode: 200,
		},
	}

	//ctx := &logger.RequestContext{UserName: MockRootUser}
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.IsSingleCluster = true
	storage.Cluster.CreateCluster(&mockCluster)
	storage.Queue.CreateQueue(&mockQueue1)
	storage.Queue.CreateQueue(&mockQueue2)
	storage.Job.CreateJob(&mockJob1)
	storage.Job.CreateJob(&mockJob2)
	storage.Job.CreateJob(&mockJob3)
	storage.Job.CreateJob(&mockJob4)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := PerformPostRequest(tt.args.router, baseURL+"/statistics/cardTime", tt.args.req)
			assert.NoError(t, err)
			t.Logf("get card time batch %v", res)
			if tt.wantErr {
				assert.NotEqual(t, res.Code, 200)
			} else {
				t.Logf("res: %v", res)
				assert.Equal(t, tt.responseCode, res.Code)
			}
		})
	}
}
