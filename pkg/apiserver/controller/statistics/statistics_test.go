package statistics

import (
	"database/sql"
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

const (
	MockQueueName   = "default-queue"
	MockQueueID     = "default-queue"
	MockClusterName = "default-cluster"
	MockClusterID   = "default-cluster"
	MockRootUser    = "root"
)

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
		Name: MockQueueName,
		Model: model.Model{
			ID: MockQueueID,
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

	type args struct {
		ctx          *logger.RequestContext
		queueNames   []string
		startTimeStr string
		endTimeStr   string
	}
	testCases := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "case1",
			args: args{
				ctx:        &logger.RequestContext{UserName: MockRootUser},
				queueNames: []string{MockQueueName, MockQueueName + "2"},
				//queueNames:   []string{MockQueueName},
				startTimeStr: "2023-03-02 06:00:00",
				endTimeStr:   "2023-03-02 08:00:00",
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "case2",
			args: args{
				ctx:        &logger.RequestContext{UserName: MockRootUser},
				queueNames: []string{MockQueueName, MockQueueName + "2"},
				//queueNames:   []string{MockQueueName},
				startTimeStr: "2023-03-02 06:00:00",
				endTimeStr:   "2023-03-02 16:00:00",
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "case3",
			args: args{
				ctx:        &logger.RequestContext{UserName: MockRootUser},
				queueNames: []string{MockQueueName, MockQueueName + "2"},
				//queueNames:   []string{MockQueueName},
				startTimeStr: "2023-03-01 00:00:00",
				endTimeStr:   "2023-03-05 00:00:00",
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "case4",
			args: args{
				ctx:        &logger.RequestContext{UserName: MockRootUser},
				queueNames: []string{MockQueueName, MockQueueName + "2"},
				//queueNames:   []string{MockQueueName},
				startTimeStr: "2023-03-01 12:00:00",
				endTimeStr:   "2023-03-02 16:00:00",
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "start time parse err",
			args: args{
				ctx:        &logger.RequestContext{UserName: MockRootUser},
				queueNames: []string{MockQueueName, MockQueueName + "2"},
				//queueNames:   []string{MockQueueName},
				startTimeStr: "2023-03-01 00-00-00",
				endTimeStr:   "2023-03-05 00:00:00",
			},
			wantErr:      true,
			responseCode: 500,
		},
		{
			name: "end time parse err",
			args: args{
				ctx:        &logger.RequestContext{UserName: MockRootUser},
				queueNames: []string{MockQueueName, MockQueueName + "2"},
				//queueNames:   []string{MockQueueName},
				startTimeStr: "2023-03-01 00:00:00",
				endTimeStr:   "2023-03-05 00-00-00",
			},
			wantErr:      true,
			responseCode: 500,
		},
		{
			name: "end time before start time",
			args: args{
				ctx:        &logger.RequestContext{UserName: MockRootUser},
				queueNames: []string{MockQueueName, MockQueueName + "2"},
				//queueNames:   []string{MockQueueName},
				startTimeStr: "2023-03-05 00:00:00",
				endTimeStr:   "2023-03-01 00:00:00",
			},
			wantErr:      true,
			responseCode: 500,
		},
		{
			name: "get queue by name failed",
			args: args{
				ctx:        &logger.RequestContext{UserName: MockRootUser},
				queueNames: []string{"000"},
				//queueNames:   []string{MockQueueName},
				startTimeStr: "2023-03-01 00:00:00",
				endTimeStr:   "2023-03-05 00:00:00",
			},
			wantErr:      true,
			responseCode: 500,
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
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			res, err := GetCardTimeInfo(tt.args.ctx, tt.args.queueNames, tt.args.startTimeStr, tt.args.endTimeStr)
			if tt.wantErr {
				assert.Error(t, err)
				t.Logf("name=%s err: %v", tt.name, err)
			} else {
				assert.Equal(t, nil, err)
				for _, v := range res {
					t.Logf("case[%s] create job, result=%+v", tt.name, v)
				}
			}
		})
	}
}

//
//func TestGetGpuCards(t *testing.T) {
//	jobStatus := &model.Job{}
//	t.Run("TestGetGpuCards-1", func(t *testing.T) {
//		resourceJsonStr := `{"k8s":1,"slurm":2,"k8s-new":3,"aistudio":4,"kubernetes":5}`
//		jobStatus.ResourceJson = resourceJsonStr
//		require.Equal(t, GetGpuCards(jobStatus), 1+2+3+4+5, "GetGpuCards func return %v, expect 1+2+3+4+5", GetGpuCards(jobStatus))
//	})
//	t.Run("TestGetGpuCards-2", func(t *testing.T) {
//		resourceJsonStr := `{"k8s":0,"slurm":0,"k8s-new":0,"aistudio":0,"kubernetes":0}`
//		jobStatus.ResourceJson = resourceJsonStr
//		require.Equal(t, GetGpuCards(jobStatus), 0, "GetGpuCards func return %v, expect 0", GetGpuCards(jobStatus))
//	})
//	t.Run("TestGetGpuCards-3", func(t *testing.T) {
//		resourceJsonStr := `{"k8s":1,"slurm":0,"k8s-new":0,"aistudio":0,"kubernetes":0}`
//		jobStatus.ResourceJson = resourceJsonStr
//		require.Equal(t, GetGpuCards(jobStatus), 1, "GetGpuCards func return %v, expect 1", GetGpuCards(jobStatus))
//	})
//}
