package statistics

import (
	"database/sql"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	MockQueueName   = "default-queue"
	MockQueueID     = "default-queue"
	MockClusterName = "default-cluster"
	MockClusterID   = "default-cluster"
	MockRootUser    = "root"
)

func TestGetCardTimeByQueueName(t *testing.T) {
	maxRes, err := resources.NewResourceFromMap(map[string]string{
		resources.ResCPU:    "10",
		resources.ResMemory: "20Gi",
		"nvidia.com/gpu":    "500",
	})
	assert.Equal(t, nil, err)

	//ctx := &logger.RequestContext{}
	//ctx := logger.NewContext(nil, &apiserver.RequestContext{})
	startTime := "2023-03-01 00:00:00"
	endTime := "2023-03-05 00:00:00"
	driver.InitMockDB()
	mockCluster := model.ClusterInfo{
		Model: model.Model{
			ID: MockClusterID,
		},
		Name: MockClusterName,
	}
	mockQueue := model.Queue{
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
	mockJob := model.Job{
		ID:       "MockJobID",
		Name:     "MockJobName",
		UserName: MockRootUser,
		QueueID:  MockQueueID,
		ActivatedAt: sql.NullTime{
			Time:  time.Date(2023, 3, 2, 0, 0, 0, 0, time.UTC),
			Valid: true,
		},
		UpdatedAt: time.Date(2023, 3, 2, 1, 0, 0, 0, time.UTC),
	}
	ctx := &logger.RequestContext{UserName: MockRootUser}
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.IsSingleCluster = true
	storage.Cluster.CreateCluster(&mockCluster)
	storage.Queue.CreateQueue(&mockQueue)
	storage.Job.CreateJob(&mockJob)
	t.Run("testname", func(t *testing.T) {
		cardTimeRes, err := GetCardTimeByQueue(ctx, MockQueueName, startTime, endTime)
		if err != nil {
			t.Errorf("GetCardTimeByQueueName failed,err:%s", err)
		}
		t.Logf("cardTimeRes:%v", *cardTimeRes)
	})
}
