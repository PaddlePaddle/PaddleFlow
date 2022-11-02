package job

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestDeleteJob(t *testing.T) {
	mockQueue := model.Queue{
		Name: MockQueueName,
		Model: model.Model{
			ID: MockQueueID,
		},
		Namespace:   "paddleflow",
		ClusterId:   "MockClusterID",
		ClusterName: "MockClusterName",
		QuotaType:   schema.TypeVolcanoCapabilityQuota,
		MaxResources: &resources.Resource{
			Resources: map[string]resources.Quantity{
				"cpu":            10 * 1000,
				"mem":            1000,
				"nvidia.com/gpu": 500,
			},
		},
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueOpen,
	}

	driver.InitMockDB()
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.IsSingleCluster = true
	storage.Queue.CreateQueue(&mockQueue)
	type args struct {
		ctx *logger.RequestContext
		req string
	}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "empty request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: "empty",
			},
			wantErr:      true,
			responseCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			err := DeleteJob(tt.args.ctx, tt.args.req)
			t.Logf("case[%s] create single job, response=%+v", tt.name, err)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
