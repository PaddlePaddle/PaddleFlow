package job

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	mockRootUser       = "root"
	mockCreatedJobName = "job-xxxx1"
	MockQueueName      = "default-queue"
	MockQueueID        = "default-queue"
)

func TestCreatePFJob(t *testing.T) {
	driver.InitMockDB()
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.IsSingleCluster = true

	type args struct {
		ctx *logger.RequestContext
		req *CreateJobInfo
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
				req: &CreateJobInfo{},
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "create success request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					CommonJobInfo: CommonJobInfo{
						ID:          mockCreatedJobName,
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					Framework: schema.FrameworkStandalone,
				},
			},
			wantErr:      false,
			responseCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			res, err := CreatePFJob(tt.args.ctx, tt.args.req)
			t.Logf("case[%s] create single job, response=%+v", tt.name, res)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Contains(t, err.Error(), "record not found")
			}
		})
	}

}
