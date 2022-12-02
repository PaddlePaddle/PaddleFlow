package job

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	mockRootUser    = "root"
	MockQueueName   = "default-queue"
	MockQueueID     = "default-queue"
	MockClusterName = "default-cluster"
)

var clusterInfo = model.ClusterInfo{
	Name:        MockClusterName,
	Description: "Description",
	Endpoint:    "Endpoint",
	Source:      "Source",
	ClusterType: schema.KubernetesType,
	Version:     "1.16",
	Status:      model.ClusterStatusOnLine,
	Credential:  "credential",
	Setting:     "Setting",
}

func TestCreatePFJob(t *testing.T) {
	driver.InitMockDB()
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.IsSingleCluster = true

	err := storage.Cluster.CreateCluster(&model.ClusterInfo{
		Model: model.Model{
			ID: MockClusterName,
		},
		Name:        MockClusterName,
		ClusterType: schema.KubernetesType,
	})
	assert.Equal(t, nil, err)
	maxRes, err := resources.NewResourceFromMap(map[string]string{
		resources.ResCPU:    "10",
		resources.ResMemory: "20Gi",
		"nvidia.com/gpu":    "500",
	})
	assert.Equal(t, nil, err)
	queueInfo := model.Queue{
		Model: model.Model{
			ID: MockQueueID,
		},
		Name:         MockQueueName,
		Namespace:    "default",
		MaxResources: maxRes,
		MinResources: maxRes,
		QuotaType:    schema.TypeVolcanoCapabilityQuota,
		ClusterId:    MockClusterName,
		ClusterName:  MockClusterName,
		Status:       "open",
	}
	err = storage.Queue.CreateQueue(&queueInfo)
	assert.NoError(t, err)

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
			name: "create pod success request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					Members: []MemberSpec{
						{
							CommonJobInfo: CommonJobInfo{
								ID:          uuid.GenerateIDWithLength("job", 5),
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image: "busybox",
							},
							Role:     string(schema.RoleWorker),
							Replicas: 1,
						},
					},
					Type:      schema.TypeSingle,
					Framework: schema.FrameworkStandalone,
				},
			},
			wantErr:      false,
			responseCode: 400,
		},
		{
			name: "create pod failed, image absent",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					Members: []MemberSpec{
						{
							CommonJobInfo: CommonJobInfo{
								ID:          uuid.GenerateIDWithLength("job", 5),
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec:  JobSpec{},
							Role:     string(schema.RoleWorker),
							Replicas: 1,
						},
					},
					Type:      schema.TypeSingle,
					Framework: schema.FrameworkStandalone,
				},
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "create paddleJob success request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					CommonJobInfo: CommonJobInfo{
						ID:          uuid.GenerateIDWithLength("job", 5),
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					Type:      schema.TypeDistributed,
					Framework: schema.FrameworkPaddle,
					Members: []MemberSpec{
						{
							Replicas: 1,
							Role:     string(schema.RolePServer),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
						{
							Replicas: 1,
							Role:     string(schema.RolePWorker),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
					},
					ExtensionTemplate: map[string]interface{}{
						"a": "b",
					},
				},
			},
			wantErr:      false,
			responseCode: 400,
		},
		{
			name: "extensionTemplate paddleJob flavour validate cpu failed,err: cpu cannot be negative",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					CommonJobInfo: CommonJobInfo{
						ID:          uuid.GenerateIDWithLength("job", 5),
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					Type:      schema.TypeDistributed,
					Framework: schema.FrameworkPaddle,
					Members: []MemberSpec{
						{
							Replicas: 1,
							Role:     string(schema.RolePServer),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
								Flavour: schema.Flavour{
									ResourceInfo: schema.ResourceInfo{CPU: "-1", Mem: "3"}},
							},
						},
						{
							Replicas: 1,
							Role:     string(schema.RolePWorker),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
					},
					ExtensionTemplate: map[string]interface{}{
						"a": "b",
					},
				},
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "custom paddleJob flavour validate cpu failed,err: cpu cannot be negative",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					CommonJobInfo: CommonJobInfo{
						ID:          uuid.GenerateIDWithLength("job", 5),
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					Type:      schema.TypeDistributed,
					Framework: schema.FrameworkPaddle,
					Members: []MemberSpec{
						{
							Replicas: 1,
							Role:     string(schema.RolePServer),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
								Flavour: schema.Flavour{
									ResourceInfo: schema.ResourceInfo{CPU: "-1", Mem: "3"}},
							},
						},
						{
							Replicas: 1,
							Role:     string(schema.RolePWorker),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
					},
				},
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "paddleJob flavour validate cpu failed,err: cpu cannot be 0",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					CommonJobInfo: CommonJobInfo{
						ID:          uuid.GenerateIDWithLength("job", 5),
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					Type:      schema.TypeDistributed,
					Framework: schema.FrameworkPaddle,
					Members: []MemberSpec{
						{
							Replicas: 1,
							Role:     string(schema.RolePServer),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
								Flavour: schema.Flavour{
									ResourceInfo: schema.ResourceInfo{CPU: "0", Mem: "3"}},
							},
						},
						{
							Replicas: 1,
							Role:     string(schema.RolePWorker),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
					},
					ExtensionTemplate: map[string]interface{}{
						"a": "b",
					},
				},
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "priority 0 err: invalid job priority",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					CommonJobInfo: CommonJobInfo{
						ID:          uuid.GenerateIDWithLength("job", 5),
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					Type:      schema.TypeDistributed,
					Framework: schema.FrameworkPaddle,
					Members: []MemberSpec{
						{
							Replicas: 1,
							Role:     string(schema.RolePServer),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue:    MockQueueName,
									Priority: "a",
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
								Flavour: schema.Flavour{
									ResourceInfo: schema.ResourceInfo{CPU: "1", Mem: "3"}},
							},
						},
						{
							Replicas: 1,
							Role:     string(schema.RolePWorker),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
					},
					ExtensionTemplate: map[string]interface{}{
						"a": "b",
					},
				},
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "schedulingPolicy.Queue should be the same",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					CommonJobInfo: CommonJobInfo{
						ID:          uuid.GenerateIDWithLength("job", 5),
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					Type:      schema.TypeDistributed,
					Framework: schema.FrameworkPaddle,
					Members: []MemberSpec{
						{
							Replicas: 1,
							Role:     string(schema.RolePServer),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: "a",
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
						{
							Replicas: 1,
							Role:     string(schema.RolePWorker),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
					},
					ExtensionTemplate: map[string]interface{}{
						"a": "b",
					},
				},
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "the role[master] for framework paddle is not supported",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					CommonJobInfo: CommonJobInfo{
						ID:          uuid.GenerateIDWithLength("job", 5),
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					Type:      schema.TypeDistributed,
					Framework: schema.FrameworkPaddle,
					Members: []MemberSpec{
						{
							Replicas: 1,
							Role:     string(schema.RoleMaster),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
						{
							Replicas: 1,
							Role:     string(schema.RolePWorker),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
					},
					ExtensionTemplate: map[string]interface{}{
						"a": "b",
					},
				},
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "create paddleJob success request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					CommonJobInfo: CommonJobInfo{
						ID:          uuid.GenerateIDWithLength("job", 5),
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					Type:      schema.TypeDistributed,
					Framework: schema.FrameworkPaddle,
					ExtensionTemplate: map[string]interface{}{
						"a": "b",
					},
				},
			},
			wantErr:      false,
			responseCode: 400,
		},
		{
			name: "create mpijob success request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					CommonJobInfo: CommonJobInfo{
						ID:          uuid.GenerateIDWithLength("job", 5),
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					Type:      schema.TypeDistributed,
					Framework: schema.FrameworkMPI,
					Members: []MemberSpec{
						{
							Replicas: 1,
							Role:     string(schema.RoleMaster),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
						{
							Replicas: 1,
							Role:     string(schema.RoleWorker),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
					},
				},
			},
			wantErr:      false,
			responseCode: 400,
		},
		{
			name: "the role[pserver] for framework mpi is not supported",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					CommonJobInfo: CommonJobInfo{
						ID:          uuid.GenerateIDWithLength("job", 5),
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					Type:      schema.TypeDistributed,
					Framework: schema.FrameworkMPI,
					Members: []MemberSpec{
						{
							Replicas: 1,
							Role:     string(schema.RolePServer),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
						{
							Replicas: 1,
							Role:     string(schema.RoleWorker),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
					},
				},
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "mpi job must be set a master role and a worker role",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: &CreateJobInfo{
					CommonJobInfo: CommonJobInfo{
						ID:          uuid.GenerateIDWithLength("job", 5),
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					Type:      schema.TypeDistributed,
					Framework: schema.FrameworkMPI,
					Members: []MemberSpec{
						{
							Replicas: 1,
							Role:     string(schema.RoleWorker),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
						{
							Replicas: 1,
							Role:     string(schema.RoleWorker),
							CommonJobInfo: CommonJobInfo{
								Name:        "normal",
								Labels:      map[string]string{},
								Annotations: map[string]string{},
								SchedulingPolicy: SchedulingPolicy{
									Queue: MockQueueName,
								},
							},
							JobSpec: JobSpec{
								Image:   "iregistry.baidu-int.com/bmlc/trainingjob:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu",
								Command: "sleep 20",
							},
						},
					},
				},
			},
			wantErr:      true,
			responseCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			res, err := CreatePFJob(tt.args.ctx, tt.args.req)
			t.Logf("case[%s] create job, response=%+v", tt.name, res)
			if tt.wantErr {
				assert.Error(t, err)
				t.Logf("name=%s err: %v", tt.name, err)
			} else {
				assert.Equal(t, nil, err)
				t.Logf("response: %+v", res)
			}
		})
	}

}
