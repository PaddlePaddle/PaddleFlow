/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queue

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	MockRootUser    = "root"
	MockClusterName = "testCn"
	MockNamespace   = "paddle"
	MockQueueName   = "mock-q-000"
	MockQueueName1  = "mock-q-001"
	mockRootUser    = "root"
)

var clusterInfo = model.ClusterInfo{
	Name:          MockClusterName,
	Description:   "Description",
	Endpoint:      "Endpoint",
	Source:        "Source",
	ClusterType:   schema.KubernetesType,
	Version:       "1.16",
	Status:        model.ClusterStatusOnLine,
	Credential:    "credential",
	Setting:       "Setting",
	NamespaceList: []string{"n1", "n2", MockNamespace},
}

func TestCreateQueue(t *testing.T) {
	ServerConf := &config.ServerConfig{}
	err := config.InitConfigFromYaml(ServerConf, "../../../../config/server/default/paddleserver.yaml")
	assert.NoError(t, err)
	config.GlobalServerConfig = ServerConf

	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))

	rts := &runtime.KubeRuntime{}
	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "Init", func() error {
		return nil
	})
	defer p2.Reset()

	var p3 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "CreateQueue", func(*api.QueueInfo) error {
		return nil
	})
	defer p3.Reset()
	var p4 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "CreateObject", func(*unstructured.Unstructured) error {
		return nil
	})
	defer p4.Reset()

	type args struct {
		ctx *logger.RequestContext
		req CreateQueueRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "success request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: CreateQueueRequest{
					Name:      MockQueueName,
					Namespace: MockNamespace,
					QuotaType: schema.TypeVolcanoCapabilityQuota,
					MaxResources: schema.ResourceInfo{
						CPU: "1",
						Mem: "1G",
					},
					SchedulingPolicy: []string{"s1", "s2"},
					ClusterName:      MockClusterName,
				},
			},
			wantErr: nil,
		},
		{
			name: "wrong request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: CreateQueueRequest{
					Name:      "aBC",
					Namespace: MockNamespace,
					QuotaType: schema.TypeVolcanoCapabilityQuota,
					MaxResources: schema.ResourceInfo{
						CPU: "1",
						Mem: "1G",
					},
					SchedulingPolicy: []string{"s1", "s2"},
					ClusterName:      MockClusterName,
				},
			},
			wantErr: fmt.Errorf("a lowercase RFC 1123 label must consist of"),
		},
		{
			name: "not in the specified values",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: CreateQueueRequest{
					Name:      MockQueueName1,
					Namespace: "new1",
					QuotaType: schema.TypeVolcanoCapabilityQuota,
					MaxResources: schema.ResourceInfo{
						CPU: "1",
						Mem: "1G",
					},
					SchedulingPolicy: []string{"s1", "s2"},
					ClusterName:      MockClusterName,
				},
			},
			wantErr: fmt.Errorf("not in the specified values"),
		},
		{
			name: "not exist in runtime",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: CreateQueueRequest{
					Name:      MockQueueName1,
					Namespace: "n1",
					QuotaType: schema.TypeVolcanoCapabilityQuota,
					MaxResources: schema.ResourceInfo{
						CPU: "1",
						Mem: "1G",
					},
					SchedulingPolicy: []string{"s1", "s2"},
					ClusterName:      MockClusterName,
				},
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			resp, err := CreateQueue(ctx, &tt.args.req)
			if err != nil {
				assert.NotNil(t, tt.wantErr)
				assert.Contains(t, err.Error(), tt.wantErr.Error())
			} else {
				assert.Nil(t, tt.wantErr)
				t.Logf("case[%s] create queue resp=%#v", tt.name, resp)
			}
		})
	}
}

func TestGetQueueByName(t *testing.T) {
	TestCreateQueue(t)

	rts := &runtime.KubeRuntime{}
	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "Init", func() error {
		return nil
	})
	defer p2.Reset()

	var p3 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "GetQueueUsedQuota", func(*api.QueueInfo) (*resources.Resource, error) {
		return resources.EmptyResource(), nil
	})
	defer p3.Reset()
	ctx := &logger.RequestContext{UserName: MockRootUser}
	if queue, err := GetQueueByName(ctx, MockQueueName); err != nil {
		t.Error(err)
	} else {
		assert.Equal(t, queue.ClusterName, MockClusterName)
		assert.Equal(t, queue.Namespace, MockNamespace)
	}
}

func TestListQueue(t *testing.T) {
	TestCreateQueue(t)

	ctx := &logger.RequestContext{UserName: MockRootUser}

	if queues, err := ListQueue(ctx, "", 0, MockQueueName); err != nil {
		t.Error(err)
	} else {
		for _, queue := range queues.QueueList {
			assert.Equal(t, queue.ClusterName, MockClusterName)
			assert.Equal(t, queue.Namespace, MockNamespace)
		}
	}
}

func TestCloseAndDeleteQueue(t *testing.T) {
	TestCreateQueue(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}

	rts := &runtime.KubeRuntime{}
	var p1 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "Init", func() error {
		return nil
	})
	defer p1.Reset()
	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "DeleteQueue", func(*api.QueueInfo) error {
		return nil
	})
	defer p2.Reset()

	err := DeleteQueue(ctx, MockQueueName)
	assert.Nil(t, err)
}

// TestMarshalJSONForTime test for time format
func TestMarshalJSONForTime(t *testing.T) {
	driver.InitMockDB()
	queue := model.Queue{
		Name: "mockQueueName",
	}
	err := storage.Queue.CreateQueue(&queue)
	if err != nil {
		t.Errorf(err.Error())
	}
	t.Logf("queue=%+v", queue)
	queueStr, err := json.Marshal(queue)
	t.Logf("json.Marshal(queue)=%+v", string(queueStr))
}
