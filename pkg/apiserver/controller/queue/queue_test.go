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
	kschema "k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime/kubernetes/executor"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	MockRootUser    = "root"
	MockClusterName = "testCn"
	MockNamespace   = "paddle"
	MockQueueName   = "mock-q-001"
)

var clusterInfo = models.ClusterInfo{
	Name:          MockClusterName,
	Description:   "Description",
	Endpoint:      "Endpoint",
	Source:        "Source",
	ClusterType:   schema.KubernetesType,
	Version:       "1.16",
	Status:        models.ClusterStatusOnLine,
	Credential:    "credential",
	Setting:       "Setting",
	NamespaceList: []string{"n1", "n2", MockNamespace},
}

func TestCreateQueue(t *testing.T) {
	ServerConf := &config.ServerConfig{}
	err := config.InitConfigFromYaml(ServerConf, "../../../../config/server/default/paddleserver.yaml")
	config.GlobalServerConfig = ServerConf

	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	assert.Nil(t, models.CreateCluster(&clusterInfo))

	rts := &runtime.KubeRuntime{}
	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "Init", func() error {
		return nil
	})
	defer p2.Reset()

	var p3 = gomonkey.ApplyFunc(executor.Create, func(resource interface{}, gvk kschema.GroupVersionKind, clientOpt *k8s.DynamicClientOption) error {
		return nil
	})
	defer p3.Reset()

	createQueueReq := CreateQueueRequest{
		Name:      "mockQueueName",
		Namespace: MockNamespace,
		QuotaType: schema.TypeVolcanoCapabilityQuota,
		MaxResources: schema.ResourceInfo{
			CPU: "1",
			Mem: "1G",
		},
		SchedulingPolicy: []string{"s1", "s2"},
		ClusterName:      MockClusterName,
	}
	// test queue name
	resp, err := CreateQueue(ctx, &createQueueReq)
	expectErrMsg := fmt.Sprintf("name[%s] of queue is invalid", createQueueReq.Name)
	if err != nil {
		assert.Contains(t, err.Error(), expectErrMsg)
	}

	// test create queue
	createQueueReq.Name = MockQueueName
	err = nil
	resp, err = CreateQueue(ctx, &createQueueReq)
	assert.Nil(t, err)

	t.Logf("resp=%v", resp)
}

func TestGetQueueByName(t *testing.T) {
	TestCreateQueue(t)

	rts := &runtime.KubeRuntime{}
	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "Init", func() error {
		return nil
	})
	defer p2.Reset()

	var p3 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "GetQueueUsedQuota", func(*models.Queue) (*schema.ResourceInfo, error) {
		return schema.EmptyResourceInfo(), nil
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
	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "DeleteQueue", func(q *models.Queue) error {
		return nil
	})
	defer p2.Reset()
	var p3 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "CloseQueue", func(q *models.Queue) error {
		return nil
	})
	defer p3.Reset()

	err := DeleteQueue(ctx, MockQueueName)
	assert.Nil(t, err)
}

// TestMarshalJSONForTime test for time format
func TestMarshalJSONForTime(t *testing.T) {
	driver.InitMockDB()
	queue := models.Queue{
		Name: "mockQueueName",
	}
	err := models.CreateQueue(&queue)
	if err != nil {
		t.Errorf(err.Error())
	}
	t.Logf("queue=%+v", queue)
	queueStr, err := json.Marshal(queue)
	t.Logf("json.Marshal(queue)=%+v", string(queueStr))
}
