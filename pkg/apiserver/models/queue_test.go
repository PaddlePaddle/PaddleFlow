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

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

var (
	mockUserName     = "user1"
	mockRootUserName = "root"
)

func TestCreateQueue(t *testing.T) {
	initMockDB()

	cluster1 := ClusterInfo{
		Name:          "cluster1",
		Description:   "Description",
		Endpoint:      "127.0.0.1:6655",
		Source:        "Source",
		ClusterType:   schema.KubernetesType,
		Version:       "1.16",
		Status:        "Status",
		Credential:    "credential",
		Setting:       "Setting",
		NamespaceList: []string{"n1", "n2"},
	}
	if err := CreateCluster(&cluster1); err != nil {
		t.Error(err)
	}
	assert.NotEmpty(t, cluster1.ID)

	r1, err := resources.NewResourceFromMap(map[string]string{
		"cpu":            "10",
		"mem":            "100G",
		"nvidia.com/gpu": "500",
	})
	assert.Equal(t, nil, err)

	r2, err := resources.NewResourceFromMap(map[string]string{
		"cpu":            "20",
		"mem":            "200G",
		"nvidia.com/gpu": "200",
	})
	assert.Equal(t, nil, err)

	queue1 := Queue{
		Name:             "queue1",
		Namespace:        "paddleflow",
		ClusterId:        cluster1.ID,
		QuotaType:        schema.TypeVolcanoCapabilityQuota,
		MaxResources:     r1,
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueCreating,
	}

	queue2 := Queue{
		Name:             "queue2",
		Namespace:        "paddleflow",
		ClusterId:        "cluster1.ID",
		QuotaType:        schema.TypeVolcanoCapabilityQuota,
		MaxResources:     r2,
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueCreating,
	}

	err = CreateQueue(&queue1)
	assert.Equal(t, nil, err)

	err = CreateQueue(&queue2)
	assert.Equal(t, nil, err)
}

func TestUpdateQueue(t *testing.T) {
	initMockDB()

	cluster1 := ClusterInfo{
		Name:          "cluster1",
		Description:   "Description",
		Endpoint:      "127.0.0.1:6655",
		Source:        "Source",
		ClusterType:   schema.KubernetesType,
		Version:       "1.16",
		Status:        "Status",
		Credential:    "credential",
		Setting:       "Setting",
		NamespaceList: []string{"n1", "n2"},
	}
	if err := CreateCluster(&cluster1); err != nil {
		t.Error(err)
	}
	assert.NotEmpty(t, cluster1.ID)

	r1, err := resources.NewResourceFromMap(map[string]string{
		"cpu":            "10",
		"mem":            "100G",
		"nvidia.com/gpu": "500",
	})
	assert.Equal(t, nil, err)

	r2, err := resources.NewResourceFromMap(map[string]string{
		"cpu":            "1",
		"mem":            "10G",
		"nvidia.com/gpu": "500",
	})
	assert.Equal(t, nil, err)

	r3, err := resources.NewResourceFromMap(map[string]string{
		"cpu":            "10",
		"mem":            "100G",
		"nvidia.com/gpu": "5",
	})
	assert.Equal(t, nil, err)

	queue1 := Queue{
		Name:             "queue1",
		Namespace:        "paddleflow",
		ClusterId:        cluster1.ID,
		QuotaType:        schema.TypeVolcanoCapabilityQuota,
		MaxResources:     r1,
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueCreating,
	}

	queue2 := Queue{
		Name:             "queue2",
		Namespace:        "paddleflow",
		ClusterId:        "cluster1.ID",
		QuotaType:        schema.TypeVolcanoCapabilityQuota,
		MaxResources:     r2,
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueCreating,
	}

	err = CreateQueue(&queue1)
	assert.Equal(t, nil, err)

	err = CreateQueue(&queue2)
	assert.Equal(t, nil, err)

	queue1.MaxResources = r3
	err = UpdateQueue(&queue1)
	assert.NoError(t, err)
}

func TestListQueue(t *testing.T) {
	TestCreateQueue(t)
	ctx := &logger.RequestContext{UserName: mockUserName}

	// init grant
	grantModel := &model.Grant{ID: "fakeID", UserName: mockUserName, ResourceID: "queue1", ResourceType: GrantFsType}
	if err := storage.Auth.CreateGrant(ctx, grantModel); err != nil {
		t.Error(err)
	}
	grants, err := storage.Auth.ListGrant(ctx, 0, 0, mockUserName)
	if err != nil {
		t.Error(err)
	}
	t.Logf("grants=%+v", grants)

	// case1 list queue
	queueList, err := ListQueue(0, 0, "", "")
	if err != nil {
		ctx.Logging().Errorf("models list queue failed. err:[%s]", err.Error())
		ctx.ErrorCode = common.InternalError
	}
	for _, queue := range queueList {
		assert.NotEmpty(t, queue.ClusterName)
	}
	t.Logf("%+v", queueList)

	// case2 for root
	ctx = &logger.RequestContext{UserName: mockRootUserName}
	queueList, err = ListQueue(0, 0, "", "")
	if err != nil {
		ctx.Logging().Errorf("models list queue failed. err:[%s]", err.Error())
		ctx.ErrorCode = common.InternalError
	}
	for _, queue := range queueList {
		assert.NotEmpty(t, queue.ClusterName)
	}
	t.Logf("%+v", queueList)
}

func TestGetQueueByName(t *testing.T) {
	TestCreateQueue(t)

	queue, err := GetQueueByName("queue1")
	if err != nil {
		t.Error(err)
	}
	t.Logf("queue=%+v", queue)
	assert.NotEmpty(t, queue.ClusterName)
}
