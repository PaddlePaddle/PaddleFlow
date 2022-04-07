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
	"gorm.io/gorm"
	"paddleflow/pkg/common/database"
	"testing"

	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
)

var (
	mockUserName     = "user1"
	mockRootUserName = "root"
)

func createQueue(t *testing.T) *gorm.DB {
	db := database.InitMockDB()
	ctx := &logger.RequestContext{UserName: mockUserName}

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
	if err := CreateCluster(db, ctx, &cluster1); err != nil {
		t.Error(err)
	}
	assert.NotEmpty(t, cluster1.ID)

	queue1 := Queue{
		Name:      "queue1",
		Namespace: "paddleflow",
		ClusterId: cluster1.ID,
		QuotaType: schema.TypeVolcanoCapabilityQuota,
		MaxResources: schema.ResourceInfo{
			CPU: "10",
			Mem: "100G",
			ScalarResources: schema.ScalarResourcesType{
				"nvidia.com/gpu": "500",
			},
		},
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueCreating,
	}

	queue2 := Queue{
		Name:      "queue2",
		Namespace: "paddleflow",
		ClusterId: "cluster1.ID",
		QuotaType: schema.TypeVolcanoCapabilityQuota,
		MaxResources: schema.ResourceInfo{
			CPU: "20",
			Mem: "200G",
			ScalarResources: schema.ScalarResourcesType{
				"nvidia.com/gpu": "200",
			},
		},
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueCreating,
	}

	CreateQueue(db, ctx, &queue1)

	CreateQueue(db, ctx, &queue2)
	return db
}

func TestCreateQueue(t *testing.T) {
	createQueue(t)
}

func TestListQueue(t *testing.T) {
	db := createQueue(t)
	ctx := &logger.RequestContext{UserName: mockUserName}

	// init grant
	grantModel := &Grant{ID: "fakeID", UserName: mockUserName, ResourceID: "queue1", ResourceType: GrantFsType}
	if err := CreateGrant(db, ctx, grantModel); err != nil {
		t.Error(err)
	}
	grants, err := ListGrant(db, ctx, 0, 0, mockUserName)
	if err != nil {
		t.Error(err)
	}
	t.Logf("grants=%+v", grants)

	// case1 list queue
	queueList, err := ListQueue(db, ctx, 0, 0, "")
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
	queueList, err = ListQueue(db, ctx, 0, 0, "")
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
	db := createQueue(t)
	ctx := &logger.RequestContext{UserName: mockUserName}

	queue, err := GetQueueByName(db, ctx, "queue1")
	if err != nil {
		t.Error(err)
	}
	t.Logf("queue=%+v", queue)
	assert.NotEmpty(t, queue.ClusterName)
}
