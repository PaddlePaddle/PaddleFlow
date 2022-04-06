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
	"paddleflow/pkg/common/database/dbflag"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
)

var (
	mockUserName     = "user1"
	mockRootUserName = "root"
)

func InitFakeDB() {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
		Logger: glogger.Default.LogMode(glogger.Info),
	})
	if err != nil {
		log.Fatalf("The fake DB doesn't create successfully. Fail fast. error: %v", err)
	}
	// Create tables
	db.AutoMigrate(
		&Grant{},
		&Queue{},
		&ClusterInfo{},
	)
	dbflag.DB = db
}

func TestCreateQueue(t *testing.T) {
	InitFakeDB()
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
	if err := CreateCluster(ctx, &cluster1); err != nil {
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

	CreateQueue(ctx, &queue1)

	CreateQueue(ctx, &queue2)
}

func TestListQueue(t *testing.T) {
	TestCreateQueue(t)
	ctx := &logger.RequestContext{UserName: mockUserName}

	// init grant
	grantModel := &Grant{ID: "fakeID", UserName: mockUserName, ResourceID: "queue1", ResourceType: GrantFsType}
	if err := CreateGrant(ctx, grantModel); err != nil {
		t.Error(err)
	}
	grants, err := ListGrant(ctx, 0, 0, mockUserName)
	if err != nil {
		t.Error(err)
	}
	t.Logf("grants=%+v", grants)

	// case1 list queue
	queueList, err := ListQueue(ctx, 0, 0, "")
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
	queueList, err = ListQueue(ctx, 0, 0, "")
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
	ctx := &logger.RequestContext{UserName: mockUserName}

	queue, err := GetQueueByName(ctx, "queue1")
	if err != nil {
		t.Error(err)
	}
	t.Logf("queue=%+v", queue)
	assert.NotEmpty(t, queue.ClusterName)
}
