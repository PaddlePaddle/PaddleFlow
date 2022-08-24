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

package grant

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	MockRootUser    = "root"
	MockUserName    = "user1"
	MockResourceID  = "fakeID"
	MockClusterName = "fakeCluster"
	MockNamespace   = "paddle"
)

var clusterInfo = model.ClusterInfo{
	Name:          MockClusterName,
	Description:   "Description",
	Endpoint:      "Endpoint",
	Source:        "Source",
	ClusterType:   schema.KubernetesType,
	Version:       "1.16",
	Status:        "Status",
	Credential:    "credential",
	Setting:       "Setting",
	NamespaceList: []string{"n1", "n2", MockNamespace},
}

func TestCreateGrant(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}
	// mock queue & cluster
	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))
	cluser, _ := storage.Cluster.GetClusterByName(MockClusterName)

	err := storage.Queue.CreateQueue(&model.Queue{
		Name:      MockResourceID,
		Namespace: "fake",
		ClusterId: cluser.ID,
	})
	assert.Nil(t, err)
	// momodeler
	mockUser := &model.User{
		UserInfo: model.UserInfo{
			Name: MockUserName, Password: "fake",
		}}
	err = storage.Auth.CreateUser(ctx, mockUser)
	assert.Nil(t, err)

	// case start
	grant := CreateGrantRequest{
		UserName:     MockUserName,
		ResourceType: common.ResourceTypeQueue,
		ResourceID:   MockResourceID,
	}

	resp, err := CreateGrant(ctx, grant)
	assert.Nil(t, err)
	assert.NotNil(t, resp.GrantID)
}

func TestListGrant(t *testing.T) {
	TestCreateGrant(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}
	resp, err := ListGrant(ctx, "", 0, "")
	assert.Nil(t, err)
	assert.NotZero(t, len(resp.GrantList))
}

func TestDeleteGrant(t *testing.T) {
	TestCreateGrant(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}
	err := DeleteGrant(ctx, MockUserName, MockResourceID, common.ResourceTypeQueue)
	assert.Nil(t, err)
}
