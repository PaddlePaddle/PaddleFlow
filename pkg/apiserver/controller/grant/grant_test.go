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

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/database/db_fake"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"

)

const (
	MockRootUser = "root"
	MockUserName = "user1"
	MockResourceID = "fakeID"
	MockClusterName = "fakeCluster"
	MockNamespace  = "paddle"
)

var clusterInfo = models.ClusterInfo{
	Name:         MockClusterName,
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
//CGO_ENABLED=1
func TestCreateGrant(t *testing.T) {
	db_fake.InitFakeDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}
	// mock queue & cluster
	assert.Nil(t, models.CreateCluster(ctx, &clusterInfo))
	cluser, _ := models.GetClusterByName(ctx, MockClusterName)

	err := models.CreateQueue(ctx, &models.Queue{
		Name: MockResourceID,
		Namespace: "fake",
		ClusterId: cluser.ID,
	})
	assert.Nil(t, err)
	// mock user
	mockUser := &models.User{
		UserInfo: models.UserInfo{
			Name: MockUserName, Password: "fake",
		}}
	err = models.CreateUser(ctx, mockUser)
	assert.Nil(t, err)

	// case start
	grant := &models.Grant{
		UserName: MockUserName,
		ResourceType: common.ResourceTypeQueue,
		ResourceID: MockResourceID,
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

	// mock queue
	var p1 = gomonkey.ApplyFunc(models.GetQueueByName, func(ctx *logger.RequestContext, queueName string) (models.Queue, error) {
		return models.Queue{}, nil
	})
	defer p1.Reset()

	err := DeleteGrant(ctx, MockUserName, MockResourceID, common.ResourceTypeQueue)
	assert.Nil(t, err)
}




