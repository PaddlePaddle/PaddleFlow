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

package cluster

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
	"github.com/PaddlePaddle/PaddleFlow/pkg/service/db_service"
)

const (
	MockRootUser    = "root"
	MockClusterName = "testCn"
)

func TestCreateCluster(t *testing.T) {
	database.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	createClusterReq := CreateClusterRequest{
		Name: MockClusterName,
	}

	createClusterReq.ID = ""
	createClusterReq.Description = ""
	resp, err := CreateCluster(ctx, &createClusterReq)
	assert.Equal(t, "Endpoint should not be empty", err.Error())

	createClusterReq.Endpoint = "127.0.0.1"
	createClusterReq.Source = ""
	createClusterReq.ClusterType = schema.KubernetesType
	createClusterReq.Version = "1.16"
	createClusterReq.Status = db_service.ClusterStatusOnLine
	createClusterReq.Credential = ""
	createClusterReq.Setting = ""
	createClusterReq.NamespaceList = []string{"n1", "n2"}
	// case1 test CreateCluster
	resp, err = CreateCluster(ctx, &createClusterReq)
	assert.NotNil(t, err)
	err = nil

	// case2 client init success
	rts := &runtime.KubeRuntime{}
	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "Init", func() error {
		return nil
	})
	defer p2.Reset()
	resp, err = CreateCluster(ctx, &createClusterReq)

	assert.Equal(t, createClusterReq.Status, resp.Status)
	t.Logf("resp=%v", resp)
}

func TestGetCluster(t *testing.T) {
	TestCreateCluster(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}

	// test get clusterInfo
	resp, err := GetCluster(ctx, MockClusterName)
	assert.Nil(t, err)
	// expect status changes from online to offline
	assert.Equal(t, MockClusterName, resp.Name)
	t.Logf("resp=%v", resp)
}

func TestUpdateCluster(t *testing.T) {
	TestCreateCluster(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}
	updateClusterRequest := UpdateClusterRequest{
		ClusterCommonInfo: ClusterCommonInfo{
			ClusterType: schema.LocalType,
		},
	}

	// test update clusterInfo
	resp, err := UpdateCluster(ctx, MockClusterName, &updateClusterRequest)
	assert.Nil(t, err)
	// expect status changes from online to offline
	assert.Equal(t, MockClusterName, resp.Name)
	t.Logf("resp=%v", resp)
}

func TestListCluster(t *testing.T) {
	TestCreateCluster(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}
	clusterNameList := []string{MockClusterName, "fake"}
	resp, err := ListCluster(ctx, "", 0, clusterNameList, "")
	assert.Nil(t, err)
	assert.NotZero(t, len(resp.ClusterList))
	// expect status changes from online to offline
	for _, cluster := range resp.ClusterList {
		assert.NotEmpty(t, cluster.Name)
		t.Logf("cluster=%v", cluster)
	}

}

func TestDeleteCluster(t *testing.T) {
	TestCreateCluster(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}
	err := DeleteCluster(ctx, MockClusterName)
	assert.Nil(t, err)
}
