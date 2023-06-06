/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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

package resourcepool

import (
	"fmt"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"strings"
	"testing"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

var (
	MockClusterName  = "default-cluster"
	MockClusterName2 = "testCn2"
	MockClusterID    = "mock-cluster-id"
	MockRPName       = "test-rp"
	MockUserID       = "mock-user-id"
	MockNamespace    = "paddle"
	clusterInfo      = model.ClusterInfo{
		Model:         model.Model{ID: MockClusterID},
		Name:          MockClusterName,
		Description:   "Description",
		Endpoint:      "Endpoint",
		Source:        "Source",
		ClusterType:   schema.KubernetesType,
		Version:       "1.16",
		Status:        model.ClusterStatusOnLine,
		Credential:    "credential",
		Setting:       "Setting",
		NamespaceList: []string{"n1", "n2", "n3", "n4", MockNamespace},
	}
	offlineClusterInfo = model.ClusterInfo{
		Name:          MockClusterName2,
		Description:   "Description",
		Endpoint:      "Endpoint",
		Source:        "Source",
		ClusterType:   schema.KubernetesType,
		Version:       "1.16",
		Status:        model.ClusterStatusOffLine,
		Credential:    "credential",
		Setting:       "Setting",
		NamespaceList: []string{"n1", "n2", "n3", "n4", MockNamespace},
	}
	rp1 = model.ResourcePool{
		Name:        MockRPName,
		Namespace:   "default",
		Type:        "dev",
		ClusterId:   MockClusterID,
		Status:      schema.StatusQueueOpen,
		CreatorID:   MockUserID,
		Provider:    "cce",
		Description: "test resource pool",
		ClusterName: MockClusterName,
	}
	rp2 = model.ResourcePool{
		Name:        "mock-rp-2",
		Namespace:   "default",
		Type:        "predict",
		ClusterId:   MockClusterID,
		Status:      schema.StatusQueueOpen,
		CreatorID:   MockUserID,
		Provider:    "cce",
		Description: "predict resource pool",
		ClusterName: MockClusterName,
	}
	rp3 = model.ResourcePool{
		Name:        "mock-rp-3",
		Namespace:   "default",
		Type:        "train",
		ClusterId:   MockClusterID,
		Status:      schema.StatusQueueOpen,
		CreatorID:   MockUserID,
		Provider:    "cce",
		Description: "train resource pool",
		ClusterName: MockClusterName,
	}
)

func TestCreateRequest_Validate(t *testing.T) {
	testCases := []struct {
		name    string
		request *CreateRequest
		err     error
	}{
		{
			name: "validate name failed",
			request: &CreateRequest{
				Name: "2_name-test",
			},
			err: fmt.Errorf("name[2_name-test] is invalid, err: a lowercase RFC 1123 label must consist of " +
				"lower case alphanumeric characters or '-', and must start and end with an alphanumeric character " +
				"(regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')"),
		},
		{
			name: "name is empty",
			request: &CreateRequest{
				Namespace: "default",
			},
			err: fmt.Errorf("name is emtpy"),
		},
		{
			name: "namespace is empty",
			request: &CreateRequest{
				Name: "test-pod",
			},
			err: fmt.Errorf("namespace is emtpy"),
		},
		{
			name: "field type is more than 32",
			request: &CreateRequest{
				Name:      "test-pod",
				Namespace: "default",
				Type:      "aaaabbbbccccddddaaaabbbbccccddddee",
			},
			err: fmt.Errorf("the value of field [type] is invalid, length should be less than 32"),
		},
		{
			name: "field provider is more than 32",
			request: &CreateRequest{
				Name:      "test-pod",
				Namespace: "default",
				Type:      "dev",
				Provider:  "aaaabbbbccccddddaaaabbbbccccddddee",
			},
			err: fmt.Errorf("the value of field [provider] is invalid, length should be less than 32"),
		},
		{
			name: "field description is more than 2048",
			request: &CreateRequest{
				Name:        "test-pod",
				Namespace:   "default",
				Type:        "dev",
				Provider:    "cce",
				Description: strings.Repeat("desc", 1000),
			},
			err: fmt.Errorf("the value of field [description] is invalid, length should be less than 2048"),
		},
		{
			name: "cluster not found",
			request: &CreateRequest{
				Name:        "test-pod",
				Namespace:   "default",
				Type:        "dev",
				Provider:    "cce",
				Description: "test resource pool",
				ClusterName: "default-cluster-2",
			},
			err: fmt.Errorf("cluster not found by Name"),
		},
		{
			name: "cluster status is offline",
			request: &CreateRequest{
				Name:        "test-pod",
				Namespace:   "test",
				Type:        "dev",
				Provider:    "cce",
				Description: "test resource pool",
				ClusterName: MockClusterName2,
			},
			err: fmt.Errorf("cluster[testCn2] not in online status, operator not permit"),
		},
		{
			name: "resource pool quota error",
			request: &CreateRequest{
				Name:        "test-pod",
				Namespace:   "test",
				Type:        "dev",
				Provider:    "cce",
				Description: "test resource pool",
				ClusterName: "",
				QuotaType:   "xx",
			},
			err: fmt.Errorf("the type of quota [xx] is not supported"),
		},
		{
			name: "resource pool total resource is invalid",
			request: &CreateRequest{
				Name:        "test-pod",
				Namespace:   "test",
				Type:        "dev",
				Provider:    "cce",
				Description: "test resource pool",
				ClusterName: MockClusterName,
				TotalResources: schema.ResourceInfo{
					CPU: "10",
					Mem: "20xGi",
				},
			},
			err: fmt.Errorf("quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'"),
		},
		{
			name: "resource pool total resource is negative",
			request: &CreateRequest{
				Name:        "test-pod",
				Namespace:   "test",
				Type:        "dev",
				Provider:    "cce",
				Description: "test resource pool",
				ClusterName: MockClusterName,
				TotalResources: schema.ResourceInfo{
					CPU: "-10",
					Mem: "20Gi",
				},
			},
			err: fmt.Errorf("negative resources not permitted: map[cpu:-10 memory:20Gi]"),
		},
		{
			name: "validate CreateRequest successfully",
			request: &CreateRequest{
				Name:        "test-pod",
				Namespace:   "test",
				Type:        "dev",
				Provider:    "cce",
				Description: "test resource pool",
				ClusterName: MockClusterName,
				TotalResources: schema.ResourceInfo{
					CPU: "10",
					Mem: "20Gi",
				},
			},
		},
	}

	driver.InitMockDB()
	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))
	assert.Nil(t, storage.Cluster.CreateCluster(&offlineClusterInfo))
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &logger.RequestContext{UserName: "mock-user"}
			err := tc.request.Validate(ctx)
			assert.Equal(t, tc.err, err)
		})
	}
}

func TestCreate(t *testing.T) {
	testCases := []struct {
		name     string
		request  *CreateRequest
		userName string
		err      error
	}{
		{
			name: "permission deny",
			request: &CreateRequest{
				Name:        "test-pod",
				Namespace:   "test",
				Type:        "dev",
				Provider:    "cce",
				Description: "test resource pool",
				ClusterName: MockClusterName,
				TotalResources: schema.ResourceInfo{
					CPU: "10",
					Mem: "20Gi",
				},
			},
			userName: "mock-user",
			err:      fmt.Errorf("admin is needed"),
		},
		{
			name: "create request validate failed",
			request: &CreateRequest{
				Name:        "test-pod",
				Namespace:   "test",
				Type:        "dev",
				Provider:    "cce",
				Description: "test resource pool",
				ClusterName: MockClusterName,
				TotalResources: schema.ResourceInfo{
					CPU: "-10",
					Mem: "20Gi",
				},
			},
			userName: "root",
			err:      fmt.Errorf("negative resources not permitted: map[cpu:-10 memory:20Gi]"),
		},
		{
			name: "create resource pool success",
			request: &CreateRequest{
				Name:        "test-pod",
				Namespace:   "test",
				Type:        "dev",
				Provider:    "cce",
				Description: "test resource pool",
				ClusterName: MockClusterName,
				TotalResources: schema.ResourceInfo{
					CPU: "10",
					Mem: "20Gi",
				},
				IsHybrid: true,
			},
			userName: "root",
			err:      nil,
		},
	}

	driver.InitMockDB()
	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))
	assert.Nil(t, storage.Cluster.CreateCluster(&offlineClusterInfo))
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &logger.RequestContext{UserName: tc.userName}
			_, err := Create(ctx, tc.request)
			assert.Equal(t, tc.err, err)
		})
	}
}

func TestUpdateRequest_Validate(t *testing.T) {
	testCases := []struct {
		name       string
		userName   string
		request    *UpdateRequest
		needUpdate bool
		err        error
	}{
		{
			name:       "not upate",
			request:    &UpdateRequest{},
			needUpdate: false,
			err:        nil,
		},
		{
			name: "update the status of resource pool failed",
			request: &UpdateRequest{
				Status: schema.StatusQueueClosing,
			},
			needUpdate: false,
			err:        fmt.Errorf("the status of resource pool[closing] is invalid"),
		},
		{
			name: "update the annotations of resource pool failed",
			request: &UpdateRequest{
				Annotations: map[string]string{
					v1beta1.QuotaTypeKey: v1beta1.QuotaTypeLogical,
				},
			},
			needUpdate: false,
			err:        fmt.Errorf("the isolaction type of resource pool cannot be changed"),
		},
		{
			name: "update the cpu resource of resource pool failed",
			request: &UpdateRequest{
				TotalResources: schema.ResourceInfo{
					CPU: "20x",
					Mem: "10Gi",
				},
			},
			needUpdate: false,
			err:        fmt.Errorf("parse cpu resource[20x] failed"),
		},
		{
			name: "update the mem resource of resource pool failed",
			request: &UpdateRequest{
				TotalResources: schema.ResourceInfo{
					CPU: "20",
					Mem: "-10Gi",
				},
			},
			needUpdate: false,
			err:        fmt.Errorf("parse memory resource[-10Gi] failed"),
		},
		{
			name: "update the scalar resource of resource pool failed",
			request: &UpdateRequest{
				TotalResources: schema.ResourceInfo{
					CPU: "20",
					Mem: "10Gi",
					ScalarResources: schema.ScalarResourcesType{
						"nvidia.com/gpu": "-10",
					},
				},
			},
			needUpdate: false,
			err:        fmt.Errorf("parse scalarResource nvidia.com/gpu resource[-10] failed"),
		},
		{
			name: "update resource pool successfully",
			request: &UpdateRequest{
				Status: schema.StatusQueueOpen,
				Annotations: map[string]string{
					"region": "bj",
					"test":   "",
				},
				TotalResources: schema.ResourceInfo{
					CPU: "20",
					Mem: "10Gi",
					ScalarResources: schema.ScalarResourcesType{
						"nvidia.com/gpu": "10",
					},
				},
			},
			needUpdate: true,
		},
	}

	driver.InitMockDB()
	res, err1 := resources.NewResourceFromMap(map[string]string{
		"cpu":    "10",
		"memory": "20Gi",
	})
	assert.Nil(t, err1)
	rpInfo := &model.ResourcePool{
		Name:      "test-pod",
		Namespace: "default",
		Type:      "dev",
		Provider:  "cce",
		QuotaType: schema.TypeElasticQuota,
		Status:    schema.StatusQueueOpen,
		Annotations: map[string]string{
			v1beta1.QuotaTypeKey: v1beta1.QuotaTypePhysical,
		},
		TotalResources: model.Resource(*res),
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &logger.RequestContext{UserName: tc.userName}
			needUpdated, err := tc.request.Validate(ctx, rpInfo)
			assert.Equal(t, tc.err, err)
			assert.Equal(t, tc.needUpdate, needUpdated)
		})
	}
}

func TestList(t *testing.T) {
	testCases := []struct {
		name     string
		request  ListRequest
		userName string
		wantLen  int
		err      error
	}{
		{
			name: "list with maxKeys 1",
			request: ListRequest{
				MaxKeys: 1,
			},
			userName: "mock-user",
			wantLen:  1,
		},
		{
			name: "list with maxKeys 2",
			request: ListRequest{
				MaxKeys: 2,
			},
			userName: "mock-user",
			wantLen:  2,
		},
		{
			name: "list with maxKeys 3",
			request: ListRequest{
				MaxKeys: 3,
			},
			userName: "mock-user",
			wantLen:  3,
		},
		{
			name: "list with marker",
			request: ListRequest{
				Marker:  "7cb58c3f0b696c392472fe5af73c227d",
				MaxKeys: 2,
			},
			userName: "mock-user",
			wantLen:  1,
		},
		{
			name: "list with wrong marker",
			request: ListRequest{
				Marker:  "xx",
				MaxKeys: 2,
			},
			userName: "mock-user",
			err:      fmt.Errorf("decrypt marker[xx] failed. err:[encoding/hex: invalid byte: U+0078 'x']"),
			wantLen:  0,
		},
		{
			name: "list with wrong marker",
			request: ListRequest{
				Marker:  "xx",
				MaxKeys: 2,
			},
			userName: "mock-user",
			err:      fmt.Errorf("decrypt marker[xx] failed. err:[encoding/hex: invalid byte: U+0078 'x']"),
			wantLen:  0,
		},
	}

	driver.InitMockDB()
	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))
	assert.Nil(t, storage.ResourcePool.Create(&rp1))
	assert.Nil(t, storage.ResourcePool.Create(&rp2))
	assert.Nil(t, storage.ResourcePool.Create(&rp3))
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &logger.RequestContext{UserName: tc.userName}
			rpList, err := List(ctx, tc.request)
			assert.Equal(t, tc.err, err)
			assert.Equal(t, tc.wantLen, len(rpList.ResourcePoolList))
			t.Logf(rpList.NextMarker)
		})
	}
}

func TestGetByName(t *testing.T) {
	testCases := []struct {
		caseName string
		rpName   string
		userName string
		err      error
	}{
		{
			caseName: "rp name not found",
			rpName:   "xx",
			err:      fmt.Errorf("resource pool[xx] not found"),
		},
		{
			caseName: "get rp successfully",
			userName: "root",
			rpName:   MockRPName,
		},
	}

	driver.InitMockDB()
	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))
	assert.Nil(t, storage.ResourcePool.Create(&rp1))
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			ctx := &logger.RequestContext{UserName: tc.userName}
			_, err := GetByName(ctx, tc.rpName)
			assert.Equal(t, tc.err, err)
		})
	}
}

func TestDelete(t *testing.T) {
	testCases := []struct {
		caseName string
		rpName   string
		userName string
		err      error
	}{
		{
			caseName: "permission deny",
			rpName:   "xx",
			userName: "mock_user",
			err:      fmt.Errorf("delete resource pool failed. error: admin is needed"),
		},
		{
			caseName: "rp name not found",
			rpName:   "xx",
			userName: "root",
			err:      fmt.Errorf("resource pool:xx not found"),
		},
		{
			caseName: "delete rp success",
			rpName:   MockRPName,
			userName: "root",
		},
	}

	driver.InitMockDB()
	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))
	assert.Nil(t, storage.ResourcePool.Create(&rp1))
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			ctx := &logger.RequestContext{UserName: tc.userName}
			err := Delete(ctx, tc.rpName)
			assert.Equal(t, tc.err, err)
		})
	}
}
