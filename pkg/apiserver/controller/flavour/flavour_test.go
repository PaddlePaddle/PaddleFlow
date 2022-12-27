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

package flavour

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	MockRootUser    = "root"
	MockNonRootUser = "abc"
	MockClusterName = "testCn"
	MockClusterID   = "testClusterID"
	MockNamespace   = "paddle"
	MockFlavourName = "flavour1"
)

var clusterInfo = model.ClusterInfo{
	Model:         model.Model{ID: MockClusterID},
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

func initCluster(t *testing.T) {
	err := storage.Cluster.CreateCluster(&clusterInfo)
	assert.Nil(t, err)
}

func TestListFlavour(t *testing.T) {
	driver.InitMockDB()

	initCluster(t)
	// insert flavour
	num := 20
	clusterNum := 5
	for i := 0; i < num; i++ {
		flavour := model.Flavour{
			Name: fmt.Sprintf("flavour%d", i),
			CPU:  "20",
			Mem:  "20G",
			ScalarResources: map[schema.ResourceName]string{
				"nvidia.com/gpu": "1",
			},
		}
		if i < clusterNum {
			flavour.ClusterID = MockClusterID
		}

		err := storage.Flavour.CreateFlavour(&flavour)
		assert.Nil(t, err)
	}

	// base query
	flavours, err := ListFlavour(0, "", "", "")
	assert.Nil(t, err)
	assert.Equal(t, num-clusterNum, len(flavours.FlavourList))

	// base query with limit
	var limit = 10
	flavours, err = ListFlavour(limit, "", "", "")
	assert.Nil(t, err)
	assert.Equal(t, limit, len(flavours.FlavourList))

	// with clusterName
	flavours, err = ListFlavour(limit, "", MockClusterName, "")
	assert.Nil(t, err)
	assert.Equal(t, limit, len(flavours.FlavourList))

	// with clusterName
	flavours, err = ListFlavour(limit, "1", MockClusterName, "")
	assert.Error(t, err)

}

func TestCreateFlavour(t *testing.T) {
	driver.InitMockDB()
	initCluster(t)
	createFlavourReq := CreateFlavourRequest{
		Name: MockFlavourName,
		CPU:  "20",
		Mem:  "20G",
		ScalarResources: map[schema.ResourceName]string{
			"com/gpu": "1",
		},
		ClusterID: MockClusterID,
		UserName:  MockRootUser,
	}
	resp, err := CreateFlavour(&createFlavourReq)

	assert.NoError(t, err)
	assert.Equal(t, createFlavourReq.Name, resp.FlavourName)
	t.Logf("resp=%v", resp)
}

func TestUpdateFlavour(t *testing.T) {
	TestCreateFlavour(t)

	type args struct {
		ctx *logger.RequestContext
		req UpdateFlavourRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "root update request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockRootUser,
				},
				req: UpdateFlavourRequest{
					Name: MockFlavourName,
					CPU:  "40",
					Mem:  "20G",
					ScalarResources: map[schema.ResourceName]string{
						"nvidia.com/gpu": "1",
					},
					ClusterID: MockClusterID,
					UserName:  MockRootUser,
				},
			},
			wantErr: false,
		},
		{
			name: "root update request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockRootUser,
				},
				req: UpdateFlavourRequest{
					Name: MockFlavourName,
					CPU:  "40",
					Mem:  "4G",
					ScalarResources: map[schema.ResourceName]string{
						"nvidia.com/gpu": "1",
					},
					ClusterID: MockClusterID,
					UserName:  MockRootUser,
				},
			},
			wantErr: false,
		},
		{
			name: "wrong cluster request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockRootUser,
				},
				req: UpdateFlavourRequest{
					Name: MockFlavourName,
					CPU:  "40",
					Mem:  "4G",
					ScalarResources: map[schema.ResourceName]string{
						"nvidia.com/gpu": "1",
					},
					ClusterID:   MockClusterID,
					ClusterName: "fake",
					UserName:    MockRootUser,
				},
			},
			wantErr: true,
		},
		{
			name: "not found",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockRootUser,
				},
				req: UpdateFlavourRequest{
					Name: MockFlavourName + "fake",
					CPU:  "40",
					Mem:  "20G",
					ScalarResources: map[schema.ResourceName]string{
						"nvidia.com/gpu": "1",
					},
					ClusterID: MockClusterID,
					UserName:  MockRootUser,
				},
			},
			wantErr: true,
		},
		{
			name: "non-root update request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockNonRootUser,
				},
				req: UpdateFlavourRequest{
					Name: MockFlavourName,
					CPU:  "40",
					Mem:  "20G",
					ScalarResources: map[schema.ResourceName]string{
						"nvidia.com/gpu": "1",
					},
					ClusterID: MockClusterID,
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			response, err := UpdateFlavour(tt.args.ctx, &tt.args.req)
			t.Logf("case[%s] updateFlavour, response=%+v", tt.name, err)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				t.Logf("response: %v", response)
			}
		})
	}
}

func TestGetFlavour(t *testing.T) {
	TestCreateFlavour(t)
	flavour, err := storage.Flavour.GetFlavour(MockFlavourName)
	assert.Nil(t, err)
	assert.Equal(t, flavour.ClusterID, MockClusterID)
}

func TestDeleteFlavour(t *testing.T) {
	//MockFlavourName
	type args struct {
		ctx *logger.RequestContext
		req string
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "root delete request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockRootUser,
				},
				req: MockFlavourName,
			},
			wantErr: nil,
		},
		{
			name: "non-root delete request",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockNonRootUser,
				},
				req: MockFlavourName,
			},
			wantErr: errors.New("has no access to resource"),
		},
		{
			name: "record not found",
			args: args{
				ctx: &logger.RequestContext{
					UserName: MockRootUser,
				},
				req: MockFlavourName + "fake",
			},
			wantErr: errors.New("record not found"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			TestCreateFlavour(t)
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			err := DeleteFlavour(tt.args.ctx, tt.args.req)
			t.Logf("case[%s] delete flavour, response=%+v", tt.name, err)
			if tt.wantErr != nil {
				assert.Contains(t, err.Error(), tt.wantErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
