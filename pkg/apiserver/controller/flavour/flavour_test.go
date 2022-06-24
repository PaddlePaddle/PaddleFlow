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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	MockRootUser    = "root"
	MockClusterName = "testCn"
	MockClusterID   = "testClusterID"
	MockNamespace   = "paddle"
	MockFlavourName = "flavour1"
)

var clusterInfo = models.ClusterInfo{
	Model:         models.Model{ID: MockClusterID},
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
	err := models.CreateCluster(&clusterInfo)
	assert.Nil(t, err)
}

func TestListFlavour(t *testing.T) {
	driver.InitMockDB()

	initCluster(t)
	// insert flavour
	num := 20
	clusterNum := 5
	for i := 0; i < num; i++ {
		flavour := models.Flavour{
			Name: fmt.Sprintf("flavour%d", i),
			CPU:  "20",
			Mem:  "20G",
			ScalarResources: map[schema.ResourceName]string{
				"com/gpu": "1",
			},
		}
		if i < clusterNum {
			flavour.ClusterID = MockClusterID
		}

		err := models.CreateFlavour(&flavour)
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
	}
	resp, err := CreateFlavour(&createFlavourReq)

	assert.NoError(t, err)
	assert.Equal(t, createFlavourReq.Name, resp.FlavourName)
	t.Logf("resp=%v", resp)
}

func TestUpdateFlavour(t *testing.T) {
	TestCreateFlavour(t)
	// get
	flavour, err := models.GetFlavour(MockFlavourName)
	assert.Nil(t, err)
	assert.NotEmpty(t, flavour.ClusterName)
	// update
	newCPU := "40"
	flavour.CPU = newCPU
	err = models.UpdateFlavour(&flavour)
	assert.Nil(t, err)
	// query again
	newFlavour, err := models.GetFlavour(MockFlavourName)
	assert.Nil(t, err)
	assert.Equal(t, newFlavour.CPU, newCPU)
}

func TestGetFlavour(t *testing.T) {
	TestCreateFlavour(t)
	flavour, err := models.GetFlavour(MockFlavourName)
	assert.Nil(t, err)
	assert.NotEmpty(t, flavour.ClusterName)
}

func TestDeleteFlavour(t *testing.T) {
	TestCreateFlavour(t)
	err := models.DeleteFlavour(MockFlavourName)
	assert.Nil(t, err)
}
