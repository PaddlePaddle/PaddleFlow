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

package v1

import (
	"fmt"
	storage_db2 "github.com/PaddlePaddle/PaddleFlow/pkg/service/db_service"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/flavour"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const (
	MockClusterName = "testCn"
	MockClusterID   = "testClusterID"
	MockNamespace   = "paddle"
)

var (
	apiURL          = "/api/paddleflow/v1/flavour"
	mockFlavourName = "mockFlavourName"
	clusterInfo     = storage_db2.ClusterInfo{
		Model:         storage_db2.Model{ID: MockClusterID},
		Name:          MockClusterName,
		Description:   "Description",
		Endpoint:      "Endpoint",
		Source:        "Source",
		ClusterType:   schema.KubernetesType,
		Version:       "1.16",
		Status:        storage_db2.ClusterStatusOnLine,
		Credential:    "credential",
		Setting:       "Setting",
		NamespaceList: []string{"n1", "n2", MockNamespace},
	}
	mockFlavour = storage_db2.Flavour{
		Name: mockFlavourName,
		CPU:  "1",
		Mem:  "1",
	}
)

func initCluster(t *testing.T) {
	err := storage_db2.CreateCluster(&clusterInfo)
	assert.Nil(t, err)
}

func initFlavour(t *testing.T) string {
	err := storage_db2.CreateFlavour(&mockFlavour)
	assert.Nil(t, err)
	return mockFlavour.Name
}

func TestListFlavour(t *testing.T) {
	router, baseURL := prepareDBAndAPIForUser(t, "")

	initCluster(t)

	res, err := PerformGetRequest(router, baseURL+"/flavour")
	var flavours flavour.ListFlavourResponse
	err = ParseBody(res.Body, &flavours)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(flavours.FlavourList))
	assert.Equal(t, 200, res.Code)

	num := 10
	for i := 0; i < num; i++ {
		f := flavour.CreateFlavourRequest{
			Name: fmt.Sprintf("%s-%d", mockFlavourName, i),
			CPU:  "1",
			Mem:  "1",
		}
		if i%2 == 0 {
			f.ClusterName = MockClusterName
		}
		res, err := PerformPostRequest(router, baseURL+"/flavour", f)
		assert.NoError(t, err)
		t.Logf("create flavour, response=%+v", res)
		assert.Equal(t, 200, res.Code)
	}
	// base query
	res, err = PerformGetRequest(router, baseURL+"/flavour")
	t.Logf("list flavour, res=%+v", res)
	err = ParseBody(res.Body, &flavours)
	assert.Nil(t, err)
	assert.Equal(t, num/2, len(flavours.FlavourList))
	assert.Equal(t, 200, res.Code)

	// query with clusterID
	res, err = PerformGetRequest(router, baseURL+"/flavour?clusterName="+MockClusterName)
	t.Logf("list flavour, res=%+v", res)
	err = ParseBody(res.Body, &flavours)
	assert.Nil(t, err)
	assert.Equal(t, num, len(flavours.FlavourList))
	assert.Equal(t, 200, res.Code)

}

func TestCreateFlavour(t *testing.T) {
	router, baseURL := prepareDBAndAPIForUser(t, "")

	f := flavour.CreateFlavourRequest{
		Name: mockFlavourName,
		CPU:  "1",
		Mem:  "1",
	}
	t.Logf("baseURL + apiURL=%s", baseURL+apiURL)
	res, err := PerformPostRequest(router, baseURL+"/flavour", f)
	assert.NoError(t, err)
	t.Logf("create flavour, response=%+v", res)
	assert.Equal(t, 200, res.Code)
}

func TestUpdateFlavour(t *testing.T) {
	router, baseURL := prepareDBAndAPIForUser(t, "")

	// create flavour
	TestCreateFlavour(t)
	f := flavour.UpdateFlavourRequest{
		Name: mockFlavourName,
		CPU:  "2",
	}
	// update
	res, err := PerformPutRequest(router, baseURL+"/flavour/"+mockFlavourName, f)
	assert.NoError(t, err)
	assert.Equal(t, 200, res.Code)

	var response flavour.UpdateFlavourResponse
	err = ParseBody(res.Body, &response)
	assert.Nil(t, err)
	assert.Equal(t, f.CPU, response.CPU)
	t.Logf("%v", response)
}

func TestGetFlavour(t *testing.T) {
	router, baseURL := prepareDBAndAPIForUser(t, "")
	res, err := PerformGetRequest(router, baseURL+"/flavour/"+mockFlavourName)
	assert.NoError(t, err)
	assert.Equal(t, 404, res.Code)

	t.Logf("%+v", res)
	// create flavour
	TestCreateFlavour(t)

	res, err = PerformGetRequest(router, baseURL+"/flavour/"+mockFlavourName)
	assert.NoError(t, err)
	assert.Equal(t, 200, res.Code)
	var response storage_db2.Flavour
	err = ParseBody(res.Body, &response)
	assert.Nil(t, err)
	t.Logf("get response %+v", response)
}

func TestDeleteFlavour(t *testing.T) {
	router, baseURL := prepareDBAndAPIForUser(t, "")

	res, err := PerformDeleteRequest(router, baseURL+"/flavour/"+mockFlavourName)
	assert.NoError(t, err)
	assert.Equal(t, 404, res.Code)

	// create flavour
	TestCreateFlavour(t)

	// delete again
	res2, err := PerformDeleteRequest(router, baseURL+"/flavour/"+mockFlavourName)
	assert.NoError(t, err)
	t.Logf("%v", res2)

	assert.Equal(t, 200, res2.Code)
}
