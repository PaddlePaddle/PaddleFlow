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

package v1

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/resourcepool"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

var (
	MockRPName = "test-rp-name"
	MockUserID = "test-user-id"
	rp1        = model.ResourcePool{
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
)

func TestResourcePool_create(t *testing.T) {
	chiRouter, baseURL := prepareDBAndAPI(t)
	testCases := []struct {
		name         string
		req          interface{}
		router       *chi.Mux
		responseCode int
		responseBody string
	}{
		{
			name:   "create resource pool successfully",
			router: chiRouter,
			req: &resourcepool.CreateRequest{
				Name:        MockRPName,
				Namespace:   "default",
				Type:        "train",
				ClusterName: MockClusterName,
				TotalResources: schema.ResourceInfo{
					CPU: "20",
					Mem: "500Gi",
					ScalarResources: schema.ScalarResourcesType{
						"nvidia.com/gpu": "100",
					},
				},
			},
			responseCode: 200,
			responseBody: `{"name":"test-rp-name"}`,
		},
		{
			name:         "create request json decode failed",
			router:       chiRouter,
			req:          `{"name","xx",,}`,
			responseCode: 400,
			responseBody: `{"requestID":"%s","code":"MalformedJSON","message":"The JSON provided was not well-formatted"}`,
		},
		{
			name:   "create resource pool failed",
			router: chiRouter,
			req: &resourcepool.CreateRequest{
				Name:        MockRPName,
				Namespace:   "default",
				Type:        "train",
				ClusterName: "xx",
				TotalResources: schema.ResourceInfo{
					CPU: "20",
					Mem: "500Gi",
					ScalarResources: schema.ScalarResourcesType{
						"nvidia.com/gpu": "100",
					},
				},
			},
			responseCode: 400,
			responseBody: `{"requestID":"%s","code":"ClusterNotFound","message":"cluster not found by Name"}`,
		},
	}

	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := PerformPostRequest(tc.router, baseURL+"/resourcepool", tc.req)
			assert.Equal(t, tc.responseCode, res.Code)
			if tc.responseCode != 200 {
				tc.responseBody = fmt.Sprintf(tc.responseBody, res.Result().Header.Get("X-Pf-Request-Id"))
				t.Logf("create failed, response: %s", res.Body.String())
			}
			assert.Equal(t, tc.responseBody, res.Body.String())
			assert.NoError(t, err)
			t.Logf("create resource pool %v", res.Result())
		})
	}
}

func TestResourcePool_update(t *testing.T) {
	chiRouter, baseURL := prepareDBAndAPI(t)
	testCases := []struct {
		name         string
		rpName       string
		req          interface{}
		router       *chi.Mux
		responseCode int
		responseBody string
	}{
		{
			name:   "update resource pool successfully",
			router: chiRouter,
			rpName: MockRPName,
			req: &resourcepool.UpdateRequest{
				Name:      MockRPName,
				Namespace: "default",
				Status:    schema.StatusQueueClosed,
				TotalResources: schema.ResourceInfo{
					CPU: "20",
					Mem: "500Gi",
					ScalarResources: schema.ScalarResourcesType{
						"nvidia.com/gpu": "100",
					},
				},
			},
			responseCode: 200,
		},
		{
			name:         "update request json decode failed",
			router:       chiRouter,
			rpName:       MockRPName,
			req:          `{"status","xx",,}`,
			responseCode: 400,
			responseBody: `{"requestID":"%s","code":"MalformedJSON","message":"The JSON provided was not well-formatted"}`,
		},
		{
			name:   "update resource pool with empty name ",
			router: chiRouter,
			rpName: "rp_not",
			req: &resourcepool.UpdateRequest{
				Name:      MockRPName,
				Namespace: "default",
				Status:    schema.StatusQueueClosed,
				TotalResources: schema.ResourceInfo{
					CPU: "20",
					Mem: "500Gi",
					ScalarResources: schema.ScalarResourcesType{
						"nvidia.com/gpu": "100",
					},
				},
			},
			responseCode: 404,
			responseBody: `{"requestID":"%s","code":"RecordNotFound","message":"record not found"}`,
		},
	}

	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))
	assert.Nil(t, storage.ResourcePool.Create(&rp1))
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := PerformPutRequest(tc.router, baseURL+"/resourcepool/"+tc.rpName, tc.req)

			assert.Equal(t, tc.responseCode, res.Code)
			if tc.responseCode != 200 {
				tc.responseBody = fmt.Sprintf(tc.responseBody, res.Result().Header.Get("X-Pf-Request-Id"))
				t.Logf("update failed, response: %s", res.Body.String())
				assert.Equal(t, tc.responseBody, res.Body.String())
			} else {
				req := tc.req.(*resourcepool.UpdateRequest)
				rp := &resourcepool.RPResponse{}
				err = json.Unmarshal([]byte(res.Body.String()), &rp)
				assert.Equal(t, nil, err)
				t.Logf(`update resource pool %v`, rp)
				if req.Status != "" {
					assert.Equal(t, req.Status, rp.Status)
				}
			}
		})
	}
}

func TestResourcePool_list(t *testing.T) {
	chiRouter, baseURL := prepareDBAndAPI(t)
	testCases := []struct {
		name         string
		req          *resourcepool.ListRequest
		router       *chi.Mux
		responseCode int
		responseBody string
	}{
		{
			name:   "list resource pools successfully",
			router: chiRouter,
			req: &resourcepool.ListRequest{
				Marker:  "",
				MaxKeys: 1,
			},
			responseCode: 200,
		},
		{
			name:   "list resource pools with wrong maxKeys",
			router: chiRouter,
			req: &resourcepool.ListRequest{
				Marker:  "",
				MaxKeys: -1,
			},
			responseCode: 400,
			responseBody: `{"requestID":"%s","code":"InvalidURI","message":"maxKeys[-1] is invalid"}`,
		},
	}

	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))
	assert.Nil(t, storage.ResourcePool.Create(&rp1))
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			queryStr := ""
			if tc.req.Marker != "" {
				queryStr = "marker=" + tc.req.Marker
			}
			if tc.req.MaxKeys != 0 {
				queryStr += "&maxKeys=" + strconv.Itoa(tc.req.MaxKeys)
			}
			res, err := PerformGetRequest(tc.router, baseURL+"/resourcepool?"+queryStr)
			assert.Equal(t, tc.responseCode, res.Code)
			assert.NoError(t, err)
			t.Logf("list request: %v", res.Body.String())
		})
	}
}

func TestResourcePool_get(t *testing.T) {
	chiRouter, baseURL := prepareDBAndAPI(t)
	testCases := []struct {
		name         string
		rpName       string
		req          *resourcepool.ListRequest
		router       *chi.Mux
		responseCode int
		responseBody string
	}{
		{
			name:         "get resource pools successfully",
			router:       chiRouter,
			rpName:       rp1.Name,
			responseCode: 200,
		},
		{
			name:         "get resource pools failed",
			router:       chiRouter,
			rpName:       "xxx",
			responseCode: 404,
			responseBody: `{"requestID":"%s","code":"ResourceNotFound","message":"The resource not found"}`,
		},
	}

	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))
	assert.Nil(t, storage.ResourcePool.Create(&rp1))
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := PerformGetRequest(tc.router, baseURL+"/resourcepool/"+tc.rpName)
			assert.Equal(t, tc.responseCode, res.Code)
			assert.NoError(t, err)
			if tc.responseCode != 200 {
				tc.responseBody = fmt.Sprintf(tc.responseBody, res.Result().Header.Get("X-Pf-Request-Id"))
				assert.Equal(t, tc.responseBody, res.Body.String())
			}
			t.Logf("get request: %v", res.Body.String())
		})
	}
}

func TestResourcePool_delete(t *testing.T) {
	chiRouter, baseURL := prepareDBAndAPI(t)
	testCases := []struct {
		name         string
		rpName       string
		router       *chi.Mux
		responseCode int
		responseBody string
	}{
		{
			name:         "delete resource pools failed",
			router:       chiRouter,
			rpName:       "xxx",
			responseCode: 404,
			responseBody: `{"requestID":"%s","code":"ResourceNotFound","message":"The resource not found"}`,
		},
		{
			name:         "delete resource pools successfully",
			router:       chiRouter,
			rpName:       rp1.Name,
			responseCode: 200,
		},
	}

	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))
	assert.Nil(t, storage.ResourcePool.Create(&rp1))
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := PerformDeleteRequest(tc.router, baseURL+"/resourcepool/"+tc.rpName)
			assert.Equal(t, tc.responseCode, res.Code)
			assert.NoError(t, err)
			if tc.responseCode != 200 {
				tc.responseBody = fmt.Sprintf(tc.responseBody, res.Result().Header.Get("X-Pf-Request-Id"))
				assert.Equal(t, tc.responseBody, res.Body.String())
			}
			t.Logf("delete resource pool: %v", res.Body.String())
		})
	}
}
