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
	"testing"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/job"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	MockQueueID      = "mockQueueID"
	MockQueueName    = "mockQueueName"
	MockNonRootUser2 = "abc"
	MockJobID        = "111"
)

var (
	MockQueue = model.Queue{
		Model: model.Model{
			ID: MockQueueID,
		},
		Name:        MockQueueName,
		Namespace:   "paddleflow",
		ClusterId:   MockClusterID,
		ClusterName: MockClusterName,
		QuotaType:   schema.TypeVolcanoCapabilityQuota,
		MaxResources: &resources.Resource{
			Resources: map[string]resources.Quantity{
				"cpu":            10 * 1000,
				"mem":            1000,
				"nvidia.com/gpu": 500,
			},
		},
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueOpen,
	}
	MockCreateJobRequest = job.CreateSingleJobRequest{
		CommonJobInfo: job.CommonJobInfo{
			ID:          MockJobID,
			Name:        "normal",
			Labels:      map[string]string{},
			Annotations: map[string]string{},
			SchedulingPolicy: job.SchedulingPolicy{
				Queue: MockQueueName,
			},
		},
		JobSpec: job.JobSpec{
			Image: "mockImage",
			Flavour: schema.Flavour{
				Name: MockFlavourName,
			},
		},
	}
)

func initQueue(t *testing.T, userName string) {
	err := storage.Queue.CreateQueue(&MockQueue)
	assert.Nil(t, err)
}

func TestCreateJob(t *testing.T) {
	type args struct {
		ctx *logger.RequestContext
		req *job.CreateSingleJobRequest
	}
	router, baseURL := prepareDBAndAPIForUser(t, MockRootUser)
	initCluster(t)
	initQueue(t, mockUserName)
	flavourName := initFlavour(t)
	ctx := &logger.RequestContext{UserName: "testusername"}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "empty request",
			args: args{
				ctx: ctx,
				req: &job.CreateSingleJobRequest{},
			},
			wantErr:      false,
			responseCode: 400,
		},
		{
			name: "normal",
			args: args{
				ctx: ctx,
				req: &job.CreateSingleJobRequest{
					CommonJobInfo: job.CommonJobInfo{
						Name:        "normal",
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						SchedulingPolicy: job.SchedulingPolicy{
							Queue: MockQueueName,
						},
					},
					JobSpec: job.JobSpec{
						Image: "mockImage",
						Flavour: schema.Flavour{
							Name: flavourName,
						},
					},
				},
			},
			wantErr:      false,
			responseCode: 200,
		},
	}
	for _, tt := range tests {
		t.Logf("baseURL=%s", baseURL)
		res, err := PerformPostRequest(router, baseURL+"/job/single", tt.args.req)
		t.Logf("case[%s] create single job, response=%+v", tt.name, res)
		if tt.wantErr {
			assert.Error(t, err)
			continue
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tt.responseCode, res.Code)
		}
	}
}

func MockCreateJobByRoot(t *testing.T) {
	initCluster(t)
	initQueue(t, MockRootUser)
	initFlavour(t)
}

func MockInitJob(t *testing.T) (*chi.Mux, *chi.Mux, string) {
	router, baseURL := prepareDBAndAPIForUser(t, MockRootUser)
	MockCreateJobByRoot(t)
	// change ctx user
	routerNonRoot := NewApiTestNonRoot()
	ctx := &logger.RequestContext{UserName: MockRootUser}
	// create user1
	token, err := CreateTestUser(ctx, mockUserName, MockPassword)
	assert.Nil(t, err)
	t.Logf("user:%s, token:%s", mockUserName, token)
	// create user1
	tokenMockNonRootUser2, err := CreateTestUser(ctx, MockNonRootUser2, MockPassword+"edf")
	t.Logf("user:%s, token:%s", MockNonRootUser2, tokenMockNonRootUser2)
	assert.Nil(t, err)

	return router, routerNonRoot, baseURL
}

func TestUpdateJob(t *testing.T) {
	router, routerNonRoot, baseURL := MockInitJob(t)
	type args struct {
		ctx    *logger.RequestContext
		req    *job.UpdateJobRequest
		router *chi.Mux
	}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "empty request",
			args: args{
				ctx:    &logger.RequestContext{UserName: MockNonRootUser2},
				req:    &job.UpdateJobRequest{},
				router: router,
			},
			wantErr:      false,
			responseCode: 404,
		},
		{
			name: "normal",
			args: args{
				ctx: &logger.RequestContext{UserName: mockUserName},
				req: &job.UpdateJobRequest{
					JobID:    MockJobID,
					Priority: schema.EnvJobHighPriority,
				},
				router: router,
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "no permit",
			args: args{
				ctx: &logger.RequestContext{UserName: MockNonRootUser2},
				req: &job.UpdateJobRequest{
					JobID:    MockJobID,
					Priority: schema.EnvJobHighPriority,
				},
				router: routerNonRoot,
			},
			wantErr:      true,
			responseCode: 403,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := PerformPostRequest(tt.args.router, baseURL+"/job/single", MockCreateJobRequest)
			assert.NoError(t, err)
			t.Logf("create Job %v", res)

			t.Logf("Case[%s] baseURL=%s", tt.name, baseURL)
			res, _ = PerformPutRequest(tt.args.router, fmt.Sprintf("%s/job/%s?action=modify", baseURL, tt.args.req.JobID), tt.args.req)
			t.Logf("case[%s] update single job, response=%+v", tt.name, res)
			if tt.wantErr {
				assert.NotEqual(t, res.Code, 200)
			} else {
				assert.Equal(t, tt.responseCode, res.Code)
			}
		})
	}
}

func TestStopJob(t *testing.T) {
	router, routerNonRoot, baseURL := MockInitJob(t)
	type args struct {
		ctx    *logger.RequestContext
		req    string
		router *chi.Mux
	}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "empty request",
			args: args{
				ctx:    &logger.RequestContext{UserName: MockNonRootUser2},
				req:    "",
				router: router,
			},
			wantErr:      false,
			responseCode: 404,
		},
		{
			name: "no permit",
			args: args{
				ctx:    &logger.RequestContext{UserName: MockNonRootUser2},
				req:    MockJobID,
				router: routerNonRoot,
			},
			wantErr:      true,
			responseCode: 403,
		},
		{
			name: "normal",
			args: args{
				ctx:    &logger.RequestContext{UserName: MockNonRootUser2},
				req:    MockJobID,
				router: router,
			},
			wantErr:      false,
			responseCode: 200,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("baseURL=%s", baseURL)
			res, err := PerformPostRequest(tt.args.router, baseURL+"/job/single", MockCreateJobRequest)
			assert.NoError(t, err)
			t.Logf("create Job %v", res)

			res, err = PerformPutRequest(tt.args.router, fmt.Sprintf("%s/job/%s?action=stop", baseURL, tt.args.req), tt.args.req)
			t.Logf("case[%s] stop job, response=%+v", tt.name, res)

			if tt.wantErr {
				assert.Contains(t, res.Body.String(), "has no access to")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.responseCode, res.Code)
			}
		})
	}
}

func TestDeleteJob(t *testing.T) {
	router, routerNonRoot, baseURL := MockInitJob(t)
	type args struct {
		ctx    *logger.RequestContext
		req    string
		router *chi.Mux
	}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "empty request",
			args: args{
				ctx:    &logger.RequestContext{UserName: MockNonRootUser2},
				req:    "",
				router: router,
			},
			wantErr:      false,
			responseCode: 404,
		},
		{
			name: "no permit",
			args: args{
				ctx:    &logger.RequestContext{UserName: MockNonRootUser2},
				req:    MockJobID,
				router: routerNonRoot,
			},
			wantErr:      true,
			responseCode: 403,
		},
		{
			name: "normal",
			args: args{
				ctx:    &logger.RequestContext{UserName: MockNonRootUser2},
				req:    MockJobID,
				router: router,
			},
			wantErr:      false,
			responseCode: 200,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("baseURL=%s", baseURL)
			res, err := PerformPostRequest(tt.args.router, baseURL+"/job/single", MockCreateJobRequest)
			res, err = PerformPutRequest(router, fmt.Sprintf("%s/job/%s?action=stop", baseURL, tt.args.req), tt.args.req)
			t.Logf("case[%s] stop job, response=%+v", tt.name, res)
			assert.NoError(t, err)

			res, err = PerformDeleteRequest(tt.args.router, fmt.Sprintf("%s/job/%s", baseURL, tt.args.req))
			t.Logf("case[%s] delete job, response=%+v", tt.name, res)
			if tt.wantErr {
				assert.Contains(t, res.Body.String(), "has no access to")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.responseCode, res.Code)
			}
		})
	}
}
