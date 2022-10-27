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
	MockQueueID   = "mockQueueID"
	MockQueueName = "mockQueueName"
)

var (
	queue1 = model.Queue{
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
)

func initQueue(t *testing.T, userName string) {
	err := storage.Queue.CreateQueue(&queue1)
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

func TestUpdateJob(t *testing.T) {
	type args struct {
		ctx    *logger.RequestContext
		req    *job.UpdateJobRequest
		router *chi.Mux
	}

	router, baseURL := prepareDBAndAPIForUser(t, MockRootUser)
	initCluster(t)
	initQueue(t, MockRootUser)
	flavourName := initFlavour(t)

	//user1 := "abc"
	user2 := "def"
	MockJobID := "111"
	createJobRequest := job.CreateSingleJobRequest{
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
				Name: flavourName,
			},
		},
	}
	// change ctx user
	routerNonRoot := NewApiTestNonRoot()
	ctx := &logger.RequestContext{UserName: MockRootUser}
	// create user1
	token, err := CreateTestUser(ctx, mockUserName, MockPassword)
	assert.Nil(t, err)
	t.Logf("user:%s, token:%s", mockUserName, token)
	//setToken(token)
	// create user1
	tokenUser2, err := CreateTestUser(ctx, user2, MockPassword+"edf")
	t.Logf("user:%s, token:%s", user2, tokenUser2)
	assert.Nil(t, err)

	//ctx = &logger.RequestContext{UserName: mockUserName}
	res, err := PerformPostRequest(router, baseURL+"/job/single", createJobRequest)
	assert.NoError(t, err)
	t.Logf("create Job %v", res)

	//ctx := &logger.RequestContext{UserName: "testusername"}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "empty request",
			args: args{
				ctx:    &logger.RequestContext{UserName: user2},
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
				ctx: &logger.RequestContext{UserName: user2},
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
		t.Logf("Case[%s] baseURL=%s", tt.name, baseURL)
		//setToken(tt.args.router)
		res, _ = PerformPutRequest(tt.args.router, fmt.Sprintf("%s/job/%s?action=modify", baseURL, tt.args.req.JobID), tt.args.req)
		t.Logf("case[%s] update single job, response=%+v", tt.name, res)
		if tt.wantErr {
			assert.NotEqual(t, res.Code, 200)
			continue
		} else {
			assert.Equal(t, tt.responseCode, res.Code)
		}
	}
}

func TestDeleteJob(t *testing.T) {
	type args struct {
		ctx *logger.RequestContext
		req *job.UpdateJobRequest
	}

	router, baseURL := prepareDBAndAPIForUser(t, MockRootUser)
	initCluster(t)
	initQueue(t, MockRootUser)
	flavourName := initFlavour(t)

	//user1 := "abc"
	user2 := "def"
	MockJobID := "111"
	createJobRequest := job.CreateSingleJobRequest{
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
				Name: flavourName,
			},
		},
	}

	res, err := PerformPostRequest(router, baseURL+"/job/single", createJobRequest)
	assert.NoError(t, err)
	t.Logf("create Job %v", res)

	//ctx := &logger.RequestContext{UserName: "testusername"}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "empty request",
			args: args{
				ctx: &logger.RequestContext{UserName: user2},
				req: &job.UpdateJobRequest{},
			},
			wantErr:      false,
			responseCode: 404,
		},
		{
			name: "normal",
			args: args{
				ctx: &logger.RequestContext{UserName: user2},
				req: &job.UpdateJobRequest{
					JobID:    MockJobID,
					Priority: schema.EnvJobHighPriority,
				},
			},
			wantErr:      false,
			responseCode: 200,
		},
	}
	for _, tt := range tests {
		t.Logf("baseURL=%s", baseURL)

		res, err = PerformPutRequest(router, fmt.Sprintf("%s/job/%s?action=stop", baseURL, tt.args.req.JobID), tt.args.req)
		t.Logf("case[%s] stop single job, response=%+v", tt.name, res)
		assert.NoError(t, err)

		res, err = PerformDeleteRequest(router, fmt.Sprintf("%s/job/%s", baseURL, tt.args.req.JobID))
		t.Logf("case[%s] delete single job, response=%+v", tt.name, res)
		if tt.wantErr {
			assert.Error(t, err)
			continue
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tt.responseCode, res.Code)
		}
	}
}
