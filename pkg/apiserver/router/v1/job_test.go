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
	storage_db2 "github.com/PaddlePaddle/PaddleFlow/pkg/service/db_service"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/job"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const (
	MockQueueID   = "mockQueueID"
	MockQueueName = "mockQueueName"
)

var (
	queue1 = storage_db2.Queue{
		Model: storage_db2.Model{
			ID: MockQueueID,
		},
		Name:      MockQueueName,
		Namespace: "paddleflow",
		ClusterId: MockClusterID,
		QuotaType: schema.TypeVolcanoCapabilityQuota,
		MaxResources: schema.ResourceInfo{
			CPU: "10",
			Mem: "1000",
			ScalarResources: schema.ScalarResourcesType{
				"nvidia.com/gpu": "500",
			},
		},
		SchedulingPolicy: []string{"s1", "s2"},
		Status:           schema.StatusQueueOpen,
	}
)

type args struct {
	ctx *logger.RequestContext
	req *job.CreateSingleJobRequest
}

func initQueue(t *testing.T, userName string) {
	err := storage_db2.CreateQueue(&queue1)
	assert.Nil(t, err)
}

func TestCreateJob(t *testing.T) {
	router, baseURL := prepareDBAndAPIForUser(t, MockRootUser)
	initCluster(t)
	initQueue(t, mockUserName)

	flavourName := initFlavour(t)
	config.GlobalServerConfig.FlavourMap = map[string]schema.Flavour{
		flavourName: {
			Name: flavourName,
			ResourceInfo: schema.ResourceInfo{
				CPU: "1",
				Mem: "100M",
			},
		},
		"cpu": {
			Name: "cpu",
			ResourceInfo: schema.ResourceInfo{
				CPU: "1",
				Mem: "100M",
			},
		},
		"gpu": {
			Name: "gpu",
			ResourceInfo: schema.ResourceInfo{
				CPU: "1",
				Mem: "100M",
				ScalarResources: schema.ScalarResourcesType{
					"nvidia.com/gpu": "500M",
				},
			},
		},
	}
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
