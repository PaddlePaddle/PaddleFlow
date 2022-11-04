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

package job

import (
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	mockQueueID = "queue-test"
)

func TestJobManager(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{}

	mockCluster := &model.ClusterInfo{
		Name:   "test-cluster",
		Status: model.ClusterStatusOnLine,
	}
	mockQueue := &model.Queue{
		Model: model.Model{
			ID: mockQueueID,
		},
		Status: schema.StatusQueueOpen,
	}
	jobM, err := NewJobManagerImpl()
	assert.Equal(t, nil, err)
	testCases := []struct {
		name       string
		jobManager *JobManagerImpl
		job        *model.Job
		err        error
	}{
		{
			name:       "start job manager v2",
			jobManager: jobM,
			job: &model.Job{
				Name:    "test-job",
				Status:  schema.StatusJobInit,
				QueueID: mockQueueID,
			},
			err: nil,
		},
	}

	driver.InitMockDB()
	err = storage.Cluster.CreateCluster(mockCluster)
	assert.Equal(t, nil, err)
	err = storage.Queue.CreateQueue(mockQueue)
	assert.Equal(t, nil, err)
	// mock cluster
	rts := &runtime.KubeRuntime{}
	var p1 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "Init", func() error {
		return nil
	})
	defer p1.Reset()
	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "SyncController", func(<-chan struct{}) error {
		return nil
	})
	defer p2.Reset()
	var p3 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "SubmitJob", func(*api.PFJob) error {
		return nil
	})
	defer p3.Reset()

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err = storage.Job.CreateJob(testCase.job)
			assert.Equal(t, nil, err)
			go testCase.jobManager.Start(storage.Cluster.ActiveClusters, storage.Job.ListQueueJob)
			time.Sleep(2 * time.Second)
		})
	}
}
