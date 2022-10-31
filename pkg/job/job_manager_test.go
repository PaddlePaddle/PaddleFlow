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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestJobManager(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{}

	clusterInfo := &model.ClusterInfo{
		Name:   "test-cluster",
		Status: model.ClusterStatusOnLine,
	}
	jobM, err := NewJobManagerImpl()
	assert.Equal(t, nil, err)
	testCases := []struct {
		name       string
		jobManager *JobManagerImpl
		err        error
	}{
		{
			name:       "start job manager v2",
			jobManager: jobM,
			err:        nil,
		},
	}

	driver.InitMockDB()
	err = storage.Cluster.CreateCluster(clusterInfo)
	assert.Equal(t, nil, err)

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			go testCase.jobManager.Start(storage.Cluster.ActiveClusters, storage.Job.ListQueueJob)
			time.Sleep(2 * time.Second)
		})
	}
}
