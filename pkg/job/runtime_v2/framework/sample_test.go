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

package framework

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
)

func TestJobSample(t *testing.T) {
	jobSample := JobSample{}

	var err error
	err = jobSample.Submit(context.TODO(), &api.PFJob{})
	assert.NotEmpty(t, err)

	err = jobSample.Update(context.TODO(), &api.PFJob{})
	assert.NotEmpty(t, err)

	err = jobSample.Stop(context.TODO(), &api.PFJob{})
	assert.NotEmpty(t, err)

	err = jobSample.Delete(context.TODO(), &api.PFJob{})
	assert.NotEmpty(t, err)

	_, err = jobSample.GetLog(context.TODO(), schema.JobLogRequest{})
	assert.NotEmpty(t, err)

	err = jobSample.AddEventListener(context.TODO(), schema.ListenerTypeJob, nil, nil)
	assert.NotEmpty(t, err)
}

func TestQueueSample(t *testing.T) {
	queueSample := QueueSample{}

	var err error
	err = queueSample.Create(context.TODO(), &api.QueueInfo{})
	assert.NotEmpty(t, err)

	err = queueSample.Update(context.TODO(), &api.QueueInfo{})
	assert.NotEmpty(t, err)

	err = queueSample.Delete(context.TODO(), &api.QueueInfo{})
	assert.NotEmpty(t, err)

	err = queueSample.AddEventListener(context.TODO(), schema.ListenerTypeQueue, nil, nil)
	assert.NotEmpty(t, err)
}
