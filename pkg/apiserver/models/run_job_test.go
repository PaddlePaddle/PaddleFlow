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

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func TestFinished(t *testing.T) {
	rj := RunJob{
		Status: schema.StatusJobCancelled,
	}

	assert.True(t, rj.Finished())

	rj.Status = ScheduleStatusRunning
	assert.False(t, rj.Finished())
}

func TestTransJob(t *testing.T) {
	rj := RunJob{
		Status: schema.StatusJobCancelled,
	}

	jobView := rj.Trans2JobView()
	assert.Equal(t, jobView.EndTime, "")

	rj = RunJob{
		Status:     schema.StatusJobTerminated,
		UpdateTime: "2022-01-01 00:01:11",
	}

	jobView = rj.Trans2JobView()
	assert.Equal(t, jobView.EndTime, "2022-01-01 00:01:11")

	rj = RunJob{
		Status:     schema.StatusJobCancelled,
		UpdateTime: "2022-01-01 00:01:11",
	}

	jobView = rj.Trans2JobView()
	assert.Equal(t, jobView.EndTime, "")

	rj = RunJob{
		Status:     schema.StatusJobRunning,
		UpdateTime: "2022-01-01 00:01:11",
	}

	jobView = rj.Trans2JobView()
	assert.Equal(t, jobView.EndTime, "")
}
