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
	"encoding/json"
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

func TestEncodeJob(t *testing.T) {
	rj := RunJob{
		Status:     schema.StatusJobRunning,
		UpdateTime: "2022-01-01 00:01:11",
	}
	rj.DistributedJobJson = ""
	rj.DistributedJob = schema.DistributedJob{
		Framework: "paddle",
		Members: []schema.Member{
			{
				Replicas: 2,
				Role:     "pworker",
				Conf: schema.Conf{
					QueueName: "train-queue",
					Command:   "echo worker",
					Image:     "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
					Env:       map[string]string{"Worker": "1"},
					Flavour:   schema.Flavour{Name: "flavour1"},
					Priority:  "high",
				},
			},
			{
				Replicas: 2,
				Role:     "pserver",
				Conf: schema.Conf{
					QueueName: "train-queue",
					Command:   "echo server",
					Image:     "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
					Env:       map[string]string{"PS": "1"},
					Flavour:   schema.Flavour{Name: "flavour1"},
					Priority:  "high",
				},
			},
		},
	}
	rj.Encode()

	distributedJob := schema.DistributedJob{}
	err := json.Unmarshal([]byte(rj.DistributedJobJson), &distributedJob)
	assert.Nil(t, err)
	assert.Equal(t, string(distributedJob.Framework), "paddle")
	assert.Equal(t, len(distributedJob.Members), 2)
}

func TestDecode(t *testing.T) {
	rj := RunJob{
		Status:     schema.StatusJobRunning,
		UpdateTime: "2022-01-01 00:01:11",
	}

	distJob := schema.DistributedJob{
		Framework: "paddle",
		Members: []schema.Member{
			{
				Replicas: 2,
				Role:     "pworker",
				Conf: schema.Conf{
					QueueName: "train-queue",
					Command:   "echo worker",
					Image:     "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
					Env:       map[string]string{"Worker": "1"},
					Flavour:   schema.Flavour{Name: "flavour1"},
					Priority:  "high",
				},
			},
			{
				Replicas: 2,
				Role:     "pserver",
				Conf: schema.Conf{
					QueueName: "train-queue",
					Command:   "echo server",
					Image:     "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
					Env:       map[string]string{"PS": "1"},
					Flavour:   schema.Flavour{Name: "flavour1"},
					Priority:  "high",
				},
			},
		},
	}
	distJson, err := json.Marshal(distJob)
	assert.Nil(t, err)
	rj.DistributedJobJson = string(distJson)
	rj.decode()

	distributedJob := schema.DistributedJob{
		Framework: "paddle",
		Members: []schema.Member{
			{
				Replicas: 2,
				Role:     "pworker",
				Conf: schema.Conf{
					QueueName: "train-queue",
					Command:   "echo worker",
					Image:     "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
					Env:       map[string]string{"Worker": "1"},
					Flavour:   schema.Flavour{Name: "flavour1"},
					Priority:  "high",
				},
			},
			{
				Replicas: 2,
				Role:     "pserver",
				Conf: schema.Conf{
					QueueName: "train-queue",
					Command:   "echo server",
					Image:     "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
					Env:       map[string]string{"PS": "1"},
					Flavour:   schema.Flavour{Name: "flavour1"},
					Priority:  "high",
				},
			},
		},
	}

	assert.Equal(t, rj.DistributedJob.Framework, distributedJob.Framework)
	assert.Equal(t, rj.DistributedJob.Members, distributedJob.Members)
}
