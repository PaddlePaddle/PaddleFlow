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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

var taskStatus = `{"phase":"Succeeded","conditions":[{"type":"Initialized","status":"True","lastProbeTime":null,
"lastTransitionTime":"2023-03-02T09:43:55Z","reason":"PodCompleted"},
{"type":"Ready","status":"False","lastProbeTime":null,"lastTransitionTime":"2023-03-02T10:43:57Z",
"reason":"PodCompleted"},{"type":"ContainersReady","status":"False","lastProbeTime":null,
"lastTransitionTime":"2023-03-02T10:43:57Z","reason":"PodCompleted"},
{"type":"PodScheduled","status":"True","lastProbeTime":null,"lastTransitionTime":"2023-03-02T09:43:55Z"}],
"hostIP":"127.0.0.1","podIP":"10.233.64.222","podIPs":[{"ip":"10.233.64.222"}],"startTime":"2023-03-02T09:43:55Z",
"containerStatuses":[{"name":"job-20220101xyz","state":{"terminated":{"exitCode":0,"reason":"Completed",
"startedAt":"2023-03-02T09:43:57Z","finishedAt":"2023-03-02T10:43:57Z",
"containerID":"docker://8517d2e225a5e580470d56c7e039208b538cb78b942cdabb028e235d1aee54b6"}},
"lastState":{},"ready":false,"restartCount":0,"image":"nginx:latest",
"imageID":"docker-pullable://nginx@sha256:1708fdec7d93bc9869d269fc20148b84110ecb75a2f4f7ad6bbb590cacbc729f",
"containerID":"docker://8517d2e225a5e580470d56c7e039208b538cb78b942cdabb028e235d1aee54b6","started":false}],
"qosClass":"Guaranteed"}`

func TestGenerateLogURL(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{
			Log: config.JobLogConfig{
				ServiceHost: "127.0.0.1",
				ServicePort: "8080",
			},
		},
	}

	testCases := []struct {
		name        string
		task        model.JobTask
		containerID string
		expectURL   string
	}{
		{
			name: "generate log url success",
			task: model.JobTask{
				ID:                   "test-task-id",
				JobID:                "test-job-id",
				ExtRuntimeStatusJSON: taskStatus,
			},
			containerID: "8517d2e225a5e580470d56c7e039208b538cb78b942cdabb028e235d1aee54b6",
			expectURL:   "http://127.0.0.1:8080/v1/containers/%s/log?jobID=test-job-id&token=%s",
		},
	}

	driver.InitMockDB()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// init db
			err := storage.Job.UpdateTask(&tc.task)
			assert.Equal(t, nil, err)
			task, err := storage.Job.GetJobTaskByID(tc.task.ID)
			assert.Equal(t, nil, err)
			// generate log url
			url := GenerateLogURL(task)
			tokenStr := getLogToken(task.JobID, tc.containerID)
			token := md5.Sum([]byte(tokenStr))
			expectURL := fmt.Sprintf(tc.expectURL, tc.containerID, hex.EncodeToString(token[:]))
			assert.Equal(t, expectURL, url)
		})
	}
}
