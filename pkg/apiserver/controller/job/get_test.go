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
)

func TestGenerateLogURL(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{
			Log: config.JobLogConfig{
				ServiceHost: "127.0.0.1",
				ServicePort: 8080,
			},
		},
	}
	testCases := []struct {
		name      string
		task      model.JobTask
		expectURL string
	}{
		{
			name: "generate log url success",
			task: model.JobTask{
				ID:    "test-task-id",
				JobID: "test-job-id",
			},
			expectURL: "http://127.0.0.1:8080/filetree?action=ls&jobID=test-job-id&containerID=test-task-id",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			url := GenerateLogURL(tc.task)
			tokenStr := getLogToken(tc.task.JobID, tc.task.ID)
			token := md5.Sum([]byte(tokenStr))
			expectURL := fmt.Sprintf("%s&token=%s", tc.expectURL, hex.EncodeToString(token[:]))
			assert.Equal(t, expectURL, url)
		})
	}
}
