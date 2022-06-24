/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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

package pipeline

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const runtimePath = "./testcase/runtime.json"
const runYaml = "name: myproject\n\ndocker_env: iregistry.baidu-int.com/bmlc/framework/paddle:2.0.2-gpu-cuda10.1-cudnn7\n\nentry_points:\n  main:\n    parameters:\n      test: \"111\"\n    command: \"echo {{test}}\"\n    env:\n      PF_JOB_QUEUE_NAME: abc-q1\n      PF_JOB_TYPE: vcjob\n      PF_JOB_MODE: Pod\n      PF_JOB_FLAVOUR: flavour1\n      PF_JOB_PRIORITY: HIGH\n    cache:\n      enable: false\n      max_expired_time: 600\n      fs_scope: \"./lalal\"\n  nomain:\n    parameters:\n      test: \"222\"\n    command: \"echo {{test}}\"\n    env:\n      PF_JOB_QUEUE_NAME: abc-q1\n      PF_JOB_TYPE: vcjob\n      PF_JOB_MODE: Pod\n      PF_JOB_FLAVOUR: flavour1\n      PF_JOB_PRIORITY: HIGH\n    cache:\n      enable: false\n      max_expired_time: 600\n      fs_scope: \"./lalal\"\ncache:\n  enable: true\n  max_expired_time: 300\n  fs_scope: \"./for_fsscope\"\n"

func getMockRunWithRuntime() models.Run {
	run1 := models.Run{
		ID:       MockRunID1,
		Name:     "run_without_runtime",
		UserName: MockRootUser,
		FsID:     MockFsID1,
		Status:   common.StatusRunRunning,
		RunYaml:  runYaml,
	}
	return run1
}

func loadCase(casePath string) []byte {
	data, err := ioutil.ReadFile(casePath)
	if err != nil {
		fmt.Println("File reading error", err)
		return []byte{}
	}
	return data
}

func TestGetJobByRun(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}
	run := getMockRunWithRuntime()
	runID, err := models.CreateRun(ctx.Logging(), &run)
	assert.Nil(t, err)

	runtimeView := schema.RuntimeView{}
	runtimeJson := loadCase(runtimePath)
	json.Unmarshal([]byte(runtimeJson), &runtimeView)

	models.CreateRunJobs(ctx.Logging(), runtimeView, runID)
	updateRunJobs(runID, runtimeView)

	jobView, err := GetJobByRun(runID, "main")
	assert.Nil(t, err)
	assert.Equal(t, "job-run-000059-main-b7a9a264", jobView.JobID)
}
