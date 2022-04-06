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

package run

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/database/db_fake"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
)

const runtimeRaw = `
{
	"main": {
		"jobID": "job-run-000059-main-b7a9a264",
		"name": "run-000059-main",
		"command": "echo hahaha",
		"parameters": {
			"test": "hahaha"
		},
		"env": {
			"PF_FS_ID": "fs-root-cyang14",
			"PF_FS_NAME": "cyang14",
			"PF_JOB_CLUSTER_ID": "cluster-05f3854d",
			"PF_JOB_FLAVOUR": "flavour1",
			"PF_JOB_MODE": "Pod",
			"PF_JOB_NAMESPACE": "default",
			"PF_JOB_PRIORITY": "HIGH",
			"PF_JOB_QUEUE_ID": "queue-48ec41bd",
			"PF_JOB_QUEUE_NAME": "abc-q1",
			"PF_JOB_TYPE": "vcjob",
			"PF_RUN_ID": "run-000059",
			"PF_STEP_NAME": "main",
			"PF_USER_NAME": "root"
		},
		"startTime": "2022-04-06 14:59:18",
		"endTime": "",
		"status": "init",
		"deps": "",
		"dockerEnv": "iregistry.baidu-int.com/bmlc/framework/paddle:2.0.2-gpu-cuda10.1-cudnn7",
		"artifacts": {
			"Input": {},
			"Output": {},
			"OutputList": null
		},
		"cache": {
			"Enable": false,
			"MaxExpiredTime": "600",
			"FsScope": "./lalal"
		},
		"jobMessage": "",
		"cacheRunID": ""
	},
	"nomain": {
		"jobID": "job-run-000059-nomain-465e402b",
		"name": "run-000059-nomain",
		"command": "echo 222",
		"parameters": {
			"test": "222"
		},
		"env": {
			"PF_FS_ID": "fs-root-cyang14",
			"PF_FS_NAME": "cyang14",
			"PF_JOB_CLUSTER_ID": "cluster-05f3854d",
			"PF_JOB_FLAVOUR": "flavour1",
			"PF_JOB_MODE": "Pod",
			"PF_JOB_NAMESPACE": "default",
			"PF_JOB_PRIORITY": "HIGH",
			"PF_JOB_QUEUE_ID": "queue-48ec41bd",
			"PF_JOB_QUEUE_NAME": "abc-q1",
			"PF_JOB_TYPE": "vcjob",
			"PF_RUN_ID": "run-000059",
			"PF_STEP_NAME": "nomain",
			"PF_USER_NAME": "root"
		},
		"startTime": "2022-04-06 14:59:18",
		"endTime": "",
		"status": "init",
		"deps": "",
		"dockerEnv": "iregistry.baidu-int.com/bmlc/framework/paddle:2.0.2-gpu-cuda10.1-cudnn7",
		"artifacts": {
			"Input": {},
			"Output": {},
			"OutputList": null
		},
		"cache": {
			"Enable": false,
			"MaxExpiredTime": "600",
			"FsScope": "./lalal"
		},
		"jobMessage": "",
		"cacheRunID": ""
	}
}`

func getMockRunWithRuntime() models.Run {
	run1 := models.Run{
		ID:       MockRunID1,
		Name:     "run_without_runtime",
		UserName: MockRootUser,
		FsID:     MockFsID1,
		Status:   common.StatusRunRunning,
		RunYaml:  "name: myproject\n\ndocker_env: iregistry.baidu-int.com/bmlc/framework/paddle:2.0.2-gpu-cuda10.1-cudnn7\n\nentry_points:\n  main:\n    parameters:\n      test: \"111\"\n    command: \"echo {{test}}\"\n    env:\n      PF_JOB_QUEUE_NAME: abc-q1\n      PF_JOB_TYPE: vcjob\n      PF_JOB_MODE: Pod\n      PF_JOB_FLAVOUR: flavour1\n      PF_JOB_PRIORITY: HIGH\n    cache:\n      enable: false\n      max_expired_time: 600\n      fs_scope: \"./lalal\"\n  nomain:\n    parameters:\n      test: \"222\"\n    command: \"echo {{test}}\"\n    env:\n      PF_JOB_QUEUE_NAME: abc-q1\n      PF_JOB_TYPE: vcjob\n      PF_JOB_MODE: Pod\n      PF_JOB_FLAVOUR: flavour1\n      PF_JOB_PRIORITY: HIGH\n    cache:\n      enable: false\n      max_expired_time: 600\n      fs_scope: \"./lalal\"\ncache:\n  enable: true\n  max_expired_time: 300\n  fs_scope: \"./for_fsscope\"\n",
	}
	return run1
}

func TestGetJobByRun(t *testing.T) {
	db_fake.InitFakeDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}
	run := getMockRunWithRuntime()
	runID, err := models.CreateRun(ctx.Logging(), &run)
	assert.Nil(t, err)

	runtimeView := schema.RuntimeView{}

	json.Unmarshal([]byte(runtimeRaw), &runtimeView)

	models.CreateRunJobs(ctx.Logging(), runtimeView, runID)
	for _, job := range runtimeView {
		runJob := models.ParseRunJob(&job)
		models.UpdateRunJob(ctx.Logging(), run.ID, runJob)
	}

	jobView, err := GetJobByRun(runID, "main")
	assert.Nil(t, err)
	assert.Equal(t, "job-run-000059-main-b7a9a264", jobView.JobID)
}
