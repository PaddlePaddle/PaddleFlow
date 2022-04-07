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

package run

import (
	"paddleflow/pkg/common/database"
	"testing"

	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/logger"
)

const runtimeRaw = `{"preprocess":{"jobID":"job-run-000361-preprocess-1220449a","name":"run-000361-preprocess","command":"mkdir data \u0026\u0026 cd ./data/ \u0026\u0026 mkdir train \u0026\u0026 mkdir validata","parameters":{"data_path":"./data/"},"env":{"PF_FS_ID":"fs-root-cyang14","PF_FS_NAME":"cyang14","PF_INPUT_ARTIFACT_DATA1":"./data/","PF_JOB_FLAVOUR":"flavour1","PF_JOB_MODE":"Pod","PF_JOB_NAMESPACE":"default","PF_JOB_PRIORITY":"NORMAL","PF_JOB_PVC_NAME":"pfs-fs-root-cyang14-pvc","PF_JOB_QUEUE_NAME":"qdh","PF_JOB_TYPE":"vcjob","PF_OUTPUT_ARTIFACT_TRAIN_DATA":"","PF_OUTPUT_ARTIFACT_VALIDATE_DATA":"","PF_RUN_ID":"run-000361","PF_STEP_NAME":"preprocess","PF_USER_NAME":"root"},"startTime":"2022-02-09 17:07:41","endTime":"2022-02-09 17:07:46","status":"succeeded","deps":"","image":"paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7","artifacts":{"Input":{"data1":"./data/"},"Output":{"train_data":"","validate_data":""},"OutputList":null},"jobMessage":"ContainerCreating:"}}`

func getMockRunWithRuntime() models.Run {
	run1 := models.Run{
		ID:         MockRunID1,
		Name:       "run_with_runtime",
		UserName:   MockRootUser,
		FsID:       MockFsID1,
		Status:     common.StatusRunRunning,
		RuntimeRaw: runtimeRaw,
	}
	return run1
}

func TestGetJobByRun(t *testing.T) {
	db := database.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}
	run := getMockRunWithRuntime()
	runID, err := models.CreateRun(db, ctx.Logging(), &run)
	assert.Nil(t, err)

	jobView, err := GetJobByRun(runID, "preprocess")
	assert.Nil(t, err)
	assert.Equal(t, "job-run-000361-preprocess-1220449a", jobView.JobID)
}
