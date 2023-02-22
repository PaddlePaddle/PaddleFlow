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
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const runtimePath = "./testcase/runtime.json"
const runYamlPath = "./testcase/run.yaml"

func getMockRunWithoutRuntime() models.Run {
	run1 := models.Run{
		ID:       MockRunID1,
		Name:     "run_without_runtime",
		UserName: MockRootUser,
		FsID:     MockFsID1,
		Status:   common.StatusRunRunning,
		RunYaml:  string(loadCase(runYamlPath)),
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
	run := getMockRunWithoutRuntime()
	runID, err := models.CreateRun(ctx.Logging(), &run)
	assert.Nil(t, err)

	runJob := models.RunJob{
		ID:          "job-run-post",
		RunID:       runID,
		ParentDagID: "",
		StepName:    "post",
	}
	_, err = models.CreateRunJob(ctx.Logging(), &runJob)
	assert.Nil(t, err)

	jobView, err := GetJobByRun("job-run-post")
	assert.Nil(t, err)
	assert.Equal(t, "job-run-post", jobView.JobID)
}

func TestUpdateRunByWfEvent(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}
	run := getMockRunWithoutRuntime()
	runID, err := models.CreateRun(ctx.Logging(), &run)
	assert.Nil(t, err)

	wfMap.Store(runID, "abc")
	event := &pipeline.WorkflowEvent{
		Event: pipeline.WfEventRunUpdate,
		Extra: map[string]interface{}{
			common.WfEventKeyRunID:     runID,
			common.WfEventKeyStatus:    common.StatusRunSucceeded,
			common.WfEventKeyStartTime: "2022-09-09 10:00:09",
		},
		Message: "mesg",
	}

	patch5 := gomonkey.ApplyFunc(models.GetRunByID, func(logEntry *log.Entry, runID string) (models.Run, error) {
		return run, nil
	})
	defer patch5.Reset()
	_, flag := UpdateRunByWfEvent(runID, event)
	assert.True(t, flag)

	_, ok := wfMap.Load(runID)
	assert.False(t, ok)
}
