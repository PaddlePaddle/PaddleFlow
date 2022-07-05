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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	MockRunID1   = "run-id_1"
	MockRunName1 = "run-name_1"
	MockFsID1    = "fs-id_1"
	MockRunID3   = "run-id_3"

	MockRunID2   = "run-id_2"
	MockRunName2 = "run-name_2"
	MockUserID2  = "user-id_2"
	MockFsID2    = "fs-id_2"

	runDagYamlPath = "./testcase/run_dag.yaml"
)

func getMockRun1() models.Run {
	run1 := models.Run{
		ID:         MockRunID1,
		Name:       MockRunName1,
		UserName:   MockRootUser,
		GlobalFsID: MockFsID1,
		Status:     common.StatusRunPending,
		RunYaml:    string(loadCase(runYamlPath)),
	}
	return run1
}

func getMockRun1_3() models.Run {
	run1 := models.Run{
		ID:         MockRunID3,
		Name:       "run_without_runtime",
		UserName:   MockRootUser,
		GlobalFsID: MockFsID1,
		Status:     common.StatusRunRunning,
		RunYaml:    string(loadCase(runYamlPath)),
	}
	return run1
}

func getMockRun2() models.Run {
	run2 := models.Run{
		ID:         MockRunID2,
		Name:       MockRunName2,
		UserName:   MockUserID2,
		GlobalFsID: MockFsID2,
		Status:     common.StatusRunPending,
		RunYaml:    string(loadCase(runYamlPath)),
	}
	return run2
}

func getMockFullRun() (models.Run, error) {
	runYaml := loadCase(runDagYamlPath)
	wfs, err := schema.GetWorkflowSource(runYaml)
	if err != nil {
		return models.Run{}, err
	}
	run := models.Run{
		Name:           "full_run",
		Source:         "run.yaml",
		UserName:       "root",
		GlobalFsID:     "fs-root-cyang14",
		GlobalFsName:   "cyang14",
		Description:    "desc",
		Parameters:     map[string]interface{}{},
		RunYaml:        string(runYaml),
		WorkflowSource: wfs,
		Status:         common.StatusRunInitiating,
	}
	return run, nil
}

func TestListRunSuccess(t *testing.T) {
	driver.InitMockDB()
	ctx1 := &logger.RequestContext{UserName: MockRootUser}
	ctx2 := &logger.RequestContext{UserName: MockUserID2}
	var err error
	run1 := getMockRun1()
	run1.ID, err = models.CreateRun(ctx1.Logging(), &run1)
	assert.Nil(t, err)
	run2 := getMockRun2()
	run2.ID, err = models.CreateRun(ctx2.Logging(), &run2)
	assert.Nil(t, err)
	run3UnderUser1 := getMockRun1_3()
	run3UnderUser1.ID, err = models.CreateRun(ctx1.Logging(), &run3UnderUser1)
	assert.Nil(t, err)

	emptyFilter := make([]string, 0)
	// test list runs under user1
	listRunResponse, err := ListRun(ctx1, "", 50, emptyFilter, emptyFilter, emptyFilter, emptyFilter, emptyFilter, emptyFilter)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(listRunResponse.RunList))
	assert.Equal(t, MockRootUser, listRunResponse.RunList[0].UserName)

	// test list runs under user2
	listRunResponse, err = ListRun(ctx2, "", 50, emptyFilter, emptyFilter, emptyFilter, emptyFilter, emptyFilter, emptyFilter)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(listRunResponse.RunList))
	assert.Equal(t, MockUserID2, listRunResponse.RunList[0].UserName)
}

func TestGetRunSuccess(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}
	var err error
	// test no runtime
	run1 := getMockRun1()
	run1.ID, err = models.CreateRun(ctx.Logging(), &run1)
	assert.Nil(t, err)

	runRsp, err := GetRunByID(ctx.Logging(), ctx.UserName, run1.ID)
	assert.Nil(t, err)
	assert.Equal(t, run1.ID, runRsp.ID)
	assert.Equal(t, run1.Name, runRsp.Name)
	assert.Equal(t, run1.Status, runRsp.Status)
}

func TestGetRunFail(t *testing.T) {
	driver.InitMockDB()
	var err error
	ctx := &logger.RequestContext{UserName: MockRootUser}
	run1 := getMockRun1()
	run1.ID, err = models.CreateRun(ctx.Logging(), &run1)

	// test non-admin user no access to other users' run
	ctxOtherNonAdmin := &logger.RequestContext{UserName: "non-admin"}
	_, err = GetRunByID(ctxOtherNonAdmin.Logging(), ctxOtherNonAdmin.UserName, run1.ID)
	assert.NotNil(t, err)
	assert.Equal(t, common.NoAccessError("non-admin", common.ResourceTypeRun, run1.ID).Error(), err.Error())

	// test no record
	_, err = GetRunByID(ctx.Logging(), ctx.UserName, "run-id_non_existed")
	assert.NotNil(t, err)
	assert.Equal(t, common.NotFoundError(common.ResourceTypeRun, "run-id_non_existed").Error(), err.Error())
}

func TestCallback(t *testing.T) {
	driver.InitMockDB()
	var err error
	ctx := &logger.RequestContext{UserName: MockRootUser}

	// test update activated_at
	run1 := getMockRun1()
	run1.ID, err = models.CreateRun(ctx.Logging(), &run1)
	assert.Nil(t, err)
	event1 := pipeline.WorkflowEvent{
		Event: pipeline.WfEventRunUpdate,
		Extra: map[string]interface{}{
			common.WfEventKeyRunID:  run1.ID,
			common.WfEventKeyStatus: common.StatusRunRunning,
		},
	}
	f := UpdateRuntimeFunc
	f(run1.ID, &event1)
	updatedRun, err := models.GetRunByID(ctx.Logging(), run1.ID)
	assert.Nil(t, err)
	assert.NotEmpty(t, updatedRun.UpdateTime)
	assert.Equal(t, common.StatusRunRunning, updatedRun.Status)

	// test not update activated_at
	run3 := getMockRun1_3()
	run3.ID, err = models.CreateRun(ctx.Logging(), &run3)
	assert.Nil(t, err)
	f(run3.ID, &event1)
	updatedRun, err = models.GetRunByID(ctx.Logging(), run3.ID)
	assert.Nil(t, err)
	assert.False(t, updatedRun.ActivatedAt.Valid)
	assert.Empty(t, updatedRun.ActivateTime)
}

func TestNewWorkflowByRun(t *testing.T) {
	driver.InitMockDB()
	var err error
	run, err := getMockFullRun()
	assert.Nil(t, err)
	wf, err := newWorkflowByRun(run)
	assert.Nil(t, err)

	text, _ := json.Marshal(wf.BaseWorkflow.Source)
	fmt.Println(string(text))

	run1, err := getMockFullRun()
	assert.Nil(t, err)
	run1.WorkflowSource.Disabled = "square-loop.square"
	_, err = newWorkflowByRun(run1)
	assert.NotNil(t, err)
	assert.Equal(t, "disabled component[square] is refered by [square-loop]", err.Error())

	run1.WorkflowSource.Disabled = "process-negetive.condition2.show"
	_, err = newWorkflowByRun(run1)
	assert.NotNil(t, err)
	assert.Equal(t, "disabled component[show] is refered by [abs]", err.Error())

	run2, err := getMockFullRun()
	assert.Nil(t, err)
	run2.Parameters = map[string]interface{}{
		"square-loop.noComp.noParam": "1",
	}
	_, err = newWorkflowByRun(run2)
	assert.NotNil(t, err)
	assert.Equal(t, "component [noComp] not exist", err.Error())
	run2.Parameters = map[string]interface{}{
		"square-loop.square.num": 3,
	}
	_, err = newWorkflowByRun(run2)
	assert.Nil(t, err)

	run2.Parameters = map[string]interface{}{
		"randint.pString": "str",
		"randint.pFloat":  1.1,
		"randint.pPath":   "testcase/run.yaml",
	}
	_, err = newWorkflowByRun(run2)
	assert.Nil(t, err)
}
