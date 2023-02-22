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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

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
	MockFsID1    = "fs-mockUser-mockFs"
	MockRunID3   = "run-id_3"

	MockRunID2   = "run-id_2"
	MockRunName2 = "run-name_2"
	MockUserID2  = "user-id_2"
	MockFsID2    = "fs-mockUser-mockFs"

	runDagYamlPath = "./testcase/run_dag.yaml"
)

func getMockRun1() models.Run {
	run1 := models.Run{
		ID:       MockRunID1,
		Name:     MockRunName1,
		UserName: MockRootUser,
		FsID:     MockFsID1,
		Status:   common.StatusRunPending,
		RunYaml:  string(loadCase(runYamlPath)),
	}
	run1.Encode()
	return run1
}

func getMockRun1_3() models.Run {
	run1 := models.Run{
		ID:       MockRunID3,
		Name:     "run_without_runtime",
		UserName: MockRootUser,
		FsID:     MockFsID1,
		Status:   common.StatusRunRunning,
		RunYaml:  string(loadCase(runYamlPath)),
	}
	run1.Encode()
	return run1
}

func getMockRun2() models.Run {
	run2 := models.Run{
		ID:       MockRunID2,
		Name:     MockRunName2,
		UserName: MockUserID2,
		FsID:     MockFsID2,
		Status:   common.StatusRunPending,
		RunYaml:  string(loadCase(runYamlPath)),
	}
	run2.Encode()
	return run2
}

func getMockRun3() models.Run {
	failureOptions := schema.FailureOptions{Strategy: schema.FailureStrategyContinue}
	failureOptionsRaw, err := json.Marshal(failureOptions)
	if err != nil {
		panic(err)
	}

	run2 := models.Run{
		ID:                 MockRunID2,
		Name:               MockRunName2,
		UserName:           MockUserID2,
		FsID:               MockFsID2,
		Status:             common.StatusRunPending,
		FailureOptionsJson: string(failureOptionsRaw),
		RunYaml:            string(loadCase(runYamlPath)),
	}
	run2.Encode()
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
		UserName:       "mockUser",
		FsID:           "fs-mockUser-mockFs",
		FsName:         "mockFs",
		Description:    "desc",
		Parameters:     map[string]interface{}{},
		RunYaml:        string(runYaml),
		WorkflowSource: wfs,
		Status:         common.StatusRunInitiating,
	}
	run.Encode()
	return run, nil
}

func newMockWorkflowByRun(run models.Run) (*pipeline.Workflow, error) {
	// bwfTemp := &pipeline.BaseWorkflow{}
	// p1 := gomonkey.ApplyPrivateMethod(reflect.TypeOf(bwfTemp), "checkFs", func() error {
	// 	return nil
	// })
	// defer p1.Reset()
	return newWorkflowByRun(run)
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
	//driver.InitMockDB()
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

	run3 := getMockRun3()
	run3.ID, err = models.CreateRun(ctx.Logging(), &run3)
	assert.Nil(t, err)
	runRsp, err = GetRunByID(ctx.Logging(), ctx.UserName, run3.ID)
	assert.Nil(t, err)
	assert.Equal(t, runRsp.FailureOptions.Strategy, schema.FailureStrategyContinue)
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
			common.WfEventKeyRunID:     run1.ID,
			common.WfEventKeyStatus:    common.StatusRunRunning,
			common.WfEventKeyStartTime: "2022-07-07 13:15:04",
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
	wf, err := newMockWorkflowByRun(run)
	assert.Nil(t, err)

	text, _ := json.Marshal(wf.BaseWorkflow.Source)
	fmt.Println(string(text))

	run1, err := getMockFullRun()
	assert.Nil(t, err)
	run1.WorkflowSource.Disabled = "square-loop.square"
	_, err = newMockWorkflowByRun(run1)
	assert.NotNil(t, err)
	assert.Equal(t, "disabled component[square] is refered by [square-loop]", err.Error())

	run1.WorkflowSource.Disabled = "process-negetive.condition2.show"
	_, err = newMockWorkflowByRun(run1)
	assert.NotNil(t, err)
	assert.Equal(t, "disabled component[show] is refered by [abs]", err.Error())

	run2, err := getMockFullRun()
	assert.Nil(t, err)
	run2.Parameters = map[string]interface{}{
		"square-loop.noComp.noParam": "1",
	}
	_, err = newMockWorkflowByRun(run2)
	assert.NotNil(t, err)
	assert.Equal(t, "cannont find component to replace param with [square-loop.noComp.noParam]", err.Error())
	run2.Parameters = map[string]interface{}{
		"square-loop.square.num": 3,
	}
	_, err = newMockWorkflowByRun(run2)
	assert.Nil(t, err)

	run2.Parameters = map[string]interface{}{
		"randint.pString": "str",
		"randint.pFloat":  1.1,
		"randint.pPath":   "testcase/run.yaml",
	}
	_, err = newMockWorkflowByRun(run2)
	assert.Nil(t, err)

	run2.ID = "1445"
	_, err = newMockWorkflowByRun(run2)
	assert.Nil(t, err)

	_, ok := wfMap.Load(run2.ID)
	assert.True(t, ok)
}

func TestCreateRunByJson(t *testing.T) {
	jsonPath := "testcase/run_dag.json"
	jsonByte := loadCase(jsonPath)
	bodyUnstructured := unstructured.Unstructured{}
	if err := bodyUnstructured.UnmarshalJSON(jsonByte); err != nil && !runtime.IsMissingKind(err) {
		// MissingKindErr不影响Json的解析
		fmt.Println("err!!")
		return
	}
	bodyMap := bodyUnstructured.UnstructuredContent()
	parser := schema.Parser{}
	parser.TransJsonMap2Yaml(bodyMap)
	wfs, err := getWorkFlowSourceByJson(bodyMap)
	assert.Nil(t, err)
	fmt.Println(wfs.EntryPoints.EntryPoints["main"].(*schema.WorkflowSourceStep).Cache)

	run := models.Run{
		Name:           "full_run",
		Source:         "run.yaml",
		UserName:       "mockUser",
		FsID:           "fs-mockUser-mockFs",
		FsName:         "mockFs",
		Description:    "desc",
		Parameters:     map[string]interface{}{},
		RunYaml:        "",
		WorkflowSource: wfs,
		Status:         common.StatusRunInitiating,
	}
	run.Encode()
	wfPtr, err := newMockWorkflowByRun(run)
	assert.Nil(t, err)
	fmt.Println(wfPtr.Source.EntryPoints.EntryPoints["main"].(*schema.WorkflowSourceStep).Cache)
}

func TestCreateRun(t *testing.T) {
	driver.InitMockDB()
	ctx := logger.RequestContext{UserName: MockRootUser}

	patch1 := gomonkey.ApplyFunc(CheckFsAndGetID, func(string, string, string) (string, error) {
		return "", nil
	})
	defer patch1.Reset()

	runYaml := string(loadCase(runYamlPath))
	yamlRaw := base64.StdEncoding.EncodeToString([]byte(runYaml))
	createRunRequest := CreateRunRequest{
		RunYamlRaw: yamlRaw,
	}

	patch2 := gomonkey.ApplyFunc(ValidateAndStartRun, func(ctx logger.RequestContext, run models.Run, userName string, req CreateRunRequest) (CreateRunResponse, error) {
		assert.Nil(t, run.FailureOptions)
		assert.Equal(t, run.WorkflowSource.FailureOptions.Strategy, schema.FailureStrategyFailFast)
		return CreateRunResponse{}, nil
	})
	defer patch2.Reset()

	_, err := CreateRun(ctx, &createRunRequest, map[string]string{})
	assert.Nil(t, err)

	patch3 := gomonkey.ApplyFunc(ValidateAndStartRun, func(ctx logger.RequestContext, run models.Run, userName string, req CreateRunRequest) (CreateRunResponse, error) {
		assert.Equal(t, run.FailureOptions.Strategy, schema.FailureStrategyContinue)
		assert.Equal(t, run.WorkflowSource.FailureOptions.Strategy, schema.FailureStrategyContinue)
		return CreateRunResponse{}, nil
	})
	defer patch3.Reset()

	createRunRequest = CreateRunRequest{
		RunYamlRaw:     yamlRaw,
		FailureOptions: &schema.FailureOptions{Strategy: schema.FailureStrategyContinue},
	}
	_, err = CreateRun(ctx, &createRunRequest, map[string]string{})
	assert.Nil(t, err)
}

func TestStopRun(t *testing.T) {
	driver.InitMockDB()
	run := getMockRunWithoutRuntime()
	ctx := &logger.RequestContext{UserName: MockRootUser}
	runID, err := models.CreateRun(ctx.Logging(), &run)
	assert.Nil(t, err)

	wfMap.Store(runID, &pipeline.Workflow{})

	var wf *pipeline.Workflow
	patch := gomonkey.ApplyMethod(reflect.TypeOf(wf), "Stop", func(*pipeline.Workflow, bool) {
		return
	})
	defer patch.Reset()

	var r *models.Run
	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(r), "Encode", func(*models.Run) error {
		return nil
	})
	defer patch2.Reset()

	patch3 := gomonkey.ApplyFunc(GetRunByID, func(logEntry *log.Entry, userName string, runID string) (models.Run, error) {
		return run, nil
	})
	defer patch3.Reset()

	req := UpdateRunRequest{StopForce: false}
	err = StopRun(ctx.Logging(), "root", runID, req)
	assert.Nil(t, err)
}

func TestStartWf(t *testing.T) {
	run := models.Run{
		ID: "run=00001",
	}
	wfptr := &pipeline.Workflow{}
	patch := gomonkey.ApplyMethod(reflect.TypeOf(wfptr), "NewWorkflowRuntime", func(*pipeline.Workflow) error {
		return nil
	})
	defer patch.Reset()

	patch2 := gomonkey.ApplyFunc(models.UpdateRunStatus, func(logEntry *log.Entry, runID string, status string) error {
		return nil
	})
	defer patch2.Reset()

	patch3 := gomonkey.ApplyMethod(reflect.TypeOf(wfptr), "Start", func(*pipeline.Workflow) {
		return
	})
	defer patch3.Reset()

	StartWf(run, wfptr)
	_, ok := wfMap.Load(run.ID)
	assert.True(t, ok)
}

func TestRestartWf(t *testing.T) {
	run := models.Run{
		ID: "run=00001",
	}
	wfptr := &pipeline.Workflow{}

	patch := gomonkey.ApplyFunc(newWorkflowByRun, func(run models.Run) (*pipeline.Workflow, error) {
		return &pipeline.Workflow{}, nil
	})
	defer patch.Reset()

	patch2 := gomonkey.ApplyFunc(models.UpdateRunStatus, func(logEntry *log.Entry, runID string, status string) error {
		return nil
	})
	defer patch2.Reset()

	patch3 := gomonkey.ApplyMethod(reflect.TypeOf(wfptr), "Restart", func(*pipeline.Workflow, *schema.DagView, schema.PostProcessView) {
		return
	})
	defer patch3.Reset()

	patch4 := gomonkey.ApplyFunc(models.GetRunJobsOfRun, func(logEntry *log.Entry, runID string) ([]models.RunJob, error) {
		return nil, nil
	})
	defer patch4.Reset()

	patch5 := gomonkey.ApplyFunc(models.GetRunDagsOfRun, func(logEntry *log.Entry, runID string) ([]models.RunDag, error) {
		return nil, nil
	})
	defer patch5.Reset()

	var r *models.Run
	patch6 := gomonkey.ApplyMethod(reflect.TypeOf(r), "Encode", func(*models.Run) error {
		return nil
	})
	defer patch6.Reset()

	patch7 := gomonkey.ApplyFunc(models.CreateRun, func(logEntry *log.Entry, run *models.Run) (string, error) {
		return "", nil
	})
	defer patch7.Reset()

	patch8 := gomonkey.ApplyFunc(models.CreateRunDag, func(logEntry *log.Entry, runDag *models.RunDag) (int64, error) {
		return 234, nil
	})
	defer patch8.Reset()

	patch9 := gomonkey.ApplyMethod(reflect.TypeOf(r), "InitRuntime", func(_ *models.Run, jobs []models.RunJob, dags []models.RunDag) error {
		return nil
	})
	defer patch9.Reset()

	id, err := RestartWf(run, false)
	assert.Nil(t, err)
	_, ok := wfMap.Load(id)
	assert.True(t, ok)
}
