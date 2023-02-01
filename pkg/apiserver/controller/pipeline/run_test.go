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
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
	pplcommon "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
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

	runRsp, err := GetRunByID(ctx, ctx.UserName, run1.ID)
	assert.Nil(t, err)
	assert.Equal(t, run1.ID, runRsp.ID)
	assert.Equal(t, run1.Name, runRsp.Name)
	assert.Equal(t, run1.Status, runRsp.Status)

	run3 := getMockRun3()
	run3.ID, err = models.CreateRun(ctx.Logging(), &run3)
	assert.Nil(t, err)
	runRsp, err = GetRunByID(ctx, ctx.UserName, run3.ID)
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
	_, err = GetRunByID(ctxOtherNonAdmin, ctxOtherNonAdmin.UserName, run1.ID)
	assert.NotNil(t, err)
	assert.Equal(t, common.NoAccessError("non-admin", common.ResourceTypeRun, run1.ID).Error(), err.Error())

	// test no record
	_, err = GetRunByID(ctx, ctx.UserName, "run-id_non_existed")
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

	patch := gomonkey.ApplyMethod(reflect.TypeOf(&parser), "TransJsonMap2Yaml", func(*schema.Parser, map[string]interface{}) error {
		return fmt.Errorf("Unexpected error")
	})
	defer patch.Reset()

	ctx := &logger.RequestContext{UserName: MockRootUser}
	CreateRunByJson(ctx, bodyMap)
	assert.Equal(t, common.InvalidPipeline, ctx.ErrorCode)

	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(&parser), "TransJsonMap2Yaml", func(*schema.Parser, map[string]interface{}) error {
		return nil
	})
	defer patch2.Reset()

	patch3 := gomonkey.ApplyMethod(reflect.TypeOf(&parser), "ParseFsOptions", func(*schema.Parser, map[string]interface{},
		*schema.FsOptions) error {
		return fmt.Errorf("Unexpected error")
	})
	defer patch3.Reset()

	ctx.ErrorCode = ""
	CreateRunByJson(ctx, bodyMap)
	assert.Equal(t, common.InvalidPipeline, ctx.ErrorCode)

	patch4 := gomonkey.ApplyMethod(reflect.TypeOf(&parser), "ParseFsOptions", func(*schema.Parser, map[string]interface{},
		*schema.FsOptions) error {
		return nil
	})
	defer patch4.Reset()

	patch5 := gomonkey.ApplyFunc(ProcessJsonAttr, func(map[string]interface{}) error {
		return fmt.Errorf("Unexpected error")
	})
	defer patch5.Reset()

	ctx.ErrorCode = ""
	CreateRunByJson(ctx, bodyMap)
	assert.Equal(t, common.InvalidPipeline, ctx.ErrorCode)

	patch6 := gomonkey.ApplyFunc(ProcessJsonAttr, func(map[string]interface{}) error {
		return nil
	})
	defer patch6.Reset()

	patch7 := gomonkey.ApplyFunc(schema.CheckReg, func(string, string) bool {
		return false
	})
	defer patch7.Reset()
	ctx.ErrorCode = ""
	CreateRunByJson(ctx, bodyMap)
	assert.Equal(t, common.InvalidNamePattern, ctx.ErrorCode)

	patch8 := gomonkey.ApplyFunc(schema.RunYaml2Map, func([]byte) (map[string]interface{}, error) {
		return nil, fmt.Errorf("error")
	})
	defer patch8.Reset()
	ctx.ErrorCode = ""
	CreateRunByJson(ctx, bodyMap)
	assert.Equal(t, common.InvalidPipeline, ctx.ErrorCode)
}

func TestCreateRun(t *testing.T) {
	driver.InitMockDB()
	ctx := logger.RequestContext{UserName: MockRootUser}

	patch1 := gomonkey.ApplyFunc(CheckFsAndGetID, func(*logger.RequestContext, string, string) (string, error) {
		return "", nil
	})
	defer patch1.Reset()

	runYaml := string(loadCase(runYamlPath))
	yamlRaw := base64.StdEncoding.EncodeToString([]byte(runYaml))
	createRunRequest := CreateRunRequest{
		RunYamlRaw: yamlRaw,
	}

	patch2 := gomonkey.ApplyFunc(ValidateAndStartRun, func(ctx *logger.RequestContext, run models.Run, userName string, req CreateRunRequest) (CreateRunResponse, error) {
		assert.Nil(t, run.FailureOptions)
		assert.Equal(t, run.WorkflowSource.FailureOptions.Strategy, schema.FailureStrategyFailFast)
		return CreateRunResponse{}, nil
	})
	defer patch2.Reset()

	_, err := CreateRun(&ctx, &createRunRequest, map[string]string{})
	assert.Nil(t, err)

	patch3 := gomonkey.ApplyFunc(ValidateAndStartRun, func(ctx *logger.RequestContext, run models.Run, userName string, req CreateRunRequest) (CreateRunResponse, error) {
		assert.Equal(t, run.FailureOptions.Strategy, schema.FailureStrategyContinue)
		assert.Equal(t, run.WorkflowSource.FailureOptions.Strategy, schema.FailureStrategyContinue)
		return CreateRunResponse{}, nil
	})
	defer patch3.Reset()

	createRunRequest = CreateRunRequest{
		RunYamlRaw:     yamlRaw,
		FailureOptions: &schema.FailureOptions{Strategy: schema.FailureStrategyContinue},
	}
	_, err = CreateRun(&ctx, &createRunRequest, map[string]string{})
	assert.Nil(t, err)

	patch4 := gomonkey.ApplyFunc(schema.CheckReg, func(string, string) bool {
		return false
	})
	defer patch4.Reset()
	_, err = CreateRun(&ctx, &createRunRequest, map[string]string{})
	assert.Equal(t, common.InvalidNamePattern, ctx.ErrorCode)

	patch5 := gomonkey.ApplyFunc(schema.CheckReg, func(string, string) bool {
		return true
	})
	defer patch5.Reset()

	createRunRequest.ScheduledAt = "now"
	_, err = CreateRun(&ctx, &createRunRequest, map[string]string{})
	assert.Equal(t, common.InternalError, ctx.ErrorCode)

	createRunRequest.ScheduledAt = ""
	extra := map[string]string{
		FinalRunStatus: common.StatusRunPending,
	}
	_, err = CreateRun(&ctx, &createRunRequest, extra)
	assert.Equal(t, common.InvalidArguments, ctx.ErrorCode)
}

func TestBuildWorkflowSource(t *testing.T) {
	ctx := &logger.RequestContext{UserName: MockRootUser}

	createRunRequest := CreateRunRequest{
		RunYamlRaw: "abc",
	}
	buildWorkflowSource(ctx, createRunRequest, "abc")
	assert.Equal(t, common.MalformedYaml, ctx.ErrorCode)

	runYaml := string(loadCase(runYamlPath))
	yamlRaw := base64.StdEncoding.EncodeToString([]byte(runYaml))
	createRunRequest = CreateRunRequest{
		RunYamlRaw: yamlRaw,
	}

	patch := gomonkey.ApplyFunc(schema.GetWorkflowSource, func([]byte) (schema.WorkflowSource, error) {
		return schema.WorkflowSource{}, fmt.Errorf("abc")
	})
	defer patch.Reset()

	buildWorkflowSource(ctx, createRunRequest, "abc")
	assert.Equal(t, common.InvalidPipeline, ctx.ErrorCode)

	patch2 := gomonkey.ApplyFunc(schema.GetWorkflowSource, func([]byte) (schema.WorkflowSource, error) {
		return schema.WorkflowSource{}, nil
	})
	patch2.Reset()

	ctx.ErrorCode = ""
	buildWorkflowSource(ctx, createRunRequest, "abc")
	assert.Equal(t, common.InvalidPipeline, ctx.ErrorCode)

	createRunRequest = CreateRunRequest{
		PipelineID: "abc",
	}

	patch3 := gomonkey.ApplyFunc(CheckPipelineVersionPermission,
		func(ctx *logger.RequestContext, userName string, pipelineID string, Pv string) (bool, model.Pipeline, model.PipelineVersion, error) {
			ctx.ErrorCode = common.PipelineNotFound
			return true, model.Pipeline{}, model.PipelineVersion{}, fmt.Errorf("abc")
		})

	defer patch3.Reset()

	_, _, _, err := buildWorkflowSource(ctx, createRunRequest, "abc")
	assert.NotNil(t, err)
	assert.Equal(t, common.PipelineNotFound, ctx.ErrorCode)

	patch4 := gomonkey.ApplyFunc(CheckPipelineVersionPermission,
		func(ctx *logger.RequestContext, userName string, pipelineID string, Pv string) (bool, model.Pipeline, model.PipelineVersion, error) {
			ctx.ErrorCode = common.AccessDenied
			return false, model.Pipeline{}, model.PipelineVersion{}, nil
		})

	defer patch4.Reset()
	_, _, _, err = buildWorkflowSource(ctx, createRunRequest, "abc")
	assert.Equal(t, common.AccessDenied, ctx.ErrorCode)
	assert.Equal(t, err.Error(), common.NoAccessError("root", "pipeline", "abc").Error())

	createRunRequest = CreateRunRequest{}
	buildWorkflowSource(ctx, createRunRequest, "")
	assert.Equal(t, common.InvalidArguments, ctx.ErrorCode)

	createRunRequest = CreateRunRequest{}
	patch5 := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return []byte{}, fmt.Errorf("abc")
	})
	defer patch5.Reset()
	buildWorkflowSource(ctx, createRunRequest, "abc")
	assert.Equal(t, common.ReadYamlFileFailed, ctx.ErrorCode)

	patch6 := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return os.ReadFile("../../../../example/wide_and_deep/run.yaml")
	})
	defer patch6.Reset()

	patch7 := gomonkey.ApplyFunc(schema.GetWorkflowSource, func([]byte) (schema.WorkflowSource, error) {
		return schema.WorkflowSource{}, nil
	})
	defer patch7.Reset()
	_, _, _, err = buildWorkflowSource(ctx, createRunRequest, "abc")
	assert.Nil(t, err)
}

func TestRunYamlAndReqToWfs(t *testing.T) {
	ctx := &logger.RequestContext{UserName: MockRootUser}
	createRunRequest := CreateRunRequest{}
	patch := gomonkey.ApplyFunc(schema.GetWorkflowSource, func([]byte) (schema.WorkflowSource, error) {
		return schema.WorkflowSource{}, fmt.Errorf("unexpected error")
	})
	defer patch.Reset()

	runYamlAndReqToWfs(ctx, "pipeline", createRunRequest)
	assert.Equal(t, common.InvalidPipeline, ctx.ErrorCode)

	patch2 := gomonkey.ApplyFunc(schema.GetWorkflowSource, func([]byte) (schema.WorkflowSource, error) {
		wfs := schema.WorkflowSource{
			FsOptions: schema.FsOptions{
				MainFS: schema.FsMount{
					Name: "abc",
				},
			},
		}
		return wfs, nil
	})
	defer patch2.Reset()
	createRunRequest = CreateRunRequest{FsName: "abd"}
	runYamlAndReqToWfs(ctx, "pipeline", createRunRequest)
	assert.Equal(t, common.InvalidArguments, ctx.ErrorCode)
}

func TestValidateAndCreateRun(t *testing.T) {
	ctx := &logger.RequestContext{
		UserName: MockRootUser,
	}

	createRunRequest := CreateRunRequest{}
	r := &models.Run{}

	ValidateAndCreateRun(ctx, r, "abc", createRunRequest)
	assert.Equal(t, common.InternalError, ctx.ErrorCode)
	ctx.ErrorCode = ""

	ctx.RequestID = "abcd"
	patch := gomonkey.ApplyMethod(reflect.TypeOf(r), "Encode", func(*models.Run) error {
		return fmt.Errorf("error")
	})
	defer patch.Reset()
	ValidateAndCreateRun(ctx, r, "abc", createRunRequest)
	assert.Equal(t, common.InternalError, ctx.ErrorCode)
	ctx.ErrorCode = ""

	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(r), "Encode", func(*models.Run) error {
		return nil
	})
	defer patch2.Reset()

	patch3 := gomonkey.ApplyFunc(pipeline.NewWorkflow, func(wfSource schema.WorkflowSource, runID string, params map[string]interface{}, extra map[string]string,
		callbacks pipeline.WorkflowCallbacks) (*pipeline.Workflow, error) {
		return &pipeline.Workflow{}, fmt.Errorf("error")
	})
	defer patch3.Reset()
	ValidateAndCreateRun(ctx, r, "abc", createRunRequest)
	assert.Equal(t, common.InvalidPipeline, ctx.ErrorCode)
	ctx.ErrorCode = ""

	patch4 := gomonkey.ApplyFunc(pipeline.NewWorkflow, func(wfSource schema.WorkflowSource, runID string, params map[string]interface{}, extra map[string]string,
		callbacks pipeline.WorkflowCallbacks) (*pipeline.Workflow, error) {
		return &pipeline.Workflow{}, nil
	})
	defer patch4.Reset()

	patch5 := gomonkey.ApplyFunc(models.CreateRun, func(*log.Entry, *models.Run) (string, error) {
		return "123", fmt.Errorf("error")
	})
	defer patch5.Reset()
	ValidateAndCreateRun(ctx, r, "abc", createRunRequest)
	assert.Equal(t, common.InternalError, ctx.ErrorCode)
	ctx.ErrorCode = ""
}

// go test 需要加上-gcflags=all=-l 参数才会生效
func TestCheckFs(t *testing.T) {
	ctx := &logger.RequestContext{
		UserName: MockRootUser,
	}
	wfs := &schema.WorkflowSource{}

	patch := gomonkey.ApplyMethod(reflect.TypeOf(wfs), "GetFsMounts", func(*schema.WorkflowSource) ([]schema.FsMount, error) {
		return nil, fmt.Errorf("error")
	})
	defer patch.Reset()

	checkFs(ctx, MockRootUser, wfs)
	assert.Equal(t, common.InvalidPipeline, ctx.ErrorCode)

	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(wfs), "GetFsMounts", func(*schema.WorkflowSource) ([]schema.FsMount, error) {
		return nil, nil
	})
	defer patch2.Reset()

	wfs.FsOptions.MainFS.SubPath = "/tmp/a.txt"
	wfs.FsOptions.MainFS.Name = "abcfs"
	patch3 := gomonkey.ApplyFunc(handler.NewFsHandlerWithServer,
		func(string, *log.Entry) (*handler.FsHandler, error) {
			return &handler.FsHandler{}, fmt.Errorf("error patch3")
		})
	defer patch3.Reset()

	checkFs(ctx, MockRootUser, wfs)
	assert.Equal(t, common.InvalidArguments, ctx.ErrorCode)

	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer

	var hl *handler.FsHandler
	patch5 := gomonkey.ApplyMethod(reflect.TypeOf(hl), "Exist", func(*handler.FsHandler, string) (bool, error) {
		return false, fmt.Errorf("error")
	})
	defer patch5.Reset()

	checkFs(ctx, MockRootUser, wfs)
	assert.Equal(t, common.InternalError, ctx.ErrorCode)

	patch6 := gomonkey.ApplyMethod(reflect.TypeOf(hl), "Exist", func(*handler.FsHandler, string) (bool, error) {
		return true, nil
	})
	defer patch6.Reset()

	patch7 := gomonkey.ApplyMethod(reflect.TypeOf(hl), "IsDir", func(*handler.FsHandler, string) (bool, error) {
		return false, fmt.Errorf("error")
	})
	defer patch7.Reset()
	checkFs(ctx, MockRootUser, wfs)
	assert.Equal(t, common.InternalError, ctx.ErrorCode)

	patch8 := gomonkey.ApplyMethod(reflect.TypeOf(hl), "IsDir", func(*handler.FsHandler, string) (bool, error) {
		return false, nil
	})
	defer patch8.Reset()
	checkFs(ctx, MockRootUser, wfs)
	assert.Equal(t, common.InvalidArguments, ctx.ErrorCode)

	wfs.FsOptions.MainFS.SubPath = ""
	patch9 := gomonkey.ApplyMethod(reflect.TypeOf(wfs), "GetFsMounts", func(*schema.WorkflowSource) ([]schema.FsMount, error) {
		fsMounts := []schema.FsMount{
			schema.FsMount{
				Name: "abc",
			},
		}
		return fsMounts, nil
	})
	defer patch9.Reset()

	patch10 := gomonkey.ApplyFunc(CheckFsAndGetID,
		func(ctx *logger.RequestContext, fsUserName string, fsName string) (fsID string, err error) {
			return "abc", nil
		})
	defer patch10.Reset()
	checkFs(ctx, "ahz", wfs)
}

func TestStopRun(t *testing.T) {
	ctx := &logger.RequestContext{
		UserName: MockRootUser,
	}

	patch := gomonkey.ApplyFunc(GetRunByID, func(ctx *logger.RequestContext, userName string, runID string) (models.Run, error) {
		ctx.ErrorCode = common.InvalidArguments
		return models.Run{}, fmt.Errorf("patch error")
	})
	defer patch.Reset()

	req := UpdateRunRequest{}
	StopRun(ctx, MockRootUser, "runID", req)
	assert.Equal(t, common.InvalidArguments, ctx.ErrorCode)

	ctx.ErrorCode = ""

	patch2 := gomonkey.ApplyFunc(GetRunByID, func(ctx *logger.RequestContext, userName string, runID string) (models.Run, error) {
		run := models.Run{
			Status: common.StatusRunTerminating,
		}
		return run, nil
	})
	defer patch2.Reset()

	StopRun(ctx, MockRootUser, "runID", req)
	assert.Equal(t, common.ActionNotAllowed, ctx.ErrorCode)

	patch3 := gomonkey.ApplyFunc(GetRunByID, func(ctx *logger.RequestContext, userName string, runID string) (models.Run, error) {
		run := models.Run{
			Status: common.StatusRunRunning,
		}
		return run, nil
	})
	defer patch3.Reset()

	StopRun(ctx, MockRootUser, "runID", req)
	assert.Equal(t, common.InternalError, ctx.ErrorCode)
	ctx.ErrorCode = ""

	wfMap["runID"] = &pipeline.Workflow{}

	patch4 := gomonkey.ApplyFunc(models.UpdateRun, func(*log.Entry, string, models.Run) error {
		return fmt.Errorf("patch4 error")
	})
	defer patch4.Reset()

	patch5 := gomonkey.ApplyFunc(models.UpdateRun, func(*log.Entry, string, models.Run) error {
		return nil
	})
	defer patch5.Reset()

	var wf *pipeline.Workflow
	patch6 := gomonkey.ApplyMethod(reflect.TypeOf(wf), "Stop", func(*pipeline.Workflow, bool) {
		ctx.ErrorCode = ""
	})
	defer patch6.Reset()
	StopRun(ctx, MockRootUser, "runID", req)
	assert.Equal(t, "", ctx.ErrorCode)
}

func TestRetryRun(t *testing.T) {
	ctx := &logger.RequestContext{
		UserName: MockRootUser,
	}

	patch := gomonkey.ApplyFunc(GetRunByID, func(ctx *logger.RequestContext, userName string, runID string) (models.Run, error) {
		ctx.ErrorCode = common.InvalidArguments
		return models.Run{}, fmt.Errorf("patch error")
	})
	defer patch.Reset()
	RetryRun(ctx, "runid")

	assert.Equal(t, common.InvalidArguments, ctx.ErrorCode)

	ctx.ErrorCode = ""
	patch2 := gomonkey.ApplyFunc(GetRunByID, func(ctx *logger.RequestContext, userName string, runID string) (models.Run, error) {
		run := models.Run{
			Status: common.StatusRunTerminating,
		}
		return run, nil
	})
	defer patch2.Reset()

	RetryRun(ctx, "runID")
	assert.Equal(t, common.ActionNotAllowed, ctx.ErrorCode)

	ctx.ErrorCode = ""
	patch3 := gomonkey.ApplyFunc(GetRunByID, func(ctx *logger.RequestContext, userName string, runID string) (models.Run, error) {
		run := models.Run{
			Status: common.StatusRunTerminated,
		}
		return run, nil
	})
	defer patch3.Reset()

	patch4 := gomonkey.ApplyFunc(schema.GetWorkflowSource, func([]byte) (schema.WorkflowSource, error) {
		return schema.WorkflowSource{}, fmt.Errorf("patch4 error")
	})
	defer patch4.Reset()
	RetryRun(ctx, "runID")
	assert.Equal(t, common.InvalidPipeline, ctx.ErrorCode)
}

func TestDeleteRun(t *testing.T) {
	ctx := &logger.RequestContext{
		UserName: MockRootUser,
	}

	patch := gomonkey.ApplyFunc(GetRunByID, func(ctx *logger.RequestContext, userName string, runID string) (models.Run, error) {
		ctx.ErrorCode = common.InvalidArguments
		return models.Run{}, fmt.Errorf("patch error")
	})
	defer patch.Reset()

	req := &DeleteRunRequest{}
	DeleteRun(ctx, "runid", req)

	assert.Equal(t, common.InvalidArguments, ctx.ErrorCode)

	ctx.ErrorCode = ""
	patch2 := gomonkey.ApplyFunc(GetRunByID, func(ctx *logger.RequestContext, userName string, runID string) (models.Run, error) {
		run := models.Run{
			Status: common.StatusRunTerminating,
		}
		return run, nil
	})
	defer patch2.Reset()

	DeleteRun(ctx, "runid", req)
	assert.Equal(t, common.ActionNotAllowed, ctx.ErrorCode)

	ctx.ErrorCode = ""
	patch3 := gomonkey.ApplyFunc(GetRunByID, func(ctx *logger.RequestContext, userName string, runID string) (models.Run, error) {
		run := models.Run{
			Status: common.StatusRunTerminated,
		}
		return run, nil
	})
	defer patch3.Reset()

	req.CheckCache = true
	var run *models.Run
	patch4 := gomonkey.ApplyMethod(reflect.TypeOf(run), "GetRunCacheIDList", func(*models.Run) []string {
		return []string{"cache1", "cache2", "cache3"}
	})
	defer patch4.Reset()

	patch5 := gomonkey.ApplyFunc(models.ListRun,
		func(logEntry *log.Entry, pk int64, maxKeys int, userFilter, fsFilter, runFilter, nameFilter, statusFilter, scheduleIdFilter []string) ([]models.Run, error) {
			run := models.Run{}
			return []models.Run{run}, nil
		})
	defer patch5.Reset()
	DeleteRun(ctx, "runid", req)
	assert.Equal(t, common.ActionNotAllowed, ctx.ErrorCode)

	req.CheckCache = false
	patch6 := gomonkey.ApplyFunc(GetRunByID, func(ctx *logger.RequestContext, userName string, runID string) (models.Run, error) {
		run := models.Run{
			Status: common.StatusRunTerminated,
			FsID:   "fs-01",
		}
		return run, nil
	})
	defer patch6.Reset()

	patch7 := gomonkey.ApplyFunc(pplcommon.NewResourceHandler, func(runID string, fsID string, logger *log.Entry) (pplcommon.ResourceHandler, error) {
		return pplcommon.ResourceHandler{}, fmt.Errorf("patch7 error")
	})
	defer patch7.Reset()
	DeleteRun(ctx, "runid", req)
	assert.Equal(t, common.InternalError, ctx.ErrorCode)

	patch8 := gomonkey.ApplyFunc(pplcommon.NewResourceHandler, func(runID string, fsID string, logger *log.Entry) (pplcommon.ResourceHandler, error) {
		return pplcommon.ResourceHandler{}, nil
	})
	defer patch8.Reset()

	var rh *pplcommon.ResourceHandler
	patch9 := gomonkey.ApplyMethod(reflect.TypeOf(rh), "ClearResource", func(*pplcommon.ResourceHandler) error {
		return fmt.Errorf("patch9 error")
	})
	defer patch9.Reset()
	ctx.ErrorCode = ""
	DeleteRun(ctx, "runid", req)
	assert.Equal(t, common.InternalError, ctx.ErrorCode)

	patch10 := gomonkey.ApplyMethod(reflect.TypeOf(rh), "ClearResource", func(*pplcommon.ResourceHandler) error {
		return nil
	})
	defer patch10.Reset()

	patch11 := gomonkey.ApplyFunc(models.DeleteRun, func(*log.Entry, string) error {
		return fmt.Errorf("patch11 error")
	})
	defer patch11.Reset()
	ctx.ErrorCode = ""
	DeleteRun(ctx, "runid", req)
	assert.Equal(t, common.InternalError, ctx.ErrorCode)

	patch12 := gomonkey.ApplyFunc(models.DeleteRun, func(*log.Entry, string) error {
		return nil
	})
	defer patch12.Reset()
	ctx.ErrorCode = ""
	DeleteRun(ctx, "runid", req)
	assert.Equal(t, "", ctx.ErrorCode)

}
