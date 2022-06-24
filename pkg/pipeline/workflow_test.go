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
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	pplcommon "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	runYamlPath           string = "./testcase/run.yaml"
	noAtfYamlPath         string = "./testcase/runNoAtf.yaml"
	runWrongParamYamlPath string = "./testcase/runWrongParam.yaml"
	runCircleYamlPath     string = "./testcase/runCircle.yaml"
)

var mockCbs = WorkflowCallbacks{
	UpdateRunCb: func(runID string, event interface{}) bool {
		fmt.Println("UpdateRunCb: ", event)
		return true
	},
	LogCacheCb: func(req schema.LogRunCacheRequest) (string, error) {
		return "cch-000027", nil
	},
	ListCacheCb: func(firstFp, fsID, step, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{{RunID: "run-000027"}, {RunID: "run-000028"}}, nil
	},
}

func loadTwoPostCaseSource() (schema.WorkflowSource, error) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	if err != nil {
		return schema.WorkflowSource{}, err
	}
	postStep := schema.WorkflowSourceStep{
		Command: "echo test",
	}
	wfs.PostProcess["mail2"] = &postStep
	return wfs, nil
}

// extra map里面的value可能会被修改，从而影响后面的case
// 为了避免上面的问题，封装成函数，不同case分别调用函数，获取全新的extra map
func GetExtra() map[string]string {
	var extra = map[string]string{
		pplcommon.WfExtraInfoKeySource:   "./testcase/run.yaml",
		pplcommon.WfExtraInfoKeyFsID:     "mockFsID",
		pplcommon.WfExtraInfoKeyFsName:   "mockFsname",
		pplcommon.WfExtraInfoKeyUserName: "mockUser",
	}

	return extra
}

// 测试NewBaseWorkflow, 只传yaml内容
func TestNewBaseWorkflowByOnlyRunYaml(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	if err := bwf.validate(); err != nil {
		t.Errorf("validate failed. error: %v", err)
	}
}

// 测试 BaseWorkflow
func TestNewBaseWorkflow(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := BaseWorkflow{
		Name:   "test_workflow",
		Desc:   "test base workflow",
		Entry:  "main",
		Source: wfs,
		Extra:  extra,
	}
	if err := bwf.validate(); err != nil {
		t.Errorf("test validate failed: Name[%s]", bwf.Name)
	}
}

// 测试带环流程
func TestNewBaseWorkflowWithCircle(t *testing.T) {
	testCase := loadcase(runCircleYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	_, err = bwf.topologicalSort(bwf.Source.EntryPoints)
	assert.NotNil(t, err)
}

// 测试无环流程
func TestTopologicalSort_noCircle(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	fmt.Println(bwf.Source.EntryPoints)
	result, err := bwf.topologicalSort(bwf.Source.EntryPoints)
	assert.Nil(t, err)
	assert.Equal(t, "data-preprocess", result[0])
	assert.Equal(t, "main", result[1])
	assert.Equal(t, "validate", result[2])

	bwf = NewBaseWorkflow(wfs, "", "main", nil, extra)
	runSteps := bwf.getRunSteps()
	assert.Equal(t, 2, len(runSteps))
	result, err = bwf.topologicalSort(bwf.Source.EntryPoints)
	assert.Nil(t, err)
	assert.Equal(t, "data-preprocess", result[0])
	assert.Equal(t, "main", result[1])
}

// 测试运行 Workflow，节点disable成功
func TestCreateNewWorkflowRunDisabled_success(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 10)
	wf.runtime.entryPoints["data-preprocess"].disabled = true
	wf.runtime.entryPoints["main"].disabled = true
	wf.runtime.entryPoints["validate"].disabled = true
	wf.runtime.postProcess["mail"].disabled = true

	go wf.Start()

	time.Sleep(time.Millisecond * 100)
	fmt.Printf("%+v\n", *wf.runtime.entryPoints["data-preprocess"])
	fmt.Printf("%+v\n", *wf.runtime.entryPoints["main"])
	fmt.Printf("%+v\n", *wf.runtime.entryPoints["validate"])
	assert.Equal(t, common.StatusRunSucceeded, wf.runtime.status)
	assert.Equal(t, schema.StatusJobSkipped, wf.runtime.entryPoints["data-preprocess"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobSkipped, wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobSkipped, wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status)
}

// 测试运行 Workflow 成功
func TestCreateNewWorkflowRun_success(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	// 先是mock data-preprocess节点返回true
	// 再设置所有节点done = true
	// 保证data-preprocess能够成功结束，然后runtime再寻找下一个能运行的节点时，能够跳过后面的节点
	patches := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["data-preprocess"].job), "Succeeded", func(_ *PaddleFlowJob) bool {
		return true
	})
	defer patches.Reset()

	time.Sleep(time.Millisecond * 10)

	wf.runtime.entryPoints["data-preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["data-preprocess"].done = true
	wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["main"].done = true
	wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["validate"].done = true
	wf.runtime.postProcess["mail"].done = true
	wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded

	go wf.Start()

	// todo: remove event, receive event from job
	event1 := WorkflowEvent{Event: WfEventJobUpdate, Extra: map[string]interface{}{"event1": "step 1 data-process finished"}}
	wf.runtime.event <- event1
	time.Sleep(time.Millisecond * 10)

	assert.Equal(t, common.StatusRunSucceeded, wf.runtime.status)
}

// 测试运行 Workflow 失败
func TestCreateNewWorkflowRun_failed(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	patch1 := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["data-preprocess"].job), "Started", func(_ *PaddleFlowJob) bool {
		return true
	})
	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["data-preprocess"].job), "NotEnded", func(_ *PaddleFlowJob) bool {
		return false
	})
	patch3 := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["data-preprocess"].job), "Failed", func(_ *PaddleFlowJob) bool {
		return true
	})
	patch4 := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["data-preprocess"].job), "Succeeded", func(_ *PaddleFlowJob) bool {
		return false
	})
	patch5 := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["data-preprocess"].job), "Skipped", func(_ *PaddleFlowJob) bool {
		return false
	})
	defer patch1.Reset()
	defer patch2.Reset()
	defer patch3.Reset()
	defer patch4.Reset()
	defer patch5.Reset()

	wf.runtime.entryPoints["data-preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobFailed
	wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status = schema.StatusJobCancelled
	wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status = schema.StatusJobCancelled
	wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status = schema.StatusJobFailed

	go wf.Start()
	time.Sleep(time.Millisecond * 10)

	// todo: remove event, receive event from job
	event1 := WorkflowEvent{Event: WfEventJobUpdate, Extra: map[string]interface{}{"event1": "step 1 data-process finished"}}
	wf.runtime.event <- event1
	time.Sleep(time.Millisecond * 100)

	assert.Equal(t, common.StatusRunFailed, wf.runtime.status)
}

// 测试停止 Workflow
func TestStopWorkflowRun(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	patch1 := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["data-preprocess"].job), "Succeeded", func(_ *PaddleFlowJob) bool {
		return true
	})
	defer patch1.Reset()

	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["main"].job), "Succeeded", func(_ *PaddleFlowJob) bool {
		return true
	})
	defer patch2.Reset()

	patch3 := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["validate"].job), "Succeeded", func(_ *PaddleFlowJob) bool {
		return false
	})
	patch4 := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["validate"].job), "Failed", func(_ *PaddleFlowJob) bool {
		return false
	})
	patch5 := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["validate"].job), "Terminated", func(_ *PaddleFlowJob) bool {
		return true
	})
	defer patch3.Reset()
	defer patch4.Reset()
	defer patch5.Reset()

	time.Sleep(time.Millisecond * 10)

	wf.runtime.entryPoints["data-preprocess"].done = true
	wf.runtime.entryPoints["main"].done = true
	wf.runtime.entryPoints["validate"].done = true
	wf.runtime.postProcess["mail"].done = true

	wf.runtime.entryPoints["data-preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status = schema.StatusJobTerminated
	wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded

	go wf.Start()
	time.Sleep(time.Millisecond * 10)

	wf.Stop(false)
	assert.Equal(t, common.StatusRunTerminating, wf.runtime.status)
	// todo: remove event, receive event from job
	event1 := WorkflowEvent{Event: WfEventJobUpdate, Extra: map[string]interface{}{"event1": "step 1 data-process finished"}}
	wf.runtime.event <- event1
	time.Sleep(time.Millisecond * 30)

	assert.Equal(t, common.StatusRunTerminated, wf.runtime.status)
}

// 测试baseWorkflow带entry初始化
func TestNewWorkflowFromEntry(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	baseWorkflow := NewBaseWorkflow(wfs, "", "main", nil, extra)
	wf := &Workflow{
		BaseWorkflow: baseWorkflow,
		callbacks:    mockCbs,
	}

	err = wf.newWorkflowRuntime()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(wf.runtime.entryPoints))
	_, ok := wf.runtime.entryPoints["main"]
	assert.True(t, ok)
	_, ok1 := wf.runtime.entryPoints["data-preprocess"]
	assert.True(t, ok1)
	_, ok2 := wf.runtime.entryPoints["validate"]
	assert.False(t, ok2)
}

func TestValidateWorkflow_WrongParam(t *testing.T) {
	testCase := loadcase(runWrongParamYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "invalid reference param {{ data-preprocess.xxxinvalid }} in step[main]: parameter[xxxinvalid] not exist", err.Error())

	// parameter 在 step [main] 中有错误，然而在将 entry 设置成 [data-preprocess] 后， validate 仍然返回成功
	bwf = NewBaseWorkflow(wfs, "", "data-preprocess", nil, extra)
	err = bwf.validate()
	assert.Nil(t, err)
}

func TestWorkflowParamDuplicate(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.Nil(t, err)

	bwf.Source.EntryPoints["main"].Parameters["train_data"] = "whatever"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "inputAtf name[train_data] has already existed in params/artifacts of step[main] (these names are case-insensitive)", err.Error())

	delete(bwf.Source.EntryPoints["main"].Parameters, "train_data") // 把上面的添加的删掉，再校验一遍
	err = bwf.validate()
	assert.Nil(t, err)

	bwf.Source.EntryPoints["main"].Parameters["train_model"] = "whatever"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "outputAtf name[train_model] has already existed in params/artifacts of step[main] (these names are case-insensitive)", err.Error())

	delete(bwf.Source.EntryPoints["main"].Parameters, "train_model") // 把上面的添加的删掉，再校验一遍
	err = bwf.validate()
	assert.Nil(t, err)

	bwf.Source.EntryPoints["main"].Artifacts.Input["train_model"] = "whatever"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "outputAtf name[train_model] has already existed in params/artifacts of step[main] (these names are case-insensitive)", err.Error())

	delete(bwf.Source.EntryPoints["main"].Artifacts.Input, "train_model") // 把上面的添加的删掉，再校验一遍
	err = bwf.validate()
	assert.Nil(t, err)
}

func TestValidateWorkflowParam(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.Nil(t, err)
	assert.Equal(t, bwf.Source.EntryPoints["validate"].Parameters["refSystem"].(string), "{{ PF_RUN_ID }}")

	bwf.Source.EntryPoints["validate"].Parameters["refSystem"] = "{{ xxx }}"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "unsupported SysParamName[xxx] for param[{{ xxx }}] of filedType[parameters]")

	bwf.Source.EntryPoints["validate"].Parameters["refSystem"] = "{{ PF_RUN_ID }}"
	err = bwf.validate()
	assert.Nil(t, err)

	// ref from downstream
	bwf.Source.EntryPoints["main"].Parameters["invalidRef"] = "{{ validate.refSystem }}"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "invalid reference param {{ validate.refSystem }} in step[main]: step[validate] not in deps")

	// ref from downstream
	bwf.Source.EntryPoints["main"].Parameters["invalidRef"] = "{{ .refSystem }}"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "unsupported SysParamName[refSystem] for param[{{ .refSystem }}] of filedType[parameters]")

	// validate param name
	bwf.Source.EntryPoints["main"].Parameters["invalidRef"] = "111" // 把上面的，改成正确的
	bwf.Source.EntryPoints["main"].Parameters["invalid-name"] = "xxx"
	err = bwf.validate()
	assert.NotNil(t, err)
	errMsg := "check parameters[invalid-name] in step[main] failed: format of variable name[invalid-name] invalid, should be in ^[A-Za-z_][A-Za-z0-9_]{1,49}$"
	assert.Equal(t, err.Error(), errMsg)

	// validate param name
	delete(bwf.Source.EntryPoints["main"].Parameters, "invalid-name") // 把上面的添加的删掉
	bwf.Source.EntryPoints["main"].Env["invalid-name"] = "xxx"
	err = bwf.validate()
	assert.NotNil(t, err)
	errMsg = "check env[invalid-name] in step[main] failed: format of variable name[invalid-name] invalid, should be in ^[A-Za-z_][A-Za-z0-9_]{1,49}$"
	assert.Equal(t, err.Error(), errMsg)
}

func TestValidateWorkflow__DictParam(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.Nil(t, err)
	// assert.Equal(t, "dictparam", bwf.Source.EntryPoints["main"].Parameters["p3"])
	// assert.Equal(t, 0.66, bwf.Source.EntryPoints["main"].Parameters["p4"])
	// assert.Equal(t, "/path/to/anywhere", bwf.Source.EntryPoints["main"].Parameters["p5"])

	// 缺 default 值
	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": "path", "default": ""}
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "invalid value[] in dict param[name: dict, value: {Type:path Default:}]", err.Error())

	// validate dict param
	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"kkk": 0.32}
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "type[] is not supported for dict param[name: dict, value: {Type: Default:<nil>}]")

	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"kkk": "111"}
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "type[] is not supported for dict param[name: dict, value: {Type: Default:<nil>}]")

	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": "kkk"}
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "invalid value[<nil>] in dict param[name: dict, value: {Type:kkk Default:<nil>}]")

	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": "float"}
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "invalid value[<nil>] in dict param[name: dict, value: {Type:float Default:<nil>}]")

	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": "float", "default": "kkk"}
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, InvalidParamTypeError("kkk", "float").Error(), err.Error())

	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": "float", "default": 111}
	err = bwf.validate()
	assert.Nil(t, err)
	// assert.Equal(t, 111, bwf.Source.EntryPoints["main"].Parameters["dict"])

	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": "string", "default": "111"}
	err = bwf.validate()
	assert.Nil(t, err)
	// assert.Equal(t, "111", bwf.Source.EntryPoints["main"].Parameters["dict"])

	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": "path", "default": "111"}
	err = bwf.validate()
	assert.Nil(t, err)
	// assert.Equal(t, "111", bwf.Source.EntryPoints["main"].Parameters["dict"])

	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": "path", "default": "/111"}
	err = bwf.validate()
	assert.Nil(t, err)
	// assert.Equal(t, "/111", bwf.Source.EntryPoints["main"].Parameters["dict"])

	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": "path", "default": "/111 / "}
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "invalid path value[/111 / ] in parameter[dict]")

	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": "path", "default": "/111-1/111_2"}
	err = bwf.validate()
	assert.Nil(t, err)
	// assert.Equal(t, "/111-1/111_2", bwf.Source.EntryPoints["main"].Parameters["dict"])

	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": "float", "default": 111}
	err = bwf.validate()
	assert.Nil(t, err)
	// assert.Equal(t, 111, bwf.Source.EntryPoints["main"].Parameters["dict"])

	// invalid actual interface type
	mapParam := map[string]string{"ffff": "2"}
	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": "float", "default": mapParam}
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, InvalidParamTypeError(mapParam, "float").Error(), err.Error())

	bwf.Source.EntryPoints["main"].Parameters["dict"] = map[interface{}]interface{}{"type": map[string]string{"ffff": "2"}, "default": "111"}
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "invalid dict parameter[map[default:111 type:map[ffff:2]]]", err.Error())

	// unsupported type
	param := map[interface{}]interface{}{"type": "unsupportedType", "default": "111"}
	bwf.Source.EntryPoints["main"].Parameters["dict"] = param
	err = bwf.validate()
	assert.NotNil(t, err)
	dictParam := DictParam{}
	decodeErr := dictParam.From(param)
	assert.Nil(t, decodeErr)
	assert.Equal(t, UnsupportedDictParamTypeError("unsupportedType", "dict", dictParam).Error(), err.Error())
}

// 校验初始化workflow时，传递param参数
func TestValidateWorkflowPassingParam(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	// not exist in source yaml
	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	bwf.Params = map[string]interface{}{
		"p1": "correct",
	}
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "param[p1] not exist", err.Error())

	bwf.Params = map[string]interface{}{
		"model": "correct",
	}
	err = bwf.validate()
	assert.Nil(t, err)
	assert.Equal(t, "correct", bwf.Source.EntryPoints["main"].Parameters["model"])

	bwf.Source.EntryPoints["main"].Parameters["model"] = "xxxxx"
	bwf.Params = map[string]interface{}{
		"main.model": "correct",
	}
	err = bwf.validate()
	assert.Nil(t, err)
	assert.Equal(t, "correct", bwf.Source.EntryPoints["main"].Parameters["model"])

	bwf.Params = map[string]interface{}{
		"model": "{{ PF_RUN_ID }}",
	}
	err = bwf.validate()
	assert.Nil(t, err)

	bwf.Params = map[string]interface{}{
		"model": "{{ xxx }}",
	}
	err = bwf.validate()
	assert.Equal(t, "unsupported SysParamName[xxx] for param[{{ xxx }}] of filedType[parameters]", err.Error())

	bwf.Params = map[string]interface{}{
		"model": "{{ step1.param }}",
	}
	err = bwf.validate()
	assert.Equal(t, "invalid reference param {{ step1.param }} in step[main]: step[step1] not in deps", err.Error())
}

func TestValidateWorkflowArtifacts(t *testing.T) {
	// 当前只会校验input artifact的值，output artifact的只不会校验，因为替换的时候，会直接用系统生成的路径覆盖原来的值
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.Nil(t, err)
	assert.Equal(t, "{{ data-preprocess.train_data }}", bwf.Source.EntryPoints["main"].Artifacts.Input["train_data"])

	// input artifact 只能引用上游 output artifact
	bwf.Source.EntryPoints["main"].Artifacts.Input["wrongdata"] = "{{ xxxx }}"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "check input artifact [wrongdata] in step[main] failed: format of value[{{ xxxx }}] invalid, should be like {{XXX.XXX}}", err.Error())

	// 上游 output artifact 不存在
	bwf.Source.EntryPoints["main"].Artifacts.Input["wrongdata"] = "{{ data-preprocess.noexist_data }}"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "invalid reference param {{ data-preprocess.noexist_data }} in step[main]: output artifact[noexist_data] not exist", err.Error())
	delete(bwf.Source.EntryPoints["main"].Artifacts.Input, "wrongdata")
}

func TestValidateWorkflowDisabled(t *testing.T) {
	// 校验workflow disable设置
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.Nil(t, err)
	assert.Equal(t, "", bwf.Source.Disabled)

	// disabled 步骤不存在，校验失败
	bwf.Source.Disabled = "notExistStepName"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "disabled step[notExistStepName] not existed!", err.Error())

	// disabled 步骤重复设定
	bwf.Source.Disabled = "validate,validate"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "disabled step[validate] is set repeatedly!", err.Error())

	// disabled 设置成功
	bwf.Source.Disabled = "validate"
	err = bwf.validate()
	assert.Nil(t, err)

	// disabled 设置失败，步骤输出artifact被下游节点依赖
	bwf.Source.Disabled = "main"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "invalid reference param {{ main.train_model }} in step[validate]: step[main] is disabled", err.Error())
	delete(bwf.Source.EntryPoints["main"].Artifacts.Input, "wrongdata")
}

func TestValidateWorkflowCache(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	// 校验节点cache配置为空时，能够使用全局配置替换
	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.Nil(t, err)
	assert.Equal(t, bwf.Source.Cache.Enable, false)
	assert.Equal(t, bwf.Source.Cache.MaxExpiredTime, "400")
	assert.Equal(t, bwf.Source.Cache.FsScope, "/path/to/run,/path/to/run2")

	assert.Equal(t, bwf.Source.EntryPoints["data-preprocess"].Cache.Enable, bwf.Source.Cache.Enable)
	assert.Equal(t, bwf.Source.EntryPoints["data-preprocess"].Cache.MaxExpiredTime, bwf.Source.Cache.MaxExpiredTime)
	assert.Equal(t, bwf.Source.EntryPoints["data-preprocess"].Cache.FsScope, bwf.Source.Cache.FsScope)

	// 全局 + 节点的cache MaxExpiredTime 设置失败
	bwf.Source.Cache.MaxExpiredTime = ""
	bwf.Source.EntryPoints["data-preprocess"].Cache.MaxExpiredTime = ""
	err = bwf.validate()
	assert.Nil(t, err)
	assert.Equal(t, "-1", bwf.Source.Cache.MaxExpiredTime)
	assert.Equal(t, "-1", bwf.Source.EntryPoints["data-preprocess"].Cache.MaxExpiredTime)

	// 全局cache MaxExpiredTime 设置失败
	bwf.Source.Cache.MaxExpiredTime = "notInt"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "MaxExpiredTime[notInt] of cache not correct", err.Error())

	// 节点cache MaxExpiredTime 设置失败
	bwf.Source.Cache.MaxExpiredTime = ""
	bwf.Source.EntryPoints["data-preprocess"].Cache.MaxExpiredTime = "notInt"
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "MaxExpiredTime[notInt] of cache in step[data-preprocess] not correct", err.Error())
}

// 测试不使用Fs时，workflow校验逻辑
func TestValidateWorkflowWithoutFs(t *testing.T) {
	testCase := loadcase(noAtfYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	// 校验 WfExtraInfoKeyFsID 和 WfExtraInfoKeyFsName，不能同时为空字符串
	extra := GetExtra()
	extra[pplcommon.WfExtraInfoKeyFsID] = ""
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "check extra failed: FsID[] and FsName[mockFsname] can only both be empty or unempty", err.Error())

	// 校验不使用Fs时，不能使用fs相关的系统参数
	extra[pplcommon.WfExtraInfoKeyFsName] = ""
	wfs.EntryPoints["data-preprocess"].Parameters["wrongParam"] = "{{ PF_FS_ID }}"
	bwf = NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "cannot use sysParam[PF_FS_ID] template in step[data-preprocess] for pipeline run with no Fs mounted", err.Error())

	wfs.EntryPoints["data-preprocess"].Parameters["wrongParam"] = "{{ PF_FS_NAME }}"
	bwf = NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "cannot use sysParam[PF_FS_NAME] template in step[data-preprocess] for pipeline run with no Fs mounted", err.Error())

	// 校验不使用Fs时，全局cache中的fs_scope字段一定为空
	delete(wfs.EntryPoints["data-preprocess"].Parameters, "wrongParam")
	bwf = NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "fs_scope of global cache should be empty if Fs is not used!", err.Error())

	// 校验不使用Fs时，节点cache中的fs_scope字段一定为空
	// 下面case会报错，是因为parse workflow的时候，会将global cache配置，作为每个节点的cache默认配置
	// 所以只修改global cache配置的fs_scope并不够
	bwf = NewBaseWorkflow(wfs, "", "", nil, extra)
	bwf.Source.Cache.FsScope = ""
	err = bwf.validate()
	assert.NotNil(t, err)
	pattern := regexp.MustCompile("fs_scope of cache in step[[a-zA-Z-]+] should be empty if Fs is not used!")
	assert.Regexp(t, pattern, err.Error())

	bwf.Source.EntryPoints["data-preprocess"].Cache.FsScope = ""
	bwf.Source.EntryPoints["main"].Cache.FsScope = ""
	bwf.Source.EntryPoints["data-preprocess"].Cache.FsScope = ""
	bwf.Source.EntryPoints["validate"].Cache.FsScope = ""
	err = bwf.validate()
	assert.Nil(t, err)

	// 校验不使用Fs时，不能定义artifact
	// 因为input artifact一定引用上游的outputAtf，所以只需要测试没法定义outputAtf即可
	wfs.EntryPoints["data-preprocess"].Artifacts.Output["Atf1"] = ""
	bwf = NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.NotNil(t, err)
	pattern = regexp.MustCompile("cannot define artifact in step[[a-zA-Z-]+] with no Fs mounted")
	assert.Regexp(t, pattern, err.Error())
}

func TestRestartWorkflow(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	runtimeView := schema.RuntimeView{
		"data-preprocess": schema.JobView{
			JobID:  "data-preprocess",
			Status: schema.StatusJobSucceeded,
		},
		"main": {
			JobID:  "data-preprocess",
			Status: schema.StatusJobRunning,
		},
		"validate": {
			JobID: "",
		},
	}
	postProcessView := map[string]schema.JobView{}

	err = wf.SetWorkflowRuntime(runtimeView, postProcessView)

	assert.Nil(t, err)
	assert.Equal(t, true, wf.runtime.entryPoints["data-preprocess"].done)
	assert.Equal(t, true, wf.runtime.entryPoints["data-preprocess"].submitted)
	assert.Equal(t, false, wf.runtime.entryPoints["main"].done)
	assert.Equal(t, true, wf.runtime.entryPoints["main"].submitted)
	assert.Equal(t, false, wf.runtime.entryPoints["validate"].done)
	assert.Equal(t, false, wf.runtime.entryPoints["validate"].submitted)
}

func TestRestartWorkflow_from1completed(t *testing.T) {
	driver.InitMockDB()
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	runtimeView := schema.RuntimeView{
		"data-preprocess": schema.JobView{
			JobID:  "data-preprocess",
			Status: schema.StatusJobSucceeded,
		},
		"main": {
			JobID: "",
		},
		"validate": {
			JobID: "",
		},
	}

	postProcessView := map[string]schema.JobView{}

	err = wf.SetWorkflowRuntime(runtimeView, postProcessView)

	assert.Nil(t, err)
	assert.Equal(t, true, wf.runtime.entryPoints["data-preprocess"].done)
	assert.Equal(t, true, wf.runtime.entryPoints["data-preprocess"].submitted)
	assert.Equal(t, false, wf.runtime.entryPoints["main"].done)
	assert.Equal(t, false, wf.runtime.entryPoints["main"].submitted)
	assert.Equal(t, false, wf.runtime.entryPoints["validate"].done)
	assert.Equal(t, false, wf.runtime.entryPoints["validate"].submitted)
}

func TestCheckPostProcess(t *testing.T) {
	driver.InitMockDB()
	wfs, err := loadTwoPostCaseSource()
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", "", nil, extra)
	err = bwf.validate()
	assert.NotNil(t, err)
	assert.Equal(t, "post_process can only has 1 step at most", err.Error())

	yamlByte := loadcase(runYamlPath)
	wfs, err = schema.ParseWorkflowSource(yamlByte)
	assert.Nil(t, err)

	extra = GetExtra()
	_, err = NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)
}
