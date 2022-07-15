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
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	pplcommon "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

func loadTwoPostCaseSource() (schema.WorkflowSource, error) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	if err != nil {
		return schema.WorkflowSource{}, err
	}
	postStep := schema.WorkflowSourceStep{
		Command: "echo test",
	}
	wfs.PostProcess["mail2"] = &postStep
	return wfs, nil
}

func NewMockWorkflow(wfSource schema.WorkflowSource, runID string, params map[string]interface{}, extra map[string]string,
	callbacks WorkflowCallbacks) (*Workflow, error) {
	// bwfTemp := &BaseWorkflow{}
	// p1 := gomonkey.ApplyPrivateMethod(reflect.TypeOf(bwfTemp), "checkFs", func() error {
	// 	return nil
	// })
	// defer p1.Reset()
	return NewWorkflow(wfSource, runID, params, extra, callbacks)
}

func mockValidate(bwf *BaseWorkflow) error {
	// bwfTemp := &BaseWorkflow{}
	// p1 := gomonkey.ApplyPrivateMethod(reflect.TypeOf(bwfTemp), "checkFs", func() error {
	// 	return nil
	// })
	// defer p1.Reset()
	return bwf.validate()
}

// 测试NewBaseWorkflow, 只传yaml内容
func TestNewBaseWorkflowByOnlyRunYaml(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	if err := mockValidate(&bwf); err != nil {
		t.Errorf("validate failed. error: %v", err)
	}
}

// 测试带环流程
func TestNewBaseWorkflowWithCircle(t *testing.T) {
	testCase := loadcase(runCircleYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	_, err = bwf.topologicalSort(bwf.Source.EntryPoints.EntryPoints)
	assert.NotNil(t, err)
}

// 测试无环流程
func TestTopologicalSort_noCircle(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	fmt.Println(bwf.Source.EntryPoints)
	result, err := bwf.topologicalSort(bwf.Source.EntryPoints.EntryPoints)
	assert.Nil(t, err)
	assert.Equal(t, "data-preprocess", result[0])
	assert.Equal(t, "main", result[1])
	assert.Equal(t, "validate", result[2])
}

func TestValidateWorkflow_WrongParam(t *testing.T) {
	testCase := loadcase(runWrongParamYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid reference param {{ data-preprocess.xxxinvalid }} in component[main]: parameter[xxxinvalid] not exist", err.Error())
}

func TestWorkflowParamDuplicate(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	err = mockValidate(&bwf)
	assert.Nil(t, err)

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["train_data"] = "whatever"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "inputAtf name[train_data] has already existed in params/artifacts of component[main] (these names are case-insensitive)", err.Error())

	delete(bwf.Source.EntryPoints.EntryPoints["main"].GetParameters(), "train_data") // 把上面的添加的删掉，再校验一遍
	err = mockValidate(&bwf)
	assert.Nil(t, err)

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["train_model"] = "whatever"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "outputAtf name[train_model] has already existed in params/artifacts of component[main] (these names are case-insensitive)", err.Error())

	delete(bwf.Source.EntryPoints.EntryPoints["main"].GetParameters(), "train_model") // 把上面的添加的删掉，再校验一遍
	err = mockValidate(&bwf)
	assert.Nil(t, err)

	bwf.Source.EntryPoints.EntryPoints["main"].GetArtifacts().Input["train_model"] = "whatever"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "outputAtf name[train_model] has already existed in params/artifacts of component[main] (these names are case-insensitive)", err.Error())

	delete(bwf.Source.EntryPoints.EntryPoints["main"].GetArtifacts().Input, "train_model") // 把上面的添加的删掉，再校验一遍
	err = mockValidate(&bwf)
	assert.Nil(t, err)
}

func TestValidateWorkflowParam(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	assert.Equal(t, bwf.Source.EntryPoints.EntryPoints["validate"].GetParameters()["refSystem"].(string), "{{ PF_RUN_ID }}")

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["refSystem"] = "{{ xxx }}"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "unsupported SysParamName[xxx] for param[{{ xxx }}] of filedType[parameters]")

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["refSystem"] = "{{ PF_RUN_ID }}"
	err = mockValidate(&bwf)
	assert.Nil(t, err)

	// ref from downstream
	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["invalidRef"] = "{{ validate.refSystem }}"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "invalid reference param {{ validate.refSystem }} in component[main]: component[validate] not in deps")

	// ref from downstream
	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["invalidRef"] = "{{ .refSystem }}"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "unsupported SysParamName[refSystem] for param[{{ .refSystem }}] of filedType[parameters]")

	// validate param name
	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["invalidRef"] = "111" // 把上面的，改成正确的
	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["invalid-name"] = "xxx"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	errMsg := "check parameters[invalid-name] in component[main] failed: format of variable name[invalid-name] invalid, should be in ^[A-Za-z_][A-Za-z0-9_]{1,49}$"
	assert.Equal(t, err.Error(), errMsg)

	// validate param name
	delete(bwf.Source.EntryPoints.EntryPoints["main"].(*schema.WorkflowSourceStep).Parameters, "invalid-name") // 把上面的添加的删掉
	bwf.Source.EntryPoints.EntryPoints["main"].(*schema.WorkflowSourceStep).Env["invalid-name"] = "xxx"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	errMsg = "check env[invalid-name] in component[main] failed: format of variable name[invalid-name] invalid, should be in ^[A-Za-z_][A-Za-z0-9_]{1,49}$"
	assert.Equal(t, err.Error(), errMsg)
}

func TestValidateWorkflow__DictParam(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	// assert.Equal(t, "dictparam", bwf.Source.EntryPoints["main"].Parameters["p3"])
	// assert.Equal(t, 0.66, bwf.Source.EntryPoints["main"].Parameters["p4"])
	// assert.Equal(t, "/path/to/anywhere", bwf.Source.EntryPoints["main"].Parameters["p5"])

	// 缺 default 值
	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": "path", "default": ""}
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid value[] in dict param[name: dict, value: {Type:path Default:}]", err.Error())

	// validate dict param
	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"kkk": 0.32}
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "type[] is not supported for dict param[name: dict, value: {Type: Default:<nil>}]")

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"kkk": "111"}
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "type[] is not supported for dict param[name: dict, value: {Type: Default:<nil>}]")

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": "kkk"}
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "invalid value[<nil>] in dict param[name: dict, value: {Type:kkk Default:<nil>}]")

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": "float"}
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "invalid value[<nil>] in dict param[name: dict, value: {Type:float Default:<nil>}]")

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": "float", "default": "kkk"}
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, pplcommon.InvalidParamTypeError("kkk", "float").Error(), err.Error())

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": "float", "default": 111}
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	// assert.Equal(t, 111, bwf.Source.EntryPoints["main"].Parameters["dict"])

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": "string", "default": "111"}
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	// assert.Equal(t, "111", bwf.Source.EntryPoints["main"].Parameters["dict"])

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": "path", "default": "111"}
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	// assert.Equal(t, "111", bwf.Source.EntryPoints["main"].Parameters["dict"])

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": "path", "default": "/111"}
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	// assert.Equal(t, "/111", bwf.Source.EntryPoints["main"].Parameters["dict"])

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": "path", "default": "/111 / "}
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "invalid path value[/111 / ] in parameter[dict]")

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": "path", "default": "/111-1/111_2"}
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	// assert.Equal(t, "/111-1/111_2", bwf.Source.EntryPoints["main"].Parameters["dict"])

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": "float", "default": 111}
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	// assert.Equal(t, 111, bwf.Source.EntryPoints["main"].Parameters["dict"])

	// invalid actual interface type
	mapParam := map[string]string{"ffff": "2"}
	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": "float", "default": mapParam}
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, pplcommon.InvalidParamTypeError(mapParam, "float").Error(), err.Error())

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = map[string]interface{}{"type": map[string]string{"ffff": "2"}, "default": "111"}
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid dict parameter[map[default:111 type:map[ffff:2]]]", err.Error())

	// unsupported type
	param := map[string]interface{}{"type": "unsupportedType", "default": "111"}
	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["dict"] = param
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	dictParam := pplcommon.DictParam{}
	decodeErr := dictParam.From(param)
	assert.Nil(t, decodeErr)
	assert.Equal(t, pplcommon.UnsupportedDictParamTypeError("unsupportedType", "dict", dictParam).Error(), err.Error())
}

// 校验初始化workflow时，传递param参数
func TestValidateWorkflowPassingParam(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	// not exist in source yaml
	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	bwf.Params = map[string]interface{}{
		"p1": "correct",
	}
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "param[p1] not exist", err.Error())

	bwf.Params = map[string]interface{}{
		"model": "correct",
	}
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	assert.Equal(t, "correct", bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["model"])

	bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["modelPath"] = "xxxxx"
	bwf.Params = map[string]interface{}{
		"main.model": "correct",
	}
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	assert.Equal(t, "correct", bwf.Source.EntryPoints.EntryPoints["main"].GetParameters()["model"])

	bwf.Params = map[string]interface{}{
		"model": "{{ PF_RUN_ID }}",
	}
	err = mockValidate(&bwf)
	assert.Nil(t, err)

	bwf.Params = map[string]interface{}{
		"model": "{{ xxx }}",
	}
	err = mockValidate(&bwf)
	assert.Equal(t, "unsupported SysParamName[xxx] for param[{{ xxx }}] of filedType[parameters]", err.Error())

	bwf.Params = map[string]interface{}{
		"model": "{{ step1.param }}",
	}
	err = mockValidate(&bwf)
	assert.Equal(t, "invalid reference param {{ step1.param }} in component[main]: component[step1] not in deps", err.Error())
}

func TestValidateWorkflowArtifacts(t *testing.T) {
	// 当前只会校验input artifact的值，output artifact的只不会校验，因为替换的时候，会直接用系统生成的路径覆盖原来的值
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	assert.Equal(t, "{{ data-preprocess.train_data }}", bwf.Source.EntryPoints.EntryPoints["main"].GetArtifacts().Input["train_data"])

	// input artifact 只能引用上游 output artifact
	bwf.Source.EntryPoints.EntryPoints["main"].GetArtifacts().Input["wrongdata"] = "{{ xxxx }}"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "check input artifact [wrongdata] in component[main] failed: format of value[{{ xxxx }}] invalid, should be like {{XX-XX.XX_XX}}", err.Error())

	// 上游 output artifact 不存在
	bwf.Source.EntryPoints.EntryPoints["main"].GetArtifacts().Input["wrongdata"] = "{{ data-preprocess.noexist_data }}"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid reference param {{ data-preprocess.noexist_data }} in component[main]: output artifact[noexist_data] not exist", err.Error())
	delete(bwf.Source.EntryPoints.EntryPoints["main"].GetArtifacts().Input, "wrongdata")
}

func TestValidateWorkflowDisabled(t *testing.T) {
	// 校验workflow disable设置
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	assert.Equal(t, "", bwf.Source.Disabled)

	// disabled 步骤不存在，校验失败
	bwf.Source.Disabled = "notExistStepName"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "disabled component[notExistStepName] not existed!", err.Error())

	// disabled 步骤重复设定
	bwf.Source.Disabled = "validate,validate"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "disabled component[validate] is set repeatedly!", err.Error())

	// disabled 设置成功
	bwf.Source.Disabled = "validate"
	err = mockValidate(&bwf)
	assert.Nil(t, err)

	// disabled 设置失败，步骤输出artifact被下游节点依赖
	bwf.Source.Disabled = "main"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "disabled component[main] is refered by [validate]", err.Error())
	delete(bwf.Source.EntryPoints.EntryPoints["main"].GetArtifacts().Input, "wrongdata")
}

func TestValidateWorkflowCache(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	// 校验节点cache配置为空时，能够使用全局配置替换
	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	assert.Equal(t, bwf.Source.Cache.Enable, false)
	assert.Equal(t, bwf.Source.Cache.MaxExpiredTime, "400")
	assert.Equal(t, len(bwf.Source.Cache.FsScope), 1)
	assert.Equal(t, bwf.Source.Cache.FsScope[0].FsName, "xd")

	assert.Equal(t, bwf.Source.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep).Cache.Enable, bwf.Source.Cache.Enable)
	assert.Equal(t, bwf.Source.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep).Cache.MaxExpiredTime, bwf.Source.Cache.MaxExpiredTime)
	assert.Equal(t, bwf.Source.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep).Cache.FsScope, bwf.Source.Cache.FsScope)

	// 全局 + 节点的cache MaxExpiredTime 设置失败
	bwf.Source.Cache.MaxExpiredTime = ""
	bwf.Source.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep).Cache.MaxExpiredTime = ""
	err = mockValidate(&bwf)
	assert.Nil(t, err)
	assert.Equal(t, "-1", bwf.Source.Cache.MaxExpiredTime)
	assert.Equal(t, "-1", bwf.Source.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep).Cache.MaxExpiredTime)

	// 全局cache MaxExpiredTime 设置失败
	bwf.Source.Cache.MaxExpiredTime = "notInt"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "MaxExpiredTime[notInt] of cache not correct", err.Error())

	// 节点cache MaxExpiredTime 设置失败
	bwf.Source.Cache.MaxExpiredTime = ""
	bwf.Source.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep).Cache.MaxExpiredTime = "notInt"
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "MaxExpiredTime[notInt] of cache in step[data-preprocess] not correct", err.Error())
}

// 测试不使用Fs时，workflow校验逻辑
func TestValidateWorkflowWithoutFs(t *testing.T) {
	testCase := loadcase(noAtfYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	// 校验 WfExtraInfoKeyFsID 和 WfExtraInfoKeyFsName，不能同时为空字符串
	extra := GetExtra()
	extra[pplcommon.WfExtraInfoKeyFsID] = ""
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "check extra failed: FsID[] and FsName[mockFs] can only both be empty or unempty", err.Error())

	// 校验不使用Fs时，不能使用fs相关的系统参数
	extra[pplcommon.WfExtraInfoKeyFsName] = ""
	wfs.EntryPoints.EntryPoints["data-preprocess"].GetParameters()["wrongParam"] = "{{ PF_FS_ID }}"
	bwf = NewBaseWorkflow(wfs, "", nil, extra)
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "unsupported SysParamName[PF_FS_ID] for param[{{ PF_FS_ID }}] of filedType[parameters]", err.Error())

	wfs.EntryPoints.EntryPoints["data-preprocess"].GetParameters()["wrongParam"] = "{{ PF_FS_NAME }}"
	bwf = NewBaseWorkflow(wfs, "", nil, extra)
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "unsupported SysParamName[PF_FS_NAME] for param[{{ PF_FS_NAME }}] of filedType[parameters]", err.Error())
	delete(wfs.EntryPoints.EntryPoints["data-preprocess"].GetParameters(), "wrongParam")

	// 校验不使用Fs时，不能定义artifact
	// 因为input artifact一定引用上游的outputAtf，所以只需要测试没法定义outputAtf即可
	wfs.EntryPoints.EntryPoints["data-preprocess"].GetArtifacts().Output["Atf1"] = ""
	bwf = NewBaseWorkflow(wfs, "", nil, extra)
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	pattern := regexp.MustCompile("cannot define artifact in component[[a-zA-Z-]+] with no Fs mounted")
	assert.Regexp(t, pattern, err.Error())
}

func TestCheckPostProcess(t *testing.T) {
	wfs, err := loadTwoPostCaseSource()
	assert.Nil(t, err)

	extra := GetExtra()
	bwf := NewBaseWorkflow(wfs, "", nil, extra)
	err = mockValidate(&bwf)
	assert.NotNil(t, err)
	assert.Equal(t, "post_process can only has 1 step at most", err.Error())

	yamlByte := loadcase(runYamlPath)
	wfs, err = schema.GetWorkflowSource(yamlByte)
	assert.Nil(t, err)

	extra = GetExtra()
	_, err = NewMockWorkflow(wfs, "", nil, extra, mockCbs)
	assert.Nil(t, err)
}

func TestFsOptions(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	assert.Equal(t, len(wfs.FsOptions.FsMount), 1)
	assert.Equal(t, wfs.FsOptions.FsMount[0].FsName, "abc")
	assert.Equal(t, wfs.EntryPoints.EntryPoints["main"].(*schema.WorkflowSourceStep).FsMount[0].FsName, "abc")
	assert.Equal(t, wfs.EntryPoints.EntryPoints["main"].(*schema.WorkflowSourceStep).Cache.FsScope[0].FsName, "xd")
}
