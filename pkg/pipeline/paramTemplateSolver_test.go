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
	"reflect"
	"strings"
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/stretchr/testify/assert"
)

// ============= 测试 innersolver
func mockComponentForInnerSolver() *schema.WorkflowSourceStep {
	return &schema.WorkflowSourceStep{
		Name: "step1",
		Parameters: map[string]interface{}{
			"p1": 1,
			"p2": []int{1, 2, 3, 4},
			"p3": "abcdefg",
			"p4": "[1, 2, 3, 4]",
			"p5": "[1, 2, \"D3\", 4]",
		},
		Artifacts: schema.Artifacts{
			Input: map[string]string{
				"in1": "./a.txt",
				"in2": "./b.txt",
			},
			Output: map[string]string{
				"out1": "out1.txt",
			},
		},
		Command:      "echo {{p1}} && cat {{in2}} >> {{out1}} && echo {{PF_RUN_ID}} ",
		Condition:    "{{p1}} > 10",
		LoopArgument: "{{p2}}",
		Env: map[string]string{
			"e1": "{{p1}}",
			"e2": "0_{{p1}}_{{p3}}_4",
			"e3": "e3_{{PF_RUN_ID}}_{{p2}}",
		},
	}
}

func mockArtInJsonFormat(artPath string) {
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	handler.NewFsHandlerWithServer("xx", logger.LoggerForRun("innersolve"))

	a := []int{10, 12, 13, 14}
	s, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}

	testFsMeta := common.FSMeta{
		UfsType: common.LocalType,
		SubPath: "./mock_fs_handler",
	}
	fsClient, err := fs.NewFSClientForTest(testFsMeta)

	writerCloser, err := fsClient.Create(artPath)
	if err != nil {
		panic(err)
	}

	defer writerCloser.Close()

	_, err = writerCloser.Write(s)
	if err != nil {
		panic(err)
	}

}
func TestResolveLoopArgument(t *testing.T) {

	component := mockComponentForInnerSolver()
	rc := runConfig{
		mainFS: &schema.FsMount{ID: "xx"},
		logger: logger.LoggerForRun("innersolver"),
	}
	is := NewInnerSolver(component, "step1", &rc)

	err := is.resolveLoopArugment()
	assert.Nil(t, err)

	typeInfo := reflect.TypeOf(component.GetLoopArgument())
	isSplice := strings.HasPrefix(typeInfo.String(), "[]")
	assert.True(t, isSplice)

	component = mockComponentForInnerSolver()
	component.UpdateLoopArguemt("{{step1.p4}}")
	is = NewInnerSolver(component, "step1", &runConfig{logger: logger.LoggerForRun("NewInnerSolver")})
	err = is.resolveLoopArugment()
	assert.Nil(t, err)

	typeInfo = reflect.TypeOf(component.GetLoopArgument())
	isSplice = strings.HasPrefix(typeInfo.String(), "[]")
	assert.True(t, isSplice)

	component = mockComponentForInnerSolver()
	component.UpdateLoopArguemt("{{p5}}")
	is = NewInnerSolver(component, "step1", &runConfig{logger: logger.LoggerForRun("NewInnerSolver")})
	err = is.resolveLoopArugment()
	assert.Nil(t, err)

	typeInfo = reflect.TypeOf(component.GetLoopArgument())
	isSplice = strings.HasPrefix(typeInfo.String(), "[]")
	assert.True(t, isSplice)

	component = mockComponentForInnerSolver()
	component.UpdateLoopArguemt("{{p1}}")
	is = NewInnerSolver(component, "step1", &runConfig{logger: logger.LoggerForRun("NewInnerSolver")})
	err = is.resolveLoopArugment()
	assert.NotNil(t, err)

	//  测试输入artifact
	mockArtInJsonFormat("./a.txt")

	component.UpdateLoopArguemt("{{in1}}")

	is = NewInnerSolver(component, "step1", &rc)
	err = is.resolveLoopArugment()
	assert.Nil(t, err)

	typeInfo = reflect.TypeOf(component.GetLoopArgument())
	isSplice = strings.HasPrefix(typeInfo.String(), "[]")
	assert.True(t, isSplice)

	loop_0 := component.GetLoopArgument().([]interface{})[0]
	loop0String := fmt.Sprintf("%v", loop_0)
	assert.Equal(t, loop0String, "10")
}

func TestResolveCondition(t *testing.T) {
	component := mockComponentForInnerSolver()
	is := NewInnerSolver(component, "step1", &runConfig{logger: logger.LoggerForRun("NewInnerSolver")})
	err := is.resolveCondition()

	assert.Nil(t, err)
	assert.Equal(t, "1 > 10", component.GetCondition())

	// 测试artifact
	mockArtInJsonFormat("./a.txt")

	component.UpdateCondition("b{{in1}}_{{p1}} == 10")
	rc := runConfig{
		mainFS: &schema.FsMount{ID: "xx"},
		logger: logger.LoggerForRun("innersolver"),
	}
	is = NewInnerSolver(component, "step1", &rc)
	err = is.resolveCondition()
	assert.Nil(t, err)

	assert.Equal(t, "b[10,12,13,14]_1 == 10", component.GetCondition())

	// 测试异常情况
	component.UpdateCondition("{{in5}} == 10")
	is = NewInnerSolver(component, "step1", &rc)
	err = is.resolveCondition()

	assert.NotNil(t, err)
}

func TestResolveCommand(t *testing.T) {
	component := mockComponentForInnerSolver()
	rc := runConfig{
		logger: logger.LoggerForRun("NewInnerSolver"),
		mainFS: &schema.FsMount{ID: "1234"},
	}
	is := NewInnerSolver(component, "step1", &rc)
	is.setSysParams(map[string]string{"PF_RUN_ID": "abc"})

	err := is.resolveCommand(true)
	assert.Nil(t, err)

	assert.Equal(t, component.Command, "echo 1 && cat /home/paddleflow/storage/mnt/1234/./b.txt >> {{out1}} && echo abc ")

	err = is.resolveCommand(false)
	assert.Nil(t, err)

	assert.Equal(t, component.Command,
		"echo 1 && cat /home/paddleflow/storage/mnt/1234/./b.txt >> /home/paddleflow/storage/mnt/1234/out1.txt && echo abc ")
}

func TestResolveEnv(t *testing.T) {
	component := mockComponentForInnerSolver()
	rc := runConfig{
		logger: logger.LoggerForRun("NewInnerSolver"),
		mainFS: &schema.FsMount{ID: "1234"},
	}
	is := NewInnerSolver(component, "step1", &rc)
	is.setSysParams(map[string]string{"PF_RUN_ID": "abc"})

	err := is.resolveEnv()
	assert.Nil(t, err)

	env := component.Env
	assert.Equal(t, "1", env["e1"])
	assert.Equal(t, "0_1_abcdefg_4", env["e2"])
	assert.Equal(t, "e3_abc_[1 2 3 4]", env["e3"])

	component.Env["e4"] = "{{in1}}"
	err = is.resolveEnv()
	assert.NotNil(t, err)
}

// 测试 dependenceResolver
func mockRunconfigForDepRsl() *runConfig {
	mainFS := schema.FsMount{
		ID:   "fs-xx",
		Name: "xx",
	}
	return &runConfig{
		logger:   logger.LoggerForRun("depResolve"),
		mainFS:   &mainFS,
		userName: "aa",
		runID:    "run-dep",
	}
}

func mockWorkflowDagForDs() *schema.WorkflowSourceDag {
	return &schema.WorkflowSourceDag{
		Name: "dag",
		Parameters: map[string]interface{}{
			"dp1": 0,
			"dp2": 1,
			"dp3": "dag1",
		},
		Artifacts: schema.Artifacts{
			Input: map[string]string{
				"di1": "./di1.txt",
				"di2": "./di2.txt",
			},
			Output: map[string]string{
				"do1": "{{step1.s1o1}}",
				"do2": "{{step2.s2o1}}",
			},
		},
		EntryPoints: map[string]schema.Component{
			"step1": &schema.WorkflowSourceStep{
				Parameters: map[string]interface{}{
					"s1p1": 10,
					"s1p2": "{{PF_PARENT.dp1}}_{{PF_PARENT.dp2}}",
					"s1p3": "st1_{{PF_RUN_ID}}",
					"s1p4": "{{PF_PARENT.PF_LOOP_ARGUMENT}}",
				},
				Artifacts: schema.Artifacts{
					Input: map[string]string{
						"s1i1": "{{PF_PARENT.di1}}",
					},
					Output: map[string]string{
						"s1o1": "./s1o1.txt",
					},
				},
			},
			"step2": &schema.WorkflowSourceStep{
				Parameters: map[string]interface{}{
					"s2p1": 12,
					"s2p2": "{{PF_PARENT.dp1}}_{{PF_PARENT.dp2}}",
					"s2p3": "{{step1.s1p1}}",
				},
				Artifacts: schema.Artifacts{
					Input: map[string]string{
						"s2i1": "{{PF_PARENT.di1}}",
						"s2i2": "{{step1.s1o1}}",
					},
					Output: map[string]string{
						"s2o1": "./s2o1.txt",
					},
				},
				Deps: "step1",
			},
		},
	}
}

func mockDagRuntime() *DagRuntime {
	bcr := baseComponentRuntime{
		runConfig:         mockRunconfigForDepRsl(),
		component:         mockWorkflowDagForDs(),
		componentFullName: "dag1",
	}

	return &DagRuntime{
		baseComponentRuntime: &bcr,
		subComponentRumtimes: make(map[string][]componentRuntime),
	}
}

func mockStepRuntime(step *schema.WorkflowSourceStep) *StepRuntime {
	bcr := baseComponentRuntime{
		runConfig:         mockRunconfigForDepRsl(),
		component:         step,
		componentFullName: "dag1.step1",
	}

	return &StepRuntime{
		baseComponentRuntime: &bcr,
	}
}

func TestResolveBeforeRun(t *testing.T) {
	dr := mockDagRuntime()
	dr.sysParams = map[string]string{"PF_LOOP_ARGUMENT": "10", "PF_RUN_ID": "run-001"}

	dr.getComponent().UpdateLoopArguemt([]int{11, 12})
	ds := NewDependencySolver(dr)

	// step1: 主要测试父子间传递
	err := ds.ResolveBeforeRun(dr.getworkflowSouceDag().EntryPoints["step1"])
	assert.Nil(t, err)

	params := dr.getworkflowSouceDag().EntryPoints["step1"].GetParameters()
	assert.Equal(t, 10, params["s1p1"])
	assert.Equal(t, "0_1", params["s1p2"])
	assert.Equal(t, "st1_run-001", params["s1p3"])
	assert.Equal(t, 11, params["s1p4"])

	inputs := dr.getworkflowSouceDag().EntryPoints["step1"].GetArtifacts().Input
	assert.Equal(t, "./di1.txt", inputs["s1i1"])

	// step2: 主要测试上下游传递
	stepRuntime1 := mockStepRuntime(dr.getworkflowSouceDag().EntryPoints["step1"].(*schema.WorkflowSourceStep))
	dr.subComponentRumtimes["step1"] = []componentRuntime{}
	dr.subComponentRumtimes["step1"] = append(dr.subComponentRumtimes["step1"], stepRuntime1)

	err = ds.ResolveBeforeRun(dr.getworkflowSouceDag().EntryPoints["step2"])
	assert.Nil(t, err)

	params = dr.getworkflowSouceDag().EntryPoints["step2"].GetParameters()
	assert.Equal(t, 12, params["s2p1"])
	assert.Equal(t, "0_1", params["s2p2"])
	assert.Equal(t, 10, params["s2p3"])

	inputs = dr.getworkflowSouceDag().EntryPoints["step2"].GetArtifacts().Input
	assert.Equal(t, "./di1.txt", inputs["s2i1"])
	assert.Equal(t, "./s1o1.txt", inputs["s2i2"])
}

func TestResolveAfterDone(t *testing.T) {
	dr := mockDagRuntime()
	dr.sysParams = map[string]string{"PF_LOOP_ARGUMENT": "10", "PF_RUN_ID": "run-001"}
	ds := NewDependencySolver(dr)

	err := ds.ResolveBeforeRun(dr.getworkflowSouceDag().EntryPoints["step1"])
	assert.Nil(t, err)

	stepRuntime1 := mockStepRuntime(dr.getworkflowSouceDag().EntryPoints["step1"].(*schema.WorkflowSourceStep))
	dr.subComponentRumtimes["step1"] = []componentRuntime{}
	dr.subComponentRumtimes["step1"] = append(dr.subComponentRumtimes["step1"], stepRuntime1)
	dr.subComponentRumtimes["step1"] = append(dr.subComponentRumtimes["step1"], stepRuntime1)

	err = ds.ResolveBeforeRun(dr.getworkflowSouceDag().EntryPoints["step2"])
	assert.Nil(t, err)

	stepRuntime2 := mockStepRuntime(dr.getworkflowSouceDag().EntryPoints["step2"].(*schema.WorkflowSourceStep))
	dr.subComponentRumtimes["step2"] = []componentRuntime{}
	dr.subComponentRumtimes["step2"] = append(dr.subComponentRumtimes["step2"], stepRuntime2)

	err = ds.ResolveAfterDone()
	assert.Nil(t, err)

	p, _ := dr.getworkflowSouceDag().GetArtifactPath("do1")
	assert.Equal(t, "./s1o1.txt,./s1o1.txt", p)

	p, _ = dr.getworkflowSouceDag().GetArtifactPath("do2")
	assert.Equal(t, "./s2o1.txt", p)
}
