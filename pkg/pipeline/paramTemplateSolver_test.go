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
	is := NewInnerSolver(component, "step1", &runConfig{fsID: "xx", logger: logger.LoggerForRun("innersolver")})

	fmt.Println(component.GetLoopArgument())
	err := is.resolveLoopArugment()
	assert.Nil(t, err)

	typeInfo := reflect.TypeOf(component.GetLoopArgument())
	isSplice := strings.HasPrefix(typeInfo.String(), "[]")
	assert.True(t, isSplice)

	component = mockComponentForInnerSolver()
	component.UpdateLoopArguemt("{{step1.p4}}")
	is = NewInnerSolver(component, "step1", &runConfig{})
	err = is.resolveLoopArugment()
	assert.Nil(t, err)

	typeInfo = reflect.TypeOf(component.GetLoopArgument())
	isSplice = strings.HasPrefix(typeInfo.String(), "[]")
	assert.True(t, isSplice)

	component = mockComponentForInnerSolver()
	component.UpdateLoopArguemt("{{p5}}")
	is = NewInnerSolver(component, "step1", &runConfig{})
	err = is.resolveLoopArugment()
	assert.Nil(t, err)

	typeInfo = reflect.TypeOf(component.GetLoopArgument())
	isSplice = strings.HasPrefix(typeInfo.String(), "[]")
	assert.True(t, isSplice)

	component = mockComponentForInnerSolver()
	component.UpdateLoopArguemt("{{p1}}")
	is = NewInnerSolver(component, "step1", &runConfig{})
	err = is.resolveLoopArugment()
	assert.NotNil(t, err)

	//  测试输入artifact
	mockArtInJsonFormat("./a.txt")

	component.UpdateLoopArguemt("{{in1}}")
	is = NewInnerSolver(component, "step1", &runConfig{fsID: "xx", logger: logger.LoggerForRun("innersolver")})
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
	is := NewInnerSolver(component, "step1", &runConfig{})
	err := is.resolveCondition()

	assert.Nil(t, err)
	assert.Equal(t, "1 > 10", component.GetCondition())

	// 测试artifact
	mockArtInJsonFormat("./a.txt")

	component.UpdateCondition("b{{in1}}_{{p1}} == 10")
	is = NewInnerSolver(component, "step1", &runConfig{fsID: "xx", logger: logger.LoggerForRun("innersolver")})
	err = is.resolveCondition()
	assert.Nil(t, err)

	assert.Equal(t, "b[10,12,13,14]_1 == 10", component.GetCondition())

	// 测试异常情况
	component.UpdateCondition("{{in5}} == 10")
	is = NewInnerSolver(component, "step1", &runConfig{fsID: "xx", logger: logger.LoggerForRun("innersolver")})
	err = is.resolveCondition()

	assert.NotNil(t, err)
}

func TestResolveCommand(t *testing.T) {
	component := mockComponentForInnerSolver()
	is := NewInnerSolver(component, "step1", &runConfig{})
	is.setSysParams(map[string]string{"PF_RUN_ID": "abc"})

	err := is.resolveCommand(true)
	assert.Nil(t, err)

	fmt.Println(component.Command)
	assert.Equal(t, component.Command, "echo 1 && cat ./b.txt >> {{out1}} && echo abc ")

	err = is.resolveCommand(false)
	assert.Nil(t, err)

	fmt.Println(component.Command)
	assert.Equal(t, component.Command, "echo 1 && cat ./b.txt >> out1.txt && echo abc ")
}

func TestResolveEnv(t *testing.T) {
	component := mockComponentForInnerSolver()
	is := NewInnerSolver(component, "step1", &runConfig{})
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
