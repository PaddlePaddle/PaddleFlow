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
	"strings"
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
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
		Command:      "cat {{p1}} && cat {{in2}} >> {{out1}} && echo {{PF_RUN_ID}} ",
		Condition:    "p1 > {{PF_LOOP_ARGUMENT}}",
		LoopArgument: "{{p2}}",
		Env: map[string]string{
			"e1": "{{p1}}",
			"e2": "0_{{p1}}_{{p3}}_4",
			"e3": "e3",
		},
	}
}

func TestResolveLoopArgument(t *testing.T) {

	component := mockComponentForInnerSolver()
	is := NewInnerSolver(component, "step1", &runConfig{})

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
}
