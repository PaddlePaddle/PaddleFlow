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
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/stretchr/testify/assert"
)

func mockerStepWithRef(name string, refName string) *schema.WorkflowSourceStep {
	return &schema.WorkflowSourceStep{
		Name:      name,
		Reference: schema.Reference{Component: refName},
		Parameters: map[string]interface{}{
			"p1": 10,
			"p2": 11,
		},
		Artifacts: schema.Artifacts{
			Input: map[string]string{
				"in1": "{{s.out1}}",
				"in2": "{{s.out2}}",
			},
		},
	}
}

func mockerComponentWithRef(name, refName string) schema.Component {
	return &schema.WorkflowSourceStep{
		Name:      name,
		Reference: schema.Reference{Component: refName},
		Parameters: map[string]interface{}{
			"p1": 0,
			"p2": 11,
			"p3": 5,
		},
		Artifacts: schema.Artifacts{
			Input: map[string]string{
				"in1": "{{st.out1}}",
				"in2": "{{st.out2}}",
			},
		},
	}
}

func mockerStep(name string) *schema.WorkflowSourceStep {
	return &schema.WorkflowSourceStep{
		Name: name,
		Parameters: map[string]interface{}{
			"p1": 1,
			"p2": 2,
			"p3": 0,
			"p4": 5,
		},
		Artifacts: schema.Artifacts{
			Input: map[string]string{
				"in1": "{{step0.out1}}",
				"in2": "{{step0.out2}}",
			},
			Output: map[string]string{
				"out1": "out1.txt",
			},
		},
	}
}

func mockerWorkflowDag() *schema.WorkflowSourceDag {
	return &schema.WorkflowSourceDag{
		EntryPoints: map[string]schema.Component{
			"ref1":  mockerStepWithRef("ref1", "cp1"),
			"ref2":  mockerStepWithRef("ref2", "cp3"),
			"step3": mockerStep("step3"),
		},
	}
}

func mockerWorkflowSource() *schema.WorkflowSource {
	return &schema.WorkflowSource{
		EntryPoints: *mockerWorkflowDag(),
		Components: map[string]schema.Component{
			"cp1": mockerComponentWithRef("cp1", "cp2"),
			"cp2": mockerComponentWithRef("cp2", "cp3"),
			"cp3": mockerStep("cp3"),
		},
	}
}

func TestResolveComponentReference(t *testing.T) {
	ws := mockerWorkflowSource()
	rs := NewReferenceSolver(ws)

	// 1. 测试无 referece 节点

	cp, err := rs.resolveComponentReference(ws.EntryPoints.EntryPoints["step3"])
	assert.Nil(t, err)

	assert.Equal(t, cp.GetName(), "step3")
	assert.Equal(t, cp.GetArtifacts(), mockerStep("step3").Artifacts)

	// 2. 测试存在一层 ref
	cp, err = rs.resolveComponentReference(ws.EntryPoints.EntryPoints["ref2"])
	assert.Equal(t, cp.GetParameters()["p3"].(int), 0)
	assert.Equal(t, cp.GetParameters()["p4"].(int), 5)
	assert.Equal(t, cp.GetParameters()["p1"].(int), 10)
	assert.Equal(t, cp.GetName(), "ref2")

	assert.Equal(t, cp.GetArtifacts().Input["in1"], "{{s.out1}}")
	assert.Equal(t, cp.GetArtifacts().Input["in2"], "{{s.out2}}")

	// 3. 测试多层ref

	cp, err = rs.resolveComponentReference(ws.EntryPoints.EntryPoints["ref1"])
	assert.Equal(t, cp.GetParameters()["p3"].(int), 5)
	assert.Equal(t, cp.GetParameters()["p4"].(int), 5)
	assert.Equal(t, cp.GetParameters()["p1"].(int), 10)
	assert.Equal(t, cp.GetName(), "ref1")

	assert.Equal(t, cp.GetArtifacts().Input["in1"], "{{s.out1}}")
	assert.Equal(t, cp.GetArtifacts().Input["in2"], "{{s.out2}}")
}
