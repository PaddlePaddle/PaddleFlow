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
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

// referenceSolver: 用于解析 reference 字段
type referenceSolver struct {
	*schema.WorkflowSource
}

func NewReferenceSolver(ws *schema.WorkflowSource) *referenceSolver {
	return &referenceSolver{
		WorkflowSource: ws,
	}
}

// resolveComponentReference: 解析指定component的reference 字段
func (rr *referenceSolver) resolveComponentReference(component schema.Component) (schema.Component, error) {
	// 引用的合法性由 parser 模块保证，因此，此处不在进行合法行校验

	// 当前，reference 会被解析成 WorkflowStep, 所以如果不能转成 WorkflowStep, 则必然没有 referenece 字段
	step, ok := component.(*schema.WorkflowSourceStep)
	if !ok {
		return component, nil
	}

	if step.Reference.Component == "" {
		return component, nil
	}

	// 被引用节点名
	referencedComponentName := step.Reference.Component

	referencedComponent := rr.WorkflowSource.Components[referencedComponentName].DeepCopy()
	referencedComponent.UpdateName(component.GetName())

	// 递归的解析 reference 字段
	referencedComponent, err := rr.resolveComponentReference(referencedComponent)
	if err != nil {
		return nil, err
	}

	// 使用reference节点的 parameter 和 输入artifact 覆盖 refereneced 节点的相同字段
	referencedParameter := referencedComponent.GetParameters()
	for name, value := range step.GetParameters() {
		referencedParameter[name] = value
	}

	referencedInputArtifact := referencedComponent.GetArtifacts().Input
	for name, value := range step.GetArtifacts().Input {
		referencedInputArtifact[name] = value
	}

	// 使用 reference 节点的 deps 取代 refereneced 节点 deps 字段
	referencedComponent.UpdateDeps(step.Deps)

	return referencedComponent, nil
}
