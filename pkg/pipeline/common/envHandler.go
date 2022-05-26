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

package common

import (
	"encoding/json"
	"fmt"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type PFRuntimeGenerator struct {
	runtimeView    schema.RuntimeView
	workflowSource schema.WorkflowSource
}

func NewPFRuntimeGenerator(runtimeView schema.RuntimeView, workflowSource schema.WorkflowSource) *PFRuntimeGenerator {
	return &PFRuntimeGenerator{
		runtimeView:    runtimeView,
		workflowSource: workflowSource,
	}
}

// 1. 对于 entry_points 中的节点， 其 PF_RUN_TIME 只包含其祖先节点的信息
// 2. 对于 post_process 中的节点，其 PF_RUN_TIME 包含entrypoint中所有节点的信息
// 3. 由于所有节点的系统的变量都是一支的，所以 PF_RUN_TIME 中的节点信息中不包含系统环境变量
func (pfg *PFRuntimeGenerator) GetPFRuntime(stepName string) (string, error) {
	pfRuntime := schema.RuntimeView{}

	_, ok := pfg.workflowSource.EntryPoints[stepName]
	if ok {
		err := pfg.recursiveGetJobView(stepName, pfRuntime)
		if err != nil {
			return "", err
		}
	} else {
		for name, jobView := range pfg.runtimeView {
			pfRuntime[name] = jobView
		}
	}

	pfg.deleteSystemParamEnvForPFRuntime(pfRuntime)
	pfRuntimeJson, err := json.Marshal(pfRuntime)

	if err != nil {
		return "", err
	}

	return string(pfRuntimeJson), nil
}

func (pfg *PFRuntimeGenerator) recursiveGetJobView(stepName string, pfRuntime schema.RuntimeView) error {
	_, ok := pfg.workflowSource.EntryPoints[stepName]
	if !ok {
		return fmt.Errorf("cannot find step[%s] in workflowSource.entrypoints", stepName)
	}
	if _, ok := pfRuntime[stepName]; ok {
		return nil
	}
	for _, step := range pfg.workflowSource.EntryPoints[stepName].GetDeps() {
		pfRuntime[step] = pfg.runtimeView[step]
		if err := pfg.recursiveGetJobView(step, pfRuntime); err != nil {
			return err
		}
	}
	return nil
}

func (pfg *PFRuntimeGenerator) deleteSystemParamEnvForPFRuntime(runtimeView schema.RuntimeView) {
	for name, jobView := range runtimeView {
		jobView.Env = DeleteSystemParamEnv(jobView.Env)
		runtimeView[name] = jobView
	}
}

// 删除系统变量
func DeleteSystemParamEnv(sysParamEnv map[string]string) map[string]string {
	envWithoutSystmeEnv := map[string]string{}
	for name, value := range sysParamEnv {
		isSysParam := false
		for _, sysParam := range SysParamNameList {
			if sysParam == name {
				isSysParam = true
				break
			}
		}

		if !isSysParam {
			envWithoutSystmeEnv[name] = value
		}
	}

	return envWithoutSystmeEnv
}
