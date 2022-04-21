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
	"paddleflow/pkg/common/schema"
)

func GetPFRuntime(stepName string, runtimeView schema.RuntimeView, workflowSource schema.WorkflowSource) (string, error) {
	pfRuntime := schema.RuntimeView{}

	_, ok := workflowSource.EntryPoints[stepName]
	if ok {
		err := recursiveGetJobView(stepName, pfRuntime, runtimeView, workflowSource)
		if err != nil {
			return "", err
		}
	} else {
		for name, jobView := range runtimeView {
			pfRuntime[name] = jobView
		}
	}

	DeleteSystemParamEnvForPFRuntime(pfRuntime)
	pfRuntimeJson, err := json.Marshal(pfRuntime)

	if err != nil {
		return "", err
	}

	return string(pfRuntimeJson), nil
}

func recursiveGetJobView(stepName string, pfRuntime, SrcRunTime schema.RuntimeView, workflowSource schema.WorkflowSource) error {
	_, ok := workflowSource.EntryPoints[stepName]
	if !ok {
		return fmt.Errorf("cannot find step[%s] in workflowSource.entrypoints", stepName)
	}
	if _, ok := pfRuntime[stepName]; ok {
		return nil
	}
	for _, step := range workflowSource.EntryPoints[stepName].GetDeps() {
		pfRuntime[step] = SrcRunTime[step]
		if err := recursiveGetJobView(step, pfRuntime, SrcRunTime, workflowSource); err != nil {
			return err
		}
	}
	return nil
}

func DeleteSystemParamEnvForPFRuntime(runtimeView schema.RuntimeView) {
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
			}
		}

		if !isSysParam {
			envWithoutSystmeEnv[name] = value
		}
	}

	return envWithoutSystmeEnv
}
