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
	"strings"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func newDagViewFromDagRuntime(drt *DagRuntime, msg string) schema.DagView {
	deps := strings.Join(drt.component.GetDeps(), string(','))

	paramters := map[string]string{}
	for name, value := range drt.component.GetParameters() {
		paramters[name] = fmt.Sprintf("%v", value)
	}

	// DAGID 在写库时生成，因此，此处并不会传递该参数, EntryPoints 在运行子节点时会同步至数据库，因此此处不包含这两个字段
	return schema.DagView{
		DagName:    drt.componentName,
		Deps:       deps,
		Parameters: paramters,
		Artifacts:  drt.component.GetArtifacts(),
		StartTime:  drt.startTime,
		EndTime:    drt.endTime,
		Status:     drt.status,
		Message:    msg,
	}
}

// newDagViewFromWorkFlowSourceDag: 从WorkflowSourceDag 生成 DagView
// 如果是生成 对应的 dagRuntime 失败，则会调用该函数来生成对应 DagView
func newDagViewFromWorkFlowSourceDag(dag *schema.WorkflowSourceDag, dagName string, msg string, status RuntimeStatus) schema.DagView {
	deps := strings.Join(dag.GetDeps(), string(','))

	paramters := map[string]string{}
	for name, value := range dag.GetParameters() {
		paramters[name] = fmt.Sprintf("%v", value)
	}

	// DAGID 在写库时生成，因此，此处并不会传递该参数, EntryPoints 在运行子节点时会同步至数据库，由于节点都没有运行，因此也就不会有starttime 和 endtime 属性
	return schema.DagView{
		DagName:    dagName,
		Deps:       deps,
		Parameters: paramters,
		Artifacts:  dag.GetArtifacts(),
		Status:     status,
		Message:    msg,
	}
}

/*
type JobView struct {
	JobID      string            `json:"jobID"`
	JobName    string            `json:"name"`
	Command    string            `json:"command"`
	Parameters map[string]string `json:"parameters"`
	Env        map[string]string `json:"env"`
	StartTime  string            `json:"startTime"`
	EndTime    string            `json:"endTime"`
	Status     JobStatus         `json:"status"`
	Deps       string            `json:"deps"`
	DockerEnv  string            `json:"dockerEnv"`
	Artifacts  Artifacts         `json:"artifacts"`
	Cache      Cache             `json:"cache"`
	JobMessage string            `json:"jobMessage"`
	CacheRunID string            `json:"cacheRunID"`
}
*/
// newJobViewFromWorkFlowSourceStep: 从 WorkflowSouceStep 生成 JobView
func newStepViewFromWorkFlowSourceStep(step schema.WorkflowSourceStep, stepName, msg string, status RuntimeStatus, config *runConfig) schema.JobView {
	jobName := generateJobName(config.runID, stepName, 0)
	params := map[string]string{}

	for name, value := range step.Parameters {
		params[name] = fmt.Sprintf("%v", value)
	}

	return schema.JobView{
		JobName:    jobName,
		Command:    step.Command,
		Parameters: params,
		Env:        step.Env,
		Status:     status,
		Deps:       step.Deps,
		Artifacts:  step.Artifacts,
		JobMessage: msg,
	}
}
