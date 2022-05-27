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
	. "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

// 测试运行 Workflow 成功
func TestStartWithPostProcess(t *testing.T) {
	yamlByte := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource(yamlByte)
	assert.Nil(t, err)

	// fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	wf.runtime.entryPoints["data-preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["data-preprocess"].done = true
	wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["main"].done = true
	wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded

	wf.runtime.entryPoints["validate"].done = true

	go wf.Start()

	time.Sleep(time.Millisecond * 10)

	assert.Equal(t, common.StatusRunRunning, wf.runtime.status)
	assert.Equal(t, schema.JobStatus(""), wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)

	event1 := WorkflowEvent{Event: WfEventJobUpdate, Extra: map[string]interface{}{"event1": "step 1 data-process finished"}}

	wf.runtime.event <- event1
	time.Sleep(time.Millisecond * 10)

	// 此时的 job 一定会失败
	assert.Equal(t, common.StatusRunFailed, wf.runtime.status)
	assert.Equal(t, NodeTypePostProcess, wf.runtime.postProcess["mail"].nodeType)

	wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	statusToEntry := wf.runtime.countStepStatus(wf.runtime.entryPoints)
	statusToPost := wf.runtime.countStepStatus(wf.runtime.postProcess)

	wf.runtime.updateStatus(statusToEntry, statusToPost)
	assert.Equal(t, common.StatusRunSucceeded, wf.runtime.status)
}

func TestStopWithPostProcess(t *testing.T) {
	yamlByte := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource(yamlByte)
	assert.Nil(t, err)

	// fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 10)

	wf.runtime.entryPoints["data-preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["data-preprocess"].done = true
	wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["main"].done = true
	wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded

	wf.runtime.entryPoints["validate"].done = true

	go wf.Start()

	time.Sleep(time.Millisecond * 10)

	wf.runtime.Stop(false)
	time.Sleep(time.Millisecond * 10)

	// 当前 Stop 不会终止 PostProcess 中节点
	assert.Equal(t, common.StatusRunTerminating, wf.runtime.status)
	assert.Equal(t, schema.JobStatus(""), wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)

	event1 := WorkflowEvent{Event: WfEventJobUpdate, Extra: map[string]interface{}{"event1": "step 1 data-process finished"}}
	wf.runtime.event <- event1
	time.Sleep(time.Millisecond * 100)

	// 此时的 job 一定会失败
	assert.Equal(t, common.StatusRunFailed, wf.runtime.status)
	assert.Equal(t, schema.StatusJobFailed, wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)
}

func TestStopEntry(t *testing.T) {
	fmt.Println("TestStopEntry Begin")
	yamlByte := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource(yamlByte)
	assert.Nil(t, err)

	// fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 10)

	wf.runtime.entryPoints["data-preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["data-preprocess"].done = true
	wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["main"].done = true
	wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded

	wf.runtime.entryPoints["validate"].done = true

	go wf.Start()
	time.Sleep(time.Millisecond * 10)

	wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status = ""
	wf.runtime.entryPoints["main"].done = false
	wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status = ""
	wf.runtime.entryPoints["validate"].done = false

	go wf.runtime.entryPoints["main"].Execute()
	go wf.runtime.entryPoints["validate"].Execute()
	time.Sleep(time.Millisecond * 10)

	assert.Equal(t, common.StatusRunRunning, wf.runtime.status)
	assert.Equal(t, schema.JobStatus(""), wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status)

	wf.runtime.Stop(false)
	time.Sleep(time.Millisecond * 10)

	assert.Equal(t, schema.StatusJobCancelled, wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobCancelled, wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobFailed, wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, common.StatusRunFailed, wf.runtime.status)

	fmt.Println("TestStopEntry End")
}

func TestRestartEntry(t *testing.T) {
	yamlByte := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource(yamlByte)
	assert.Nil(t, err)

	// fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)
	wf.runtime.entryPoints["data-preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["data-preprocess"].done = true

	assert.Equal(t, schema.JobStatus(""), wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.JobStatus(""), wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.JobStatus(""), wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)

	go wf.runtime.Restart()
	time.Sleep(time.Millisecond * 10)

	assert.Equal(t, schema.StatusJobFailed, wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobCancelled, wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobFailed, wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, common.StatusRunFailed, wf.runtime.status)
}

func TestRestartPost(t *testing.T) {
	yamlByte := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource(yamlByte)
	assert.Nil(t, err)

	// fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	wf.runtime.entryPoints["data-preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["data-preprocess"].done = true
	wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["main"].done = true
	wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded

	go wf.runtime.Restart()
	time.Sleep(time.Millisecond * 10)

	assert.Equal(t, schema.StatusJobFailed, wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, common.StatusRunFailed, wf.runtime.status)
}

// 测试 FailureOptions 为快速失败的情况
func TestFailFast(t *testing.T) {
	yamlByte := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource(yamlByte)
	assert.Nil(t, err)

	fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	go wf.runtime.Start()
	time.Sleep(time.Millisecond * 10)

	assert.Equal(t, schema.StatusJobFailed, wf.runtime.entryPoints["data-preprocess"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobCancelled, wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobCancelled, wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobFailed, wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, common.StatusRunFailed, wf.runtime.status)
}
