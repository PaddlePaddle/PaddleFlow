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
	"testing"
	"time"

	gomonkey "github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/common/schema"
	. "paddleflow/pkg/pipeline/common"
)

// 测试运行 Workflow 成功
func TestStartWithPostProcess(t *testing.T) {
	testCase := loadcase("./testcase/runPostProcess.yaml")
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["data_preprocess"].job), "Succeeded", func(_ *PaddleFlowJob) bool {
		return true
	})
	defer patches.Reset()

	time.Sleep(time.Millisecond * 10)

	wf.runtime.entryPoints["data_preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["data_preprocess"].done = true
	wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["main"].done = true
	wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded

	wf.runtime.entryPoints["validate"].done = true

	go wf.Start()

	time.Sleep(time.Millisecond * 10)
	fmt.Println("Status for postProcess['mail']:", wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)

	assert.Equal(t, common.StatusRunRunning, wf.runtime.status)
	assert.Equal(t, schema.JobStatus(""), wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)

	event1 := WorkflowEvent{Event: WfEventJobUpdate, Extra: map[string]interface{}{"event1": "step 1 data_process finished"}}

	wf.runtime.event <- event1
	time.Sleep(time.Millisecond * 10)

	// 此时的 job 一定会失败
	assert.Equal(t, common.StatusRunFailed, wf.runtime.status)
	assert.Equal(t, NodeTypePostProcess, wf.runtime.postProcess["mail"].nodeType)

	wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	statusToEntry := wf.runtime.statStepStatus(wf.runtime.entryPoints)
	statusToPost := wf.runtime.statStepStatus(wf.runtime.postProcess)

	wf.runtime.updateStatus(statusToEntry, statusToPost)
	assert.Equal(t, common.StatusRunSucceeded, wf.runtime.status)
}

func TestStopWithPostProcess(t *testing.T) {
	testCase := loadcase("./testcase/runPostProcess.yaml")
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["data_preprocess"].job), "Succeeded", func(_ *PaddleFlowJob) bool {
		return true
	})
	defer patches.Reset()

	time.Sleep(time.Millisecond * 10)

	wf.runtime.entryPoints["data_preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["data_preprocess"].done = true
	wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["main"].done = true
	wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded

	wf.runtime.entryPoints["validate"].done = true

	go wf.Start()

	// todo: remove event, receive event from job
	// event1 := WorkflowEvent{Event: WfEventJobUpdate, Extra: map[string]interface{}{"event1": "step 1 data_process finished"}}
	// wf.runtime.event <- event1
	time.Sleep(time.Millisecond * 10)
	fmt.Println("Status for postProcess['mail']", wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)

	wf.runtime.Stop()
	time.Sleep(time.Millisecond * 10)

	// 当前 Stop 不会终止 PostProcess 中节点
	assert.Equal(t, common.StatusRunTerminating, wf.runtime.status)
	assert.Equal(t, schema.JobStatus(""), wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)

	event1 := WorkflowEvent{Event: WfEventJobUpdate, Extra: map[string]interface{}{"event1": "step 1 data_process finished"}}
	wf.runtime.event <- event1
	time.Sleep(time.Millisecond * 100)

	// 此时的 job 一定会失败
	assert.Equal(t, common.StatusRunFailed, wf.runtime.status)
	assert.Equal(t, schema.StatusJobFailed, wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)
}

func TestStopEntry(t *testing.T) {
	testCase := loadcase("./testcase/runPostProcess.yaml")
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["data_preprocess"].job), "Succeeded", func(_ *PaddleFlowJob) bool {
		return true
	})
	defer patches.Reset()

	time.Sleep(time.Millisecond * 10)

	wf.runtime.entryPoints["data_preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["data_preprocess"].done = true
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

	wf.runtime.Stop()
	time.Sleep(time.Millisecond * 10)

	assert.Equal(t, schema.StatusJobCancelled, wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobCancelled, wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobFailed, wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, common.StatusRunFailed, wf.runtime.status)
}

func TestRestartEntry(t *testing.T) {
	testCase := loadcase("./testcase/runPostProcess.yaml")
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["data_preprocess"].job), "Succeeded", func(_ *PaddleFlowJob) bool {
		return true
	})
	defer patches.Reset()

	wf.runtime.entryPoints["data_preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["data_preprocess"].done = true

	assert.Equal(t, schema.JobStatus(""), wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.JobStatus(""), wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.JobStatus(""), wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)

	go wf.runtime.Restart()
	time.Sleep(time.Millisecond * 10)

	assert.Equal(t, schema.StatusJobFailed, wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobFailed, wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobFailed, wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, common.StatusRunFailed, wf.runtime.status)
}

func TestRestartPost(t *testing.T) {
	testCase := loadcase("./testcase/runPostProcess.yaml")
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	// 先是mock data_preprocess节点返回true
	// 再设置所有节点done = true
	// 保证data_preprocess能够成功结束，然后runtime再寻找下一个能运行的节点时，能够跳过后面的节点
	patches := gomonkey.ApplyMethod(reflect.TypeOf(wf.runtime.entryPoints["data_preprocess"].job), "Succeeded", func(_ *PaddleFlowJob) bool {
		return true
	})
	defer patches.Reset()

	wf.runtime.entryPoints["data_preprocess"].job.(*PaddleFlowJob).Status = schema.StatusJobSucceeded
	wf.runtime.entryPoints["data_preprocess"].done = true
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
	fmt.Println("begin")
	testCase := loadcase("./testcase/runFailureOptions.yaml")
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	fmt.Printf("\n %+v \n", wfs)
	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "", "", nil, extra, mockCbs)
	assert.Nil(t, err)

	go wf.runtime.Start()
	time.Sleep(time.Millisecond * 10)

	assert.Equal(t, schema.StatusJobFailed, wf.runtime.entryPoints["data_preprocess"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobCancelled, wf.runtime.entryPoints["main"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobCancelled, wf.runtime.entryPoints["validate"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, schema.StatusJobFailed, wf.runtime.postProcess["mail"].job.(*PaddleFlowJob).Status)
	assert.Equal(t, common.StatusRunFailed, wf.runtime.status)

	fmt.Println("end")

}
