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
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func mockWorkflowRuntime() (*WorkflowRuntime, error) {
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	yamlByte := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource(yamlByte)
	if err != nil {
		return nil, err
	}

	extra := GetExtra()
	wf, err := NewMockWorkflow(wfs, "", nil, extra, mockCbs)
	if err != nil {
		return nil, err
	}

	rf := mockRunConfigForComponentRuntime()
	rf.WorkflowSource = &wf.Source
	rf.callbacks = mockCbs

	wfr := NewWorkflowRuntime(rf)
	return wfr, nil
}

// 测试运行 Workflow 成功
func TestStartWithPostProcess(t *testing.T) {
	var srt *StepRuntime
	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Start", func(srt *StepRuntime) {
		srt.parallelismManager.increase()
		srt.updateStatus(StatusRuntimeSucceeded)
		srt.sendEventToParent <- *NewWorkflowEvent(WfEventJobUpdate, "succeeded", map[string]interface{}{
			common.WfEventKeyComponentName: srt.getComponent().GetName(),
			common.WfEventKeyStatus:        StatusRuntimeSucceeded,
		})
		return
	})
	defer patch2.Reset()

	var drt *DagRuntime
	patch3 := gomonkey.ApplyMethod(reflect.TypeOf(drt), "Start", func(drt *DagRuntime) {
		drt.updateStatus(StatusRuntimeSucceeded)
		drt.sendEventToParent <- *NewWorkflowEvent(WfEventJobUpdate, "succeeded", map[string]interface{}{
			common.WfEventKeyComponentName: drt.getComponent().GetName(),
			common.WfEventKeyStatus:        StatusRuntimeSucceeded,
		})
		return
	})
	defer patch3.Reset()

	wfr, err := mockWorkflowRuntime()
	assert.Nil(t, err)

	wfr.Start()
	wfr.Listen()

	assert.Equal(t, common.StatusRunSucceeded, wfr.status)
	assert.Equal(t, StatusRuntimeSucceeded, wfr.entryPoints.status)
	assert.Equal(t, StatusRuntimeSucceeded, wfr.postProcess.status)
}

func TestStopRun(t *testing.T) {
	wfr, err := mockWorkflowRuntime()
	assert.Nil(t, err)

	var drt *DagRuntime
	patch3 := gomonkey.ApplyMethod(reflect.TypeOf(drt), "Start", func(drt *DagRuntime) {
		drt.updateStatus(StatusRuntimeRunning)
		return
	})
	defer patch3.Reset()

	var srt *StepRuntime
	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Start", func(srt *StepRuntime) {
		srt.parallelismManager.increase()
		srt.updateStatus(StatusRuntimeSucceeded)
		srt.sendEventToParent <- *NewWorkflowEvent(WfEventJobUpdate, "succeeded", map[string]interface{}{
			common.WfEventKeyComponentName: srt.getComponent().GetName(),
			common.WfEventKeyStatus:        StatusRuntimeSucceeded,
		})
		return
	})
	defer patch2.Reset()

	wfr.Start()

	go wfr.Listen()
	time.Sleep(time.Millisecond * 100)

	go wfr.entryPoints.Stop()

	wfr.Stop(false)
	time.Sleep(time.Millisecond * 100)

	// 当前 Stop 不会终止 PostProcess 中节点
	assert.Equal(t, common.StatusRunTerminating, wfr.status)
	assert.Equal(t, StatusRuntimeTerminating, wfr.entryPoints.status)
	assert.Nil(t, wfr.postProcess)

	patch5 := gomonkey.ApplyMethod(reflect.TypeOf(drt), "Start", func(drt *DagRuntime) {
		drt.updateStatus(StatusRuntimeSucceeded)
		return
	})
	defer patch5.Reset()

	wfr.status = common.StatusRunInitiating
	wfr.Start()
	go wfr.Listen()
	time.Sleep(time.Millisecond * 100)

	assert.Equal(t, common.StatusRunRunning, wfr.status)
	assert.Equal(t, StatusRuntimeSucceeded, wfr.entryPoints.status)
	assert.Nil(t, wfr.postProcess)

	wfr.Stop(false)
	time.Sleep(time.Millisecond * 100)

	assert.Equal(t, common.StatusRunTerminating, wfr.status)
	assert.Equal(t, StatusRuntimeSucceeded, wfr.entryPoints.status)
	assert.Nil(t, wfr.postProcess)

	wfr.Stop(true)
	time.Sleep(time.Millisecond * 100)

	assert.Equal(t, common.StatusRunTerminated, wfr.status)
	assert.Equal(t, StatusRuntimeSucceeded, wfr.entryPoints.status)
	assert.Equal(t, StatusRuntimeCancelled, wfr.postProcess.status)

}

func TestRestartEntry(t *testing.T) {
	wfr, err := mockWorkflowRuntime()
	assert.Nil(t, err)

	var srt *StepRuntime
	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Restart", func(srt *StepRuntime, _ *schema.JobView) {
		srt.parallelismManager.increase()
		srt.updateStatus(StatusRuntimeSucceeded)
		srt.sendEventToParent <- *NewWorkflowEvent(WfEventJobUpdate, "succeeded", map[string]interface{}{
			common.WfEventKeyComponentName: srt.getComponent().GetName(),
			common.WfEventKeyStatus:        StatusRuntimeRunning,
		})
		return
	})
	defer patch2.Reset()

	var drt *DagRuntime
	patch3 := gomonkey.ApplyMethod(reflect.TypeOf(drt), "Restart", func(drt *DagRuntime, _ *schema.DagView) {
		drt.updateStatus(StatusRuntimeRunning)
		drt.sendEventToParent <- *NewWorkflowEvent(WfEventJobUpdate, "succeeded", map[string]interface{}{
			common.WfEventKeyComponentName: drt.getComponent().GetName(),
			common.WfEventKeyStatus:        StatusRuntimeSucceeded,
		})
		return
	})
	defer patch3.Reset()

	patch4 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Start", func(srt *StepRuntime) {
		srt.parallelismManager.increase()
		srt.updateStatus(StatusRuntimeSucceeded)
		srt.sendEventToParent <- *NewWorkflowEvent(WfEventJobUpdate, "succeeded", map[string]interface{}{
			common.WfEventKeyComponentName: srt.getComponent().GetName(),
			common.WfEventKeyStatus:        StatusRuntimeSucceeded,
		})
		return
	})
	defer patch4.Reset()

	// 测试都需要重启的情况
	go wfr.Restart(&schema.DagView{}, map[string]*schema.JobView{})
	go wfr.Listen()

	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, common.StatusRunRunning, wfr.status)
	assert.Equal(t, StatusRuntimeRunning, wfr.entryPoints.status)
	assert.Nil(t, wfr.postProcess)

	patch5 := gomonkey.ApplyMethod(reflect.TypeOf(drt), "Restart", func(drt *DagRuntime, _ *schema.DagView) {
		drt.updateStatus(StatusRuntimeSucceeded)
		drt.sendEventToParent <- *NewWorkflowEvent(WfEventJobUpdate, "succeeded", map[string]interface{}{
			common.WfEventKeyComponentName: drt.getComponent().GetName(),
			common.WfEventKeyStatus:        StatusRuntimeSucceeded,
		})
		return
	})
	defer patch5.Reset()

	wfr.status = common.StatusRunInitiating
	wfr.entryPoints.updateStatus(StatusRuntimeInit)

	// entrypoint 无需重启，postProcess 需要重启
	views := map[string][]schema.ComponentView{
		"data-preprocess": []schema.ComponentView{
			&schema.JobView{
				Status:   StatusRuntimeSucceeded,
				LoopSeq:  0,
				StepName: "data-preprocess",
			},
		},
		"main": []schema.ComponentView{
			&schema.JobView{
				Status:   StatusRuntimeSucceeded,
				LoopSeq:  0,
				StepName: "main",
			},
		},
		"validate": []schema.ComponentView{
			&schema.JobView{
				Status:   StatusRuntimeSucceeded,
				LoopSeq:  0,
				StepName: "validate",
			},
		},
	}

	dagView := &schema.DagView{
		EntryPoints: views,
		Status:      StatusRuntimeSucceeded,
	}

	go wfr.Restart(dagView, map[string]*schema.JobView{})
	go wfr.Listen()
	time.Sleep(time.Millisecond * 1000)

	assert.Equal(t, common.StatusRunSucceeded, wfr.status)
	assert.Equal(t, StatusRuntimeSucceeded, wfr.entryPoints.status)
	assert.Equal(t, StatusRuntimeSucceeded, wfr.postProcess.status)
}

func TestUpdateStatusAccordingComponentStatus(t *testing.T) {
	wfr, err := mockWorkflowRuntime()
	assert.Nil(t, err)

	wfr.status = common.StatusRunSucceeded
	msg := wfr.updateStatusAccordingComponentStatus()
	assert.Equal(t, msg, "")

	wfr.status = common.StatusRunRunning
	wfr.entryPoints.status = StatusRuntimeRunning

	msg = wfr.updateStatusAccordingComponentStatus()
	assert.Equal(t, msg, "")

	wfr.entryPoints.status = common.StatusRunSucceeded
	wfr.entryPoints.done = true
	wfr.postProcess = &StepRuntime{
		baseComponentRuntime: &baseComponentRuntime{},
	}
	wfr.postProcess.status = StatusRuntimeRunning

	msg = wfr.updateStatusAccordingComponentStatus()
	assert.Equal(t, msg, "")

	wfr.entryPoints.status = StatusRuntimeFailed
	wfr.postProcess.status = StatusRuntimeFailed
	wfr.entryPoints.done = true
	wfr.postProcess.done = true
	wfr.status = common.StatusRunRunning

	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(wfr), "getRuntimeFullNameByStatus",
		func(*WorkflowRuntime, RuntimeStatus) string {
			return "entry_points.step1, post_process"
		})
	defer patch.Reset()
	msg = wfr.updateStatusAccordingComponentStatus()
	assert.Contains(t, msg, "entry_points.step1, post_process")
	assert.Contains(t, msg, "failed")

	wfr.status = common.StatusRunRunning
	wfr.entryPoints.status = StatusRuntimeTerminated
	wfr.postProcess.status = StatusRuntimeTerminated
	msg = wfr.updateStatusAccordingComponentStatus()
	assert.Contains(t, msg, "entry_points.step1, post_process")
	assert.Contains(t, msg, "update status to failed")
	assert.Contains(t, msg, "terminated unexpectedly")

	wfr.status = common.StatusRunTerminating
	msg = wfr.updateStatusAccordingComponentStatus()
	assert.Contains(t, msg, "entry_points.step1, post_process")
	assert.Contains(t, msg, "update status to terminated")
	assert.Contains(t, msg, "some step or dag was terminated")

	wfr.status = common.StatusRunRunning
	wfr.entryPoints.status = StatusRuntimeSucceeded
	wfr.postProcess.status = StatusRuntimeSucceeded
	msg = wfr.updateStatusAccordingComponentStatus()
	assert.Contains(t, msg, "Run successfully")
}

func TestGetRuntimeFullNameByStatus(t *testing.T) {
	wfr, err := mockWorkflowRuntime()
	assert.Nil(t, err)

	wfr.WorkflowSource.Name = "test"

	var drt *DagRuntime
	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(drt), "getDeepestRuntimeByStatus",
		func(_ *DagRuntime, status RuntimeStatus, runtimes []componentRuntime) []componentRuntime {
			runtimes = append(runtimes,
				&StepRuntime{
					baseComponentRuntime: &baseComponentRuntime{componentFullName: "test.et.step1"},
				},
				&StepRuntime{
					baseComponentRuntime: &baseComponentRuntime{componentFullName: "test.et.step1-0"},
				},
				&StepRuntime{
					baseComponentRuntime: &baseComponentRuntime{componentFullName: "test.et.step2"},
				},
			)
			return runtimes
		})
	defer patch.Reset()

	wfr.postProcess = nil
	sn := wfr.getRuntimeFullNameByStatus(StatusRuntimeFailed)
	assert.Equal(t, "et.step1,et.step1-0,et.step2", sn)

	wfr.postProcess = &StepRuntime{
		baseComponentRuntime: &baseComponentRuntime{componentFullName: "test.post"},
	}
	wfr.postProcess.status = StatusRuntimeFailed
	sn = wfr.getRuntimeFullNameByStatus(StatusRuntimeFailed)
	assert.Equal(t, "et.step1,et.step1-0,et.step2,post", sn)
}
