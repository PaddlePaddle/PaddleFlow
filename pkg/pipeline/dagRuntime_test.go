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
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	apicommon "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
)

const (
	dagRunYaml = "./testcase/run_dag.yaml"
)

func mockerDagRuntime(ec chan WorkflowEvent) (*DagRuntime, error) {
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	testCase := loadcase(dagRunYaml)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	if err != nil {
		return nil, err
	}

	rf := mockRunConfigForComponentRuntime()
	rf.WorkflowSource = &wfs
	rf.callbacks = mockCbs

	extra := GetExtra()
	wfptr, err := NewMockWorkflow(wfs, rf.runID, map[string]interface{}{}, extra, rf.callbacks)
	if err != nil {
		return nil, err
	}

	wfs = wfptr.Source
	updateRuntimeCalled = false

	failctx, _ := context.WithCancel(context.Background())
	drt := NewDagRuntime("a.entrypoint", "a.entrypoint", &wfs.EntryPoints, 0, context.Background(), failctx,
		ec, rf, "0")

	var srt *StepRuntime
	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Start", func(_ *StepRuntime) {
		fmt.Println("param", drt.parallelismManager.CurrentParallelism())
	})
	defer patch2.Reset()

	return drt, nil
}

func TestNewDagRuntimeWithStatus(t *testing.T) {
	driver.InitMockDB()
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	rf := mockRunConfigForComponentRuntime()
	rf.WorkflowSource = &wfs
	rf.callbacks = mockCbs

	extra := GetExtra()
	wfptr, err := NewMockWorkflow(wfs, rf.runID, map[string]interface{}{}, extra, rf.callbacks)
	assert.Nil(t, err)

	wfs = wfptr.Source
	updateRuntimeCalled = false
	eventChan := make(chan WorkflowEvent)
	ep := &WorkflowEvent{}
	go mockToListenEvent(eventChan, ep)

	failctx, _ := context.WithCancel(context.Background())
	dr := newDagRuntimeWithStatus("a.entrypoint", "a.entrypoint", &wfs.EntryPoints, 0, context.Background(), failctx,
		eventChan, rf, "0", StatusRuntimeCancelled, "cancel")

	time.Sleep(time.Millisecond * 100)
	assert.True(t, dr.isCancelled())
	assert.Equal(t, ep.Message, "cancel")
}

func TestGenerateSubComponentFullName(t *testing.T) {
	eventChan := make(chan WorkflowEvent)
	ep := &WorkflowEvent{}
	go mockToListenEvent(eventChan, ep)
	drt, err := mockerDagRuntime(eventChan)
	assert.Nil(t, err)

	fullName := drt.generateSubRuntimeName("step1", 0)
	assert.Equal(t, "a.entrypoint.step1", fullName)

	drt.runtimeName = "entrypoint"
	fullName = drt.generateSubRuntimeName("step1", 1)
	assert.Equal(t, "a.entrypoint.step1-1", fullName)
}

func TestGetReadyComponent(t *testing.T) {
	eventChan := make(chan WorkflowEvent)
	drt, err := mockerDagRuntime(eventChan)
	assert.Nil(t, err)

	readyNames := drt.getReadyComponent()

	assert.Equal(t, len(readyNames), 2)
	_, ok := readyNames["randint"]
	assert.True(t, ok)

	_, ok = readyNames["disStep"]
	assert.True(t, ok)

	st := drt.getworkflowSouceDag().EntryPoints["randint"].(*schema.WorkflowSourceStep)
	srt := NewStepRuntime("a.entrypoint.randint", "a.entrypoint.randint", st, 0, drt.ctx, drt.failureOpitonsCtx,
		drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["randint"] = append(drt.subComponentRumtimes["randint"], srt)

	srt2 := NewStepRuntime("a.entrypoint.disStep", "a.entrypoint.disStep", st, 0, drt.ctx, drt.failureOpitonsCtx,
		drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["disStep"] = append(drt.subComponentRumtimes["randint"], srt2)
	srt.increase()
	srt2.updateStatus(StatusRuntimeRunning)

	srt.updateStatus(StatusRuntimeRunning)
	readyNames = drt.getReadyComponent()
	assert.Equal(t, len(readyNames), 0)

	// 上游节点状态为 succeeded
	srt.increase()
	srt.updateStatus(StatusRuntimeSucceeded)

	readyNames = drt.getReadyComponent()
	assert.Equal(t, len(readyNames), 2)
	_, ok = readyNames["split-by-threshold"]
	assert.True(t, ok)

	_, ok = readyNames["square-loop"]
	assert.True(t, ok)

	// 上游节点状态为 Failed
	srt.done = false
	srt.increase()
	err = srt.updateStatus(StatusRuntimeFailed)
	assert.Nil(t, err)
	readyNames = drt.getReadyComponent()
	assert.Equal(t, len(readyNames), 0)

	// 测试循环结构
	drt.getworkflowSouceDag().EntryPoints["square-loop"].UpdateLoopArguemt([]int{1, 2})
	drt2 := NewDagRuntime("a.entrypoint.square-loop", "a.entrypoint.square-loop",
		drt.getworkflowSouceDag().EntryPoints["square-loop"].(*schema.WorkflowSourceDag),
		0, drt.ctx, drt.failureOpitonsCtx, drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["square-loop"] = append(drt.subComponentRumtimes["square-loop"], drt2)

	readyNames = drt.getReadyComponent()
	assert.Equal(t, len(readyNames), 0)

	drt2.updateStatus(StatusRuntimeSucceeded)
	readyNames = drt.getReadyComponent()
	assert.Equal(t, len(readyNames), 0)

	drt3 := NewDagRuntime("a.entrypoint.square-loop-1", "a.entrypoint.square-loop",
		drt.getworkflowSouceDag().EntryPoints["square-loop"].(*schema.WorkflowSourceDag),
		0, drt.ctx, drt.failureOpitonsCtx, drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["square-loop"] = append(drt.subComponentRumtimes["square-loop"], drt3)
	drt3.updateStatus(StatusRuntimeSucceeded)

	readyNames = drt.getReadyComponent()
	assert.Equal(t, len(readyNames), 1)
	_, ok = readyNames["sum"]
	assert.True(t, ok)
}

func TestCreateAndStartSubComponentRuntime(t *testing.T) {
	eventChan := make(chan WorkflowEvent)
	drt, err := mockerDagRuntime(eventChan)
	assert.Nil(t, err)

	st := drt.getworkflowSouceDag().EntryPoints["randint"].(*schema.WorkflowSourceStep)
	srt := NewStepRuntime("a.entrypoint.randint", "a.entrypoint.randint", st, 0, drt.ctx, drt.failureOpitonsCtx,
		drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["randint"] = append(drt.subComponentRumtimes["randint"], srt)

	stepStarted := false
	patch1 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Start", func(_ *StepRuntime) {
		stepStarted = true
	})
	defer patch1.Reset()

	drt.createAndStartSubComponentRuntime("randint", st, map[int]int{})
	assert.Len(t, drt.subComponentRumtimes, 1)
	assert.Len(t, drt.subComponentRumtimes["randint"], 1)
	assert.False(t, stepStarted)

	dagStarted := false
	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(drt), "Start", func(_ *DagRuntime) {
		dagStarted = true
	})
	defer patch2.Reset()

	drt.getworkflowSouceDag().EntryPoints["square-loop"].UpdateLoopArguemt([]int{1, 2, 3})
	drt.createAndStartSubComponentRuntime("square-loop", drt.getworkflowSouceDag().EntryPoints["square-loop"], map[int]int{})

	time.Sleep(time.Millisecond * 100)
	assert.True(t, dagStarted)
	assert.Len(t, drt.subComponentRumtimes, 2)
	assert.Len(t, drt.subComponentRumtimes["square-loop"], 3)
	assert.Equal(t, drt.subComponentRumtimes["square-loop"][0].getFullName(), "a.entrypoint.square-loop")
	assert.Equal(t, drt.subComponentRumtimes["square-loop"][1].getName(), "a.entrypoint.square-loop-1")
}

func TestDagRuntimeStart(t *testing.T) {
	eventChan := make(chan WorkflowEvent)
	drt, err := mockerDagRuntime(eventChan)
	assert.Nil(t, err)

	ep := &WorkflowEvent{}
	go mockToListenEvent(eventChan, ep)

	// 这里只测试 condition 为false 和 disable 的情况，因此节点正常调度不能被mock
	drt.UpdateCondition("10 > 11")

	drt.Start()
	time.Sleep(time.Millisecond * 100)

	assert.True(t, drt.isSkipped())
	assert.True(t, strings.Contains(ep.Message, "is false"))

	drt.WorkflowSource.Disabled = "square-loop"
	drt.getworkflowSouceDag().EntryPoints["square-loop"].UpdateLoopArguemt([]int{1})
	drt2 := NewDagRuntime("a.entrypoint.square-loop", "a.entrypoint.square-loop",
		drt.getworkflowSouceDag().EntryPoints["square-loop"].(*schema.WorkflowSourceDag),
		0, drt.ctx, drt.failureOpitonsCtx, eventChan, drt.runConfig, drt.ID)

	drt2.Start()
	go mockToListenEvent(eventChan, ep)
	time.Sleep(time.Millisecond * 100)

	assert.True(t, drt2.isSkipped())
	assert.True(t, strings.Contains(ep.Message, "disabled"))

}

func TestScheduleSubComponent(t *testing.T) {
	eventChan := make(chan WorkflowEvent)
	drt, err := mockerDagRuntime(eventChan)
	assert.Nil(t, err)

	drt.getworkflowSouceDag().EntryPoints["square-loop"].UpdateLoopArguemt([]int{1, 2, 3})

	stepStarted := false
	var srt *StepRuntime
	patch1 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Start", func(_ *StepRuntime) {
		stepStarted = true
		fmt.Println("param", drt.parallelismManager.CurrentParallelism())
	})
	defer patch1.Reset()

	dagStarted := false
	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(drt), "Start", func(_ *DagRuntime) {
		dagStarted = true
	})
	defer patch2.Reset()

	drt.scheduleSubComponent()
	time.Sleep(time.Millisecond * 100)

	assert.Len(t, drt.subComponentRumtimes, 2)
	assert.Len(t, drt.subComponentRumtimes["randint"], 1)
	assert.Len(t, drt.subComponentRumtimes["disStep"], 1)
	assert.True(t, stepStarted)

	drt.parallelismManager.increase()
	drt.parallelismManager.increase()

	drt.subComponentRumtimes["disStep"][0].updateStatus(StatusRuntimeSucceeded)
	drt.subComponentRumtimes["randint"][0].updateStatus(StatusRuntimeSucceeded)

	drt.updateStatus(StatusRuntimeTerminating)
	drt.scheduleSubComponent()
	assert.Len(t, drt.subComponentRumtimes, 2)
	assert.Len(t, drt.subComponentRumtimes["randint"], 1)
	assert.Len(t, drt.subComponentRumtimes["disStep"], 1)

	drt.updateStatus(StatusRuntimeRunning)
	drt.scheduleSubComponent()
	time.Sleep(time.Millisecond * 100)

	assert.Len(t, drt.subComponentRumtimes, 4)
	assert.Len(t, drt.subComponentRumtimes["split-by-threshold"], 1)
	assert.Len(t, drt.subComponentRumtimes["square-loop"], 3)
	assert.True(t, dagStarted)

	v1, err := drt.subComponentRumtimes["square-loop"][0].(*DagRuntime).getPFLoopArgument()
	v2, err := drt.subComponentRumtimes["square-loop"][1].(*DagRuntime).getPFLoopArgument()
	assert.Equal(t, v1, 1)
	assert.Equal(t, v2, 2)
}

func TestUpdateStatusAccordingSubComponentRuntimeStatus(t *testing.T) {
	eventChan := make(chan WorkflowEvent)
	drt, err := mockerDagRuntime(eventChan)
	assert.Nil(t, err)

	drt.updateStatusAccordingSubComponentRuntimeStatus()
	assert.Equal(t, drt.status, StatusRuntimeRunning)

	st := drt.getworkflowSouceDag().EntryPoints["randint"].(*schema.WorkflowSourceStep)
	srt := NewStepRuntime("a.entrypoint.randint", "a.entrypoint.randint", st, 0, drt.ctx, drt.failureOpitonsCtx,
		drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["randint"] = append(drt.subComponentRumtimes["randint"], srt)
	srt.increase()
	srt.updateStatus(StatusRuntimeSucceeded)

	srt2 := NewStepRuntime("a.entrypoint.disStep", "a.entrypoint.disStep", st, 0, drt.ctx, drt.failureOpitonsCtx,
		drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["disStep"] = append(drt.subComponentRumtimes["randint"], srt2)
	srt2.increase()
	srt2.updateStatus(StatusRuntimeRunning)

	drt.updateStatusAccordingSubComponentRuntimeStatus()
	assert.Equal(t, drt.status, StatusRuntimeRunning)

	drt.updateStatus(StatusRuntimeTerminating)
	assert.Equal(t, drt.status, StatusRuntimeTerminating)

	srt2.updateStatus(StatusRuntimeSucceeded)

	drt.getworkflowSouceDag().EntryPoints["square-loop"].UpdateLoopArguemt([]int{1})
	drt2 := NewDagRuntime("a.entrypoint.square-loop", "a.entrypoint.square-loop",
		drt.getworkflowSouceDag().EntryPoints["square-loop"].(*schema.WorkflowSourceDag),
		0, drt.ctx, drt.failureOpitonsCtx, drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["square-loop"] = append(drt.subComponentRumtimes["square-loop"], drt2)
	drt2.updateStatus(StatusRuntimeSucceeded)

	st3 := drt.getworkflowSouceDag().EntryPoints["sum"].(*schema.WorkflowSourceStep)
	srt3 := NewStepRuntime("a.entrypoint.sum", "a.entrypoint.sum", st3, 0, drt.ctx, drt.failureOpitonsCtx,
		drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["sum"] = append(drt.subComponentRumtimes["sum"], srt3)
	srt3.increase()
	srt3.updateStatus(StatusRuntimeSucceeded)

	st4 := drt.getworkflowSouceDag().EntryPoints["process-negetive"].(*schema.WorkflowSourceStep)
	srt4 := NewStepRuntime("a.entrypoint.process-negetive", "a.entrypoint.process-negetive", st4, 0, drt.ctx,
		drt.failureOpitonsCtx, drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["process-negetive"] = append(drt.subComponentRumtimes["process-negetive"], srt4)
	srt4.increase()
	srt4.updateStatus(StatusRuntimeSucceeded)

	drt.getworkflowSouceDag().EntryPoints["process-positive"].UpdateLoopArguemt([]int{})
	st5 := drt.getworkflowSouceDag().EntryPoints["process-positive"].(*schema.WorkflowSourceDag)
	srt5 := NewDagRuntime("a.entrypoint.process-positive", "a.entrypoint.process-positive",
		st5, 0, drt.ctx, drt.failureOpitonsCtx, drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["process-positive"] = append(drt.subComponentRumtimes["process-positive"], srt5)
	srt5.increase()
	srt5.updateStatus(StatusRuntimeSucceeded)

	st6 := drt.getworkflowSouceDag().EntryPoints["split-by-threshold"].(*schema.WorkflowSourceStep)
	srt6 := NewStepRuntime("a.entrypoint.split-by-threshold", "a.entrypoint.split-by-threshold", st6, 0, drt.ctx,
		drt.failureOpitonsCtx, drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["split-by-threshold"] = append(drt.subComponentRumtimes["split-by-threshold"], srt6)
	srt6.increase()
	srt6.updateStatus(StatusRuntimeSucceeded)

	drt.updateStatusAccordingSubComponentRuntimeStatus()
	assert.Equal(t, drt.status, StatusRuntimeSucceeded)

	// 测试循环结构只有一个节点运行成功的情况
	drt.getworkflowSouceDag().EntryPoints["square-loop"].UpdateLoopArguemt([]int{1, 2})
	drt.done = false
	drt.updateStatusAccordingSubComponentRuntimeStatus()
	assert.Equal(t, drt.status, StatusRuntimeRunning)

	drt3 := NewDagRuntime("a.entrypoint.square-loop", "a.entrypoint.square-loop",
		drt.getworkflowSouceDag().EntryPoints["square-loop"].(*schema.WorkflowSourceDag),
		1, drt.ctx, drt.failureOpitonsCtx, drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["square-loop"] = append(drt.subComponentRumtimes["square-loop"], drt3)
	drt3.updateStatus(StatusRuntimeSucceeded)

	drt.updateStatusAccordingSubComponentRuntimeStatus()
	assert.Equal(t, drt.status, StatusRuntimeSucceeded)

	drt.done = false
	drt3.done = false
	drt3.updateStatus(StatusRuntimeFailed)

	drt.updateStatusAccordingSubComponentRuntimeStatus()
	assert.Equal(t, drt.status, StatusRuntimeFailed)

	drt.done = false
	drt.updateStatus(StatusRuntimeTerminating)

	drt.updateStatusAccordingSubComponentRuntimeStatus()
	assert.Equal(t, drt.status, StatusRuntimeFailed)

	drt.done = false
	drt3.done = false
	drt.updateStatus(StatusRuntimeTerminating)
	drt3.updateStatus(StatusRuntimeTerminated)

	drt.updateStatusAccordingSubComponentRuntimeStatus()

	assert.Equal(t, drt.status, StatusRuntimeTerminated)

	drt.done = false
	drt3.done = false
	drt.updateStatus(StatusRuntimeTerminating)
	drt3.updateStatus(StatusRuntimeCancelled)
	drt.updateStatusAccordingSubComponentRuntimeStatus()
	assert.Equal(t, drt.status, StatusRuntimeTerminated)

	drt.done = false
	drt2.done = false
	drt3.done = false
	drt3.updateStatus(StatusRuntimeTerminated)
	drt2.updateStatus(StatusRuntimeFailed)

	drt.updateStatusAccordingSubComponentRuntimeStatus()
	assert.Equal(t, drt.status, StatusRuntimeFailed)

	drt.done = false
	drt2.done = false
	drt3.done = false
	drt.updateStatus(StatusRuntimeRunning)
	drt3.updateStatus(StatusRuntimeSucceeded)
	drt2.updateStatus(StatusRuntimeSucceeded)

	drt.updateStatusAccordingSubComponentRuntimeStatus()
	assert.Equal(t, drt.status, StatusRuntimeSucceeded)
}

func TestDagRunRestart(t *testing.T) {
	eventChan := make(chan WorkflowEvent)
	drt, err := mockerDagRuntime(eventChan)
	assert.Nil(t, err)

	drt.updateStatus(StatusRuntimePending)
	drt.getworkflowSouceDag().EntryPoints["process-positive"].UpdateLoopArguemt([]int{})
	drt.getworkflowSouceDag().EntryPoints["square-loop"].UpdateLoopArguemt([]int{1, 2})
	drt.WorkflowSource.Components["process-negetive"].UpdateLoopArguemt([]int{1})
	drt.getworkflowSouceDag().EntryPoints["split-by-threshold"].UpdateLoopArguemt([]int{1, 2, 3})

	// 测试根据 dagView 开始调度dag
	stepStarted := false
	var srt *StepRuntime
	patch1 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Restart", func(srt *StepRuntime, _ *schema.JobView) {
		stepStarted = true
		srt.updateStatus(StatusRuntimeRunning)
		fmt.Println("param", drt.parallelismManager.CurrentParallelism())
	})
	defer patch1.Reset()

	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Start", func(srt *StepRuntime) {
		srt.updateStatus(StatusRuntimeRunning)
		fmt.Println("param", drt.parallelismManager.CurrentParallelism())
	})
	defer patch2.Reset()

	// 一个节点失败，一个节点全部cancelled
	drt.done = false
	dagView := schema.DagView{
		EntryPoints: map[string][]schema.ComponentView{
			"randint": []schema.ComponentView{
				&schema.JobView{
					Status:   StatusRuntimeSucceeded,
					LoopSeq:  0,
					StepName: "randint",
				},
			},
			"sum": []schema.ComponentView{
				&schema.JobView{
					Status:   StatusRuntimeSucceeded,
					LoopSeq:  0,
					StepName: "sum",
				},
			},
			"square-loop": []schema.ComponentView{
				&schema.DagView{
					LoopSeq: 0,
					Status:  StatusRuntimeSucceeded,
					DagName: "square-loop",
				},
				&schema.DagView{
					LoopSeq: 1,
					Status:  StatusRuntimeSucceeded,
					DagName: "square-loop",
				},
			},
			"split-by-threshold": []schema.ComponentView{
				&schema.JobView{
					Status:   StatusRuntimeFailed,
					LoopSeq:  0,
					StepName: "split-by-threshold",
				},
				&schema.JobView{
					Status:   StatusRuntimeCancelled,
					LoopSeq:  1,
					StepName: "split-by-threshold",
				}, &schema.JobView{
					Status:   StatusRuntimeFailed,
					LoopSeq:  2,
					StepName: "split-by-threshold",
				},
			},
			"process-positive": []schema.ComponentView{
				&schema.DagView{
					LoopSeq: 0,
					Status:  StatusRuntimeCancelled,
					DagName: "process-positive",
				},
			},
			"process-negetive": []schema.ComponentView{
				&schema.DagView{
					Status:  StatusRuntimeCancelled,
					LoopSeq: 0,
					DagName: "process-negetive",
				},
			},
			"disStep": []schema.ComponentView{
				&schema.JobView{
					Status:   StatusRuntimeSkipped,
					LoopSeq:  0,
					StepName: "disStep",
				},
			},
		},
	}

	drt.done = false
	drt.status = StatusRuntimeInit
	drt.subComponentRumtimes = map[string][]componentRuntime{}

	drt.Restart(&dagView)
	time.Sleep(time.Millisecond * 100)

	assert.Equal(t, drt.status, StatusRuntimeRunning)
	assert.True(t, stepStarted)
	assert.Len(t, drt.subComponentRumtimes, 5)

	assert.NotContains(t, drt.subComponentRumtimes, "process-negetive")
	assert.NotContains(t, drt.subComponentRumtimes, "process-positive")

	assert.Len(t, drt.subComponentRumtimes["split-by-threshold"], 3)
	for _, rt := range drt.subComponentRumtimes["split-by-threshold"] {
		assert.Equal(t, rt.getStatus(), StatusRuntimeRunning)
	}
}

func TestProcessEventFromSubComponent(t *testing.T) {
	eventChan := make(chan WorkflowEvent)
	drt, err := mockerDagRuntime(eventChan)
	assert.Nil(t, err)

	ep := &WorkflowEvent{}

	failed := false
	patch1 := gomonkey.ApplyMethod(reflect.TypeOf(drt), "ProcessFailureOptions",
		func(_ *DagRuntime, _ WorkflowEvent) {
			failed = true
			fmt.Println("param", drt.parallelismManager.CurrentParallelism())
		})
	defer patch1.Reset()

	var srt *StepRuntime
	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Start", func(_ *StepRuntime) {
		fmt.Println("param", drt.parallelismManager.CurrentParallelism())
	})
	defer patch2.Reset()

	go mockToListenEvent(eventChan, ep)
	time.Sleep(time.Millisecond * 100)

	assert.Equal(t, ep.Message, "")
	assert.False(t, failed)

	drt.done = false
	drt.updateStatus(StatusRuntimeInit)

	event := NewWorkflowEvent(WfEventJobUpdate, "hahaha", map[string]interface{}{})
	drt.processEventFromSubComponent(*event)

	go mockToListenEvent(eventChan, ep)
	time.Sleep(time.Millisecond * 500)
	assert.Equal(t, ep.Message, "")
	assert.Equal(t, ep.Event, WfEventDagUpdate)
	assert.False(t, failed)

	event = NewWorkflowEvent(WfEventFailureOptionsTriggered, "hahaha", map[string]interface{}{})
	drt.processEventFromSubComponent(*event)

	go mockToListenEvent(eventChan, ep)
	time.Sleep(time.Millisecond * 100)
	assert.True(t, failed)

	failed = false
	drt.done = false
	drt.updateStatus(StatusRuntimeInit)
	event = NewWorkflowEvent(WfEventJobUpdate, "hahaha222", map[string]interface{}{
		apicommon.WfEventKeyStatus: StatusRuntimeFailed,
	})
	drt.processEventFromSubComponent(*event)

	go mockToListenEvent(eventChan, ep)
	time.Sleep(time.Millisecond * 100)
	assert.True(t, failed)
}

func TestAllDownStream(t *testing.T) {

	eventChan := make(chan WorkflowEvent)
	drt, err := mockerDagRuntime(eventChan)
	assert.Nil(t, err)

	var srt *StepRuntime
	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Start", func(_ *StepRuntime) {
		fmt.Println("param", drt.parallelismManager.CurrentParallelism())
	})
	defer patch2.Reset()

	cp := drt.getworkflowSouceDag().EntryPoints["disStep"]
	downs := drt.getAllDownstreamComponents(cp)

	assert.Len(t, downs, 0)

	cp = drt.getworkflowSouceDag().EntryPoints["randint"]
	downs = drt.getAllDownstreamComponents(cp)
	assert.Len(t, downs, 5)
	assert.Contains(t, downs, "process-negetive")
	assert.Contains(t, downs, "sum")
	assert.Contains(t, downs, "square-loop")

	cp = drt.getworkflowSouceDag().EntryPoints["square-loop"]
	downs = drt.getAllDownstreamComponents(cp)
	assert.Len(t, downs, 1)
	assert.Contains(t, downs, "sum")
}

func TestProcessFailureOptions(t *testing.T) {
	eventChan := make(chan WorkflowEvent)
	drt, err := mockerDagRuntime(eventChan)
	assert.Nil(t, err)

	drt3 := NewDagRuntime("a.entrypoint.square-loop", "a.entrypoint.square-loop",
		drt.getworkflowSouceDag().EntryPoints["square-loop"].(*schema.WorkflowSourceDag),
		1, drt.ctx, drt.failureOpitonsCtx, drt.receiveEventChildren, drt.runConfig, drt.ID)
	drt.subComponentRumtimes["square-loop"] = append(drt.subComponentRumtimes["square-loop"], drt3)

	event := NewWorkflowEvent(WfEventJobUpdate, "failed", map[string]interface{}{
		apicommon.WfEventKeyStatus:        StatusRuntimeFailed,
		apicommon.WfEventKeyComponentName: "square-loop",
	})
	drt.WorkflowSource.FailureOptions.Strategy = "continue"

	drt.ProcessFailureOptions(*event)
	time.Sleep(time.Millisecond * 100)

	assert.Len(t, drt.subComponentRumtimes, 2)
	assert.Contains(t, drt.subComponentRumtimes, "sum")

	drt.WorkflowSource.FailureOptions.Strategy = "failed"
	drt.hasFailureOptionsTriggered = false
	drt.subComponentRumtimes = map[string][]componentRuntime{}
	drt.subComponentRumtimes["square-loop"] = append(drt.subComponentRumtimes["square-loop"], drt3)

	drt.ProcessFailureOptions(*event)
	time.Sleep(time.Millisecond * 100)

	assert.Len(t, drt.subComponentRumtimes, 7)
}
