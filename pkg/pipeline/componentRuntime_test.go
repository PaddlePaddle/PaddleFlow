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
	"testing"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	. "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
	"github.com/stretchr/testify/assert"
)

func mockForPara(pm *parallelismManager, action string) {
	if action == "ic" {
		pm.increase()
		fmt.Println("increase at:", time.Now())
	}

	if action == "dc" {
		pm.decrease()
	}
}

func TestParallelismManager(t *testing.T) {
	pm := NewParallelismManager(5)
	for i := 0; i < 10; i++ {
		go mockForPara(pm, "ic")
	}

	time.Sleep(time.Millisecond * 200)

	assert.Equal(t, 5, pm.CurrentParallelism())

	for i := 0; i < 10; i++ {
		go mockForPara(pm, "dc")
	}

	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, 0, pm.CurrentParallelism())
}

// 测试 ComponentRuntime

func mockRunConfigForComponentRuntime() *runConfig {
	mainFS := schema.FsMount{
		ID:      "fs-fs",
		Name:    "fs",
		SubPath: "/testcase",
	}
	return &runConfig{
		mainFS:             &mainFS,
		userName:           "xiaoming",
		logger:             logger.LoggerForRun("componentRunt"),
		runID:              "run-000001",
		parallelismManager: NewParallelismManager(50),
		callbacks:          mockCbs,
	}
}

func mockBaseComponentRuntime() *baseComponentRuntime {
	rf := mockRunConfigForComponentRuntime()
	ws := mockComponentForInnerSolver()

	failureOptionsctx, _ := context.WithCancel(context.Background())

	return NewBaseComponentRuntime("a.entrypoint.step1", "a.entrypoint.step1", ws, 0, context.Background(), failureOptionsctx,
		make(chan WorkflowEvent), rf, "0")
}

func TestUpdateStatus(t *testing.T) {
	cp := mockBaseComponentRuntime()
	assert.Equal(t, "", string(cp.status))

	cp.updateStatus(StatusRuntimeCancelled)
	assert.True(t, cp.isCancelled())
	assert.True(t, cp.isDone())

	err := cp.updateStatus(StatusRuntimeFailed)
	assert.NotNil(t, err)

	cp.done = false
	cp.updateStatus(StatusRuntimeSkipped)
	assert.True(t, cp.isSkipped())
	assert.True(t, cp.isDone())

	cp.done = false
	cp.updateStatus(StatusRuntimeSucceeded)
	assert.True(t, cp.isSucceeded())
	assert.True(t, cp.isDone())

	cp.done = false
	cp.updateStatus(StatusRuntimeRunning)
	assert.False(t, cp.isDone())
	assert.Equal(t, StatusRuntimeRunning, cp.status)

	cp.updateStatus(StatusRuntimeTerminating)
	assert.False(t, cp.isDone())
	assert.Equal(t, StatusRuntimeTerminating, cp.status)
}

func TestIsDisabled(t *testing.T) {
	cp := mockBaseComponentRuntime()
	cp.runConfig.WorkflowSource = &schema.WorkflowSource{
		Disabled: "step1",
	}
	assert.True(t, cp.isDisabled())

	cp.runConfig.WorkflowSource = &schema.WorkflowSource{
		Disabled: "step2",
	}
	assert.False(t, cp.isDisabled())
}

func TestGetFunc(t *testing.T) {
	cp := mockBaseComponentRuntime()
	ws := mockComponentForInnerSolver()

	assert.Equal(t, ws.GetParameters(), cp.getComponent().GetParameters())

	cp.getComponent().UpdateLoopArguemt([]int{1, 2, 3, 4})
	lp, _ := cp.getPFLoopArgument()
	assert.Equal(t, lp, 1)

	cp.loopSeq = 1
	lp, _ = cp.getPFLoopArgument()
	assert.Equal(t, lp, 2)
	assert.Equal(t, 1, cp.getSeq())

	cp.loopSeq = 0
	assert.Equal(t, "a.entrypoint.step1", cp.getName())
}

func TestSetSysParam(t *testing.T) {
	cp := mockBaseComponentRuntime()
	cp.getComponent().UpdateLoopArguemt([]int{1, 2, 3, 4})
	cp.setSysParams()

	assert.Equal(t, "run-000001", cp.sysParams[SysParamNamePFRunID])
	assert.Equal(t, "1", cp.sysParams[SysParamNamePFLoopArgument])
}

func TestCalculateCondition(t *testing.T) {
	cp := mockBaseComponentRuntime()
	cp.UpdateCondition("10 > 11")

	result, _ := cp.CalculateCondition()
	assert.False(t, result)

	cp.UpdateCondition("10 < 11")

	result, _ = cp.CalculateCondition()
	assert.True(t, result)
}
