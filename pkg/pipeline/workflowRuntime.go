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
	"sync"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

// 工作流运行时
type WorkflowRuntime struct {
	*runConfig
	entryPointsCtx       context.Context
	entryPointsctxCancel context.CancelFunc
	postProcessPointsCtx context.Context
	postProcessctxCancel context.CancelFunc
	entryPoints          *DagRuntime
	postProcess          *StepRuntime
	status               string
	EventChan            chan WorkflowEvent

	pk int64

	// 主要用于避免在调度节点的同时遇到终止任务的情况
	scheduleLock sync.Mutex
}

func NewWorkflowRuntime(rc *runConfig) *WorkflowRuntime {
	entryCtx, entryCtxCancel := context.WithCancel(context.Background())
	postCtx, postCtxCancel := context.WithCancel(context.Background())
	failureOptionsCtx, _ := context.WithCancel(context.Background())

	EventChan := make(chan WorkflowEvent)

	// 对于 EntryPoint 所代表的名字，此时应该没有名字的。
	entryPoint := NewDagRuntime("", &rc.WorkflowSource.EntryPoints, 0, entryCtx, failureOptionsCtx,
		EventChan, rc, "")

	wfr := &WorkflowRuntime{
		runConfig:            rc,
		entryPointsCtx:       entryCtx,
		entryPointsctxCancel: entryCtxCancel,
		postProcessPointsCtx: postCtx,
		postProcessctxCancel: postCtxCancel,
		EventChan:            EventChan,
		entryPoints:          entryPoint,
		scheduleLock:         sync.Mutex{},
	}

	wfr.status = common.StatusRunPending

	wfr.callback("finished init, update status to pending")

	return wfr
}

// 运行
func (wfr *WorkflowRuntime) Start() error {
	defer wfr.scheduleLock.Unlock()
	wfr.scheduleLock.Lock()

	// 处理正式运行前，便收到了 Stop 信号的场景
	if wfr.status == common.StatusRunTerminating || wfr.IsCompleted() {
		wfr.logger.Warningf("the status of run is %s, so it won't start run", wfr.status)
	} else {
		wfr.status = common.StatusRunRunning
		wfr.callback("begin to running, update status to running")

		go wfr.Listen()
		wfr.entryPoints.Start()
	}

	return nil
}

// Restart 从 DB 中恢复重启
func (wfr *WorkflowRuntime) Restart(entryPointView schema.RuntimeView,
	postProcessView schema.PostProcessView) error {
	defer wfr.scheduleLock.Unlock()
	wfr.scheduleLock.Lock()

	wfr.status = common.StatusRunRunning
	msg := fmt.Sprintf("restart run[%s], and update status to [%s]", wfr.runID, wfr.status)
	wfr.logger.Infof(msg)
	wfr.callback(msg)

	// 1、处理entryPoint
	dagView := schema.DagView{
		EntryPoints: entryPointView,
	}

	wfr.entryPoints.updateStatus(StatusRuntimeRunning)

	entryPointRestarted, err := wfr.entryPoints.Restart(dagView)
	if err != nil {
		errMsg := fmt.Sprintf("restart entryPoints failed: %s", err.Error())
		wfr.logger.Errorf(errMsg)
		wfr.status = common.StatusRunFailed
		wfr.callback(errMsg)
		return err
	}

	// 2、处理 PostProcess
	// - 如果此时的 entryPoint 不为终态，则 PostProcess 还没有开始运行，此时，
	// PostProcess 节点会在 entryPoint 处于终态时，由 processEvent 函数驱动
	// 因此我们在此处只需关注 entryPoint 已经处于终态（主要值 succeede， 其余终态entryPoint 都会重启）的情况。
	if !entryPointRestarted {
		for name, step := range wfr.WorkflowSource.PostProcess {
			failureOptionsCtx, _ := context.WithCancel(context.Background())
			postProcess := NewStepRuntime(name, step, 0, wfr.postProcessPointsCtx, failureOptionsCtx, wfr.EventChan,
				wfr.runConfig, "")
			wfr.postProcess = postProcess
		}

		// 如果没有 postProcess, 则说明 run 已经运行成功了
		if wfr.postProcess == nil {
			wfr.status = string(StatusRuntimeSucceeded)
			msg := fmt.Sprintf("there is no need to restart, because it is already success")
			wfr.logger.Infof(msg)
			wfr.callback(msg)
		}

		for _, view := range postProcessView {

			postProcessRestarted, err := wfr.postProcess.Restart(view)
			if err != nil {
				errMsg := fmt.Sprintf("restart postProcess failed: %s", err.Error())
				wfr.logger.Errorf(errMsg)
				wfr.status = common.StatusRunFailed
				wfr.callback(errMsg)
				return err
			}

			// 如果没有重启，则说明 postProcess 已经运行成功了。
			if !postProcessRestarted {
				wfr.status = string(StatusRuntimeSucceeded)
				msg := fmt.Sprintf("there is no need to restart, because it is already success")
				wfr.logger.Infof(msg)
				wfr.callback(msg)
			}
		}
	}

	go wfr.Listen()

	return nil
}

// Stop 停止 Workflow
// do not call ctx_cancel(), which will be called when all steps has terminated eventually.
// 这里不通过 cancel channel 去取消 Step 的原因是防止有多个地方向通过一个 channel 传递东西，防止runtime hang 住
func (wfr *WorkflowRuntime) Stop(force bool) error {
	if wfr.IsCompleted() {
		wfr.logger.Debugf("workflow has finished.")
		return nil
	}

	defer wfr.scheduleLock.Unlock()
	wfr.scheduleLock.Lock()

	// 1、 终止entryPoint
	// 1.1、 处理已经开始运行情况
	if wfr.status == common.StatusRunRunning {
		wfr.status = common.StatusRunTerminating
		wfr.callback("receive termination signal, update status to terminating")

		wfr.entryPointsctxCancel()
	} else {
		// 2、 处理还没有开始运行的情况
		wfr.status = common.StatusRunTerminating
		wfr.callback("receive termination signal, update status to terminating")

		failureOptionsCtx, _ := context.WithCancel(context.Background())

		newDagRuntimeWithStatus("", wfr.entryPoints.getworkflowSouceDag(), 0, wfr.entryPointsCtx,
			failureOptionsCtx, wfr.EventChan, wfr.runConfig, "", StatusRuntimeCancelled,
			"reveice termination signal")
	}

	// 处理 PostProcess
	if force {
		wfr.logger.Info("begin to stop postProcess step")
		// 如果 postProcess 已经调度，则直接终止
		if wfr.postProcess != nil {
			defer wfr.postProcessctxCancel()
		} else {
			// 如果在创建 stepRuntime时，直接给定状态为 Cancelled
			for name, step := range wfr.WorkflowSource.PostProcess {
				failureOptionsCtx, _ := context.WithCancel(context.Background())
				newStepRuntimeWithStatus(name, step, 0, wfr.postProcessPointsCtx, failureOptionsCtx,
					wfr.EventChan, wfr.runConfig, "", StatusRuntimeCancelled, "reveice termination signal")
			}
		}
	}

	return nil
}

func (wfr *WorkflowRuntime) Status() string {
	return wfr.status
}

func (wfr *WorkflowRuntime) Listen() {
	for {
		select {
		case event := <-wfr.EventChan:
			if err := wfr.processEvent(event); err != nil {
				// how to read event?
				wfr.logger.Debugf("process event failed %s", err.Error())
			}
			if wfr.IsCompleted() {
				return
			}
		}
	}
}

func (wfr *WorkflowRuntime) IsCompleted() bool {
	return wfr.status == common.StatusRunSucceeded ||
		wfr.status == common.StatusRunFailed ||
		wfr.status == common.StatusRunTerminated
}

func (wfr *WorkflowRuntime) schedulePostProcess() {
	defer wfr.scheduleLock.Unlock()
	wfr.scheduleLock.Lock()

	if wfr.postProcess != nil {
		wfr.logger.Warningf("the postProcess step[%s] has been scheduled", wfr.postProcess.name)
		return
	} else if len(wfr.WorkflowSource.PostProcess) != 0 {
		for name, step := range wfr.WorkflowSource.PostProcess {
			failureOptionsCtx, _ := context.WithCancel(context.Background())
			postProcess := NewStepRuntime(name, step, 0, wfr.postProcessPointsCtx, failureOptionsCtx, wfr.EventChan,
				wfr.runConfig, "")
			wfr.postProcess = postProcess
		}
		msg := fmt.Sprintf("begin to execute postProcess step [%s]", wfr.postProcess.name)
		wfr.logger.Infof(msg)
		wfr.postProcess.Start()
	} else {
		wfr.logger.Infof("there is no postProcess step")
	}

	return
}

// processEvent 处理 job 推送到 run 的事件
// 对于异常处理的情况
// 1. 提交失败，job id\status 都为空，视为 job 失败，更新 run message 字段
// 2. watch 失败，状态不更新，更新 run message 字段；等 job 恢复服务之后，job watch 恢复，run 自动恢复调度
// 3. stop 失败，状态不更新，run message 字段；需要用户根据提示再次调用 stop
func (wfr *WorkflowRuntime) processEvent(event WorkflowEvent) error {
	if wfr.IsCompleted() {
		wfr.logger.Debugf("workflow has completed. skip event")
		return nil
	}
	wfr.logger.Infof("process event: [%+v]", event)

	// 1. 如果有entryPoints 处于 Running 状态，此时只需做好信息的同步即可
	// 2. 如果 entryPoints 处于终态，但是postProcess 还没有开始执行，此时则应该开始执行 PostProcess 节点
	// 3. 如果 entryPoints 处于终态，且PostProcess 处于中间态，则只需做好信息同步即可
	// 4. 如果 entryPoints 和 postProcess 均处于终态，则会更新 Run 的状态
	if wfr.entryPoints.isDone() {
		wfr.schedulePostProcess()
	}

	wfr.updateStatusAccordingComponentStatus()
	wfr.callback(event.Message)

	return nil
}

func (wfr *WorkflowRuntime) updateStatusAccordingComponentStatus() {
	// 只有当所有的 节点都处于终态后，此函数才会更新 run 的状态
	// 有failed step，run 状态为failed
	// 如果当前状态为 terminating，存在有 cancelled step 或者 terminated step，run 状态为terminated
	// 其余情况都为succeeded，因为：
	// - 有step为 cancelled 状态，要么是因为有节点失败了，要么是用户终止了 Run
	// - 另外skipped 状态的节点也视作运行成功（目前运行所有step都skip，此时run也是为succeeded）
	// - 如果有 Step 的状态为 terminated，但是 run 的状态不为 terminating, 则说明改step 是意外终止，此时 run 的状态应该Failed

	if !wfr.entryPoints.isDone() || (wfr.postProcess != nil && !wfr.postProcess.isDone()) {
		return
	}

	hasFailedComponent := wfr.entryPoints.isFailed() ||
		(wfr.postProcess != nil && wfr.postProcess.isFailed())
	hasTerminatedComponent := wfr.entryPoints.isTerminated() ||
		(wfr.postProcess != nil && wfr.postProcess.isTerminated())
	hasCancelledComponent := wfr.entryPoints.isCancelled() ||
		(wfr.postProcess != nil && wfr.postProcess.isCancelled())

	if hasFailedComponent {
		wfr.status = common.StatusRunFailed
	} else if hasTerminatedComponent || hasCancelledComponent {
		if wfr.status == common.StatusRunTerminating {
			wfr.status = common.StatusRunTerminated
		} else {
			wfr.status = common.StatusRunFailed
		}
	} else {
		wfr.status = common.StatusRunSucceeded
	}

	wfr.logger.Debugf("workflow %s finished with status[%s]", wfr.WorkflowSource.Name, wfr.status)
	return
}

func (wfr *WorkflowRuntime) callback(msg string) {
	extra := map[string]interface{}{
		common.WfEventKeyRunID:  wfr.runID,
		common.WfEventKeyStatus: wfr.status,
	}

	wfEvent := NewWorkflowEvent(WfEventRunUpdate, msg, extra)
	for i := 0; i < 3; i++ {
		wfr.logger.Infof("callback run event [%+v]", wfEvent)
		var success bool
		if wfr.pk, success = wfr.callbacks.UpdateRuntimeCb(wfr.runID, wfEvent); success {
			break
		}
	}
}
