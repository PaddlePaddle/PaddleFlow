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
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

// 工作流运行时
type WorkflowRuntime struct {
	*runConfig
	entryPointsCtx        context.Context
	entryPointsctxCancel  context.CancelFunc
	entryPointsFailCancel context.CancelFunc
	postProcessPointsCtx  context.Context
	postProcessctxCancel  context.CancelFunc
	postProcessFailCancel context.CancelFunc
	entryPoints           *DagRuntime
	postProcess           *StepRuntime
	status                string
	EventChan             chan WorkflowEvent
	pk                    int64
	startTime             string

	// 主要用于避免在调度节点的同时遇到终止任务的情况
	scheduleLock sync.Mutex
}

func NewWorkflowRuntime(rc *runConfig) *WorkflowRuntime {
	entryCtx, entryCtxCancel := context.WithCancel(context.Background())
	postCtx, postCtxCancel := context.WithCancel(context.Background())
	failureOptionsCtx, cancel := context.WithCancel(context.Background())

	EventChan := make(chan WorkflowEvent)

	wfr := &WorkflowRuntime{
		runConfig:             rc,
		entryPointsCtx:        entryCtx,
		entryPointsctxCancel:  entryCtxCancel,
		entryPointsFailCancel: cancel,
		postProcessPointsCtx:  postCtx,
		postProcessctxCancel:  postCtxCancel,
		EventChan:             EventChan,
		scheduleLock:          sync.Mutex{},
	}

	epName := wfr.generateEntryPointFullName()
	entryPoints := NewDagRuntime(epName, epName, &rc.WorkflowSource.EntryPoints, 0, entryCtx, failureOptionsCtx,
		EventChan, rc, "")

	wfr.entryPoints = entryPoints
	wfr.status = common.StatusRunPending

	// TODO： 此时是否需要与父节点以及 apiserver 进行同步？
	// wfr.callback("finished init, update status to pending")

	return wfr
}

func (wfr *WorkflowRuntime) catchPanic() {
	if r := recover(); r != nil {
		msg := fmt.Sprintf("Inner Error occured at dagruntime[%s]: %v", wfr.WorkflowSource.Name, r)
		trace_logger.KeyWithUpdate(wfr.runID).Errorf(msg)

		wfr.entryPointsCtx.Done()
		wfr.postProcessPointsCtx.Done()

		wfr.callback(msg)
	}
}

func (wfr *WorkflowRuntime) generateEntryPointFullName() string {
	return wfr.WorkflowSource.Name + ".entry_points"
}

func (wfr *WorkflowRuntime) generatePostProcessFullName(name string) string {
	return wfr.WorkflowSource.Name + ".post_process." + name
}

// 运行
func (wfr *WorkflowRuntime) Start() {
	defer wfr.scheduleLock.Unlock()

	wfr.scheduleLock.Lock()

	defer wfr.catchPanic()

	// 处理正式运行前，便收到了 Stop 信号的场景
	if wfr.status == common.StatusRunTerminating || wfr.IsCompleted() {
		wfr.logger.Warningf("the status of run is %s, so it won't start run", wfr.status)
	} else {
		wfr.status = common.StatusRunRunning
		wfr.startTime = time.Now().Format("2006-01-02 15:04:05")

		wfr.callback("begin to running, update status to running")

		go wfr.Listen()
		wfr.entryPoints.Start()
	}
}

func (wfr *WorkflowRuntime) Resume(entryPointView *schema.DagView, postProcessView schema.PostProcessView,
	runStatus string, stopForce bool) {
	defer wfr.scheduleLock.Unlock()
	wfr.scheduleLock.Lock()

	defer wfr.catchPanic()

	wfr.status = runStatus

	wfr.startTime = entryPointView.StartTime

	// 1、如果 ep 未处于终态， 则需要重启ep
	if !isRuntimeFinallyStatus(entryPointView.Status) {
		go wfr.entryPoints.Resume(entryPointView)
		go wfr.Listen()

		if runStatus == string(StatusRuntimeTerminating) {
			wfr.entryPointsCtx.Done()
		}

		return
	} else {
		err := wfr.entryPoints.updateStatus(entryPointView.Status)
		if err != nil {
			wfr.logger.Errorf("update entrypoint status failed: %s", err.Error())
		}
	}

	// 2、判断是否有 postProcess 节点，有的话则需要判断其状态决定是否运行
	if len(wfr.WorkflowSource.PostProcess) != 0 {
		for name, view := range postProcessView {
			if !isRuntimeFinallyStatus(view.Status) {
				step := wfr.WorkflowSource.PostProcess[name]
				failureOptionsCtx, cancel := context.WithCancel(context.Background())
				wfr.postProcessFailCancel = cancel

				postName := wfr.generatePostProcessFullName(name)
				postStep, err := NewReferenceSolver(wfr.WorkflowSource).resolveComponentReference(step)
				if err != nil {
					newStepRuntimeWithStatus(postName, postName, step, 0, wfr.postProcessPointsCtx, failureOptionsCtx,
						wfr.EventChan, wfr.runConfig, "", StatusRuntimeFailed, err.Error())
				}

				postProcess := NewStepRuntime(postName, postName, postStep.(*schema.WorkflowSourceStep), 0,
					wfr.postProcessPointsCtx, failureOptionsCtx, wfr.EventChan, wfr.runConfig, "")
				wfr.postProcess = postProcess

				wfr.postProcess.Resume(view)
			}
		}

		if runStatus == string(StatusRuntimeTerminating) && stopForce {
			wfr.postProcessPointsCtx.Done()
		}
		go wfr.Listen()
		return
	}

	// 统计状态，同步至 Server
	for _, view := range postProcessView {
		if len(wfr.WorkflowSource.PostProcess) != 0 {
			err := wfr.postProcess.updateStatus(view.Status)
			if err != nil {
				wfr.logger.Errorf("update postProcess status failed: %s", err.Error())
			}
		}
	}
	wfr.updateStatusAccordingComponentStatus()
	wfr.callback("update status after resum")

	return
}

// Restart: 重新运行
func (wfr *WorkflowRuntime) Restart(entryPointView *schema.DagView,
	postProcessView schema.PostProcessView) {
	defer wfr.scheduleLock.Unlock()
	wfr.scheduleLock.Lock()

	defer wfr.catchPanic()

	wfr.status = common.StatusRunRunning
	wfr.startTime = time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf("restart run[%s], and update status to [%s]", wfr.runID, wfr.status)
	wfr.logger.Infof(msg)
	wfr.callback(msg)

	if entryPointView.Status != StatusRuntimeSucceeded {
		wfr.entryPoints.Restart(entryPointView)
		go wfr.Listen()
		return
	} else {
		// 此时 postPost节点的状态一定为 异常状态，直接重新调度 postProcess 即可
		err := wfr.entryPoints.updateStatus(StatusRuntimeSucceeded)
		if err != nil {
			wfr.logger.Errorf("update postProcess status failed: %s", err.Error())
		}
		wfr.schedulePostProcess()
		go wfr.Listen()
		return
	}
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
		wfr.entryPoints.processStartAbnormalStatus("reveice termination signal", StatusRuntimeCancelled)
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
				failureOptionsCtx, cancel := context.WithCancel(context.Background())
				wfr.postProcessFailCancel = cancel

				postName := wfr.generatePostProcessFullName(name)
				wfr.postProcess = newStepRuntimeWithStatus(postName, postName, step, 0, wfr.postProcessPointsCtx, failureOptionsCtx,
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
		event := <-wfr.EventChan
		if err := wfr.processEvent(event); err != nil {
			// how to read event?
			wfr.logger.Debugf("process event failed %s", err.Error())
		}

		if wfr.IsCompleted() {
			return
		}
	}
}

func (wfr *WorkflowRuntime) IsCompleted() bool {
	return wfr.status == common.StatusRunSucceeded ||
		wfr.status == common.StatusRunFailed ||
		wfr.status == common.StatusRunTerminated
}

func (wfr *WorkflowRuntime) schedulePostProcess() {
	wfr.logger.Debugf("begin to start postProcess")
	if wfr.postProcess != nil {
		wfr.logger.Warningf("the postProcess step[%s] has been scheduled", wfr.postProcess.runtimeName)
		return
	} else if len(wfr.WorkflowSource.PostProcess) != 0 {
		for name, step := range wfr.WorkflowSource.PostProcess {
			failureOptionsCtx, cancel := context.WithCancel(context.Background())
			wfr.postProcessFailCancel = cancel

			postName := wfr.generatePostProcessFullName(name)

			postStep, err := NewReferenceSolver(wfr.WorkflowSource).resolveComponentReference(step)
			if err != nil {
				newStepRuntimeWithStatus(postName, postName, step, 0, wfr.postProcessPointsCtx, failureOptionsCtx,
					wfr.EventChan, wfr.runConfig, "", StatusRuntimeFailed, err.Error())
			}

			postProcess := NewStepRuntime(postName, postName, postStep.(*schema.WorkflowSourceStep), 0, wfr.postProcessPointsCtx, failureOptionsCtx, wfr.EventChan,
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
	defer wfr.scheduleLock.Unlock()
	wfr.scheduleLock.Lock()

	if wfr.entryPoints.isDone() {
		wfr.schedulePostProcess()
	}

	wfr.updateStatusAccordingComponentStatus()
	wfr.callback(event.Message)

	return nil
}

// TODO: 并发状态下的状态一致性
func (wfr *WorkflowRuntime) updateStatusAccordingComponentStatus() {
	// 只有当所有的 节点都处于终态后，此函数才会更新 run 的状态
	// 有failed step，run 状态为failed
	// 如果当前状态为 terminating，存在有 cancelled step 或者 terminated step，run 状态为terminated
	// 其余情况都为succeeded，因为：
	// - 有step为 cancelled 状态，要么是因为有节点失败了，要么是用户终止了 Run
	// - 另外skipped 状态的节点也视作运行成功（目前运行所有step都skip，此时run也是为succeeded）
	// - 如果有 Step 的状态为 terminated，但是 run 的状态不为 terminating, 则说明改step 是意外终止，此时 run 的状态应该Failed
	if wfr.IsCompleted() {
		wfr.logger.Errorf("cannot update status for run, because it is already in status[%s]", wfr.status)
		return
	}

	if !wfr.entryPoints.isDone() {
		return
	}

	if len(wfr.WorkflowSource.PostProcess) != 0 {
		if wfr.postProcess == nil || !wfr.postProcess.isDone() {
			return
		}
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

	wfr.logger.Infof("workflow %s finished with status[%s]", wfr.WorkflowSource.Name, wfr.status)
	return
}

func (wfr *WorkflowRuntime) callback(msg string) {
	extra := map[string]interface{}{
		common.WfEventKeyRunID:     wfr.runID,
		common.WfEventKeyStatus:    wfr.status,
		common.WfEventKeyStartTime: wfr.startTime,
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
