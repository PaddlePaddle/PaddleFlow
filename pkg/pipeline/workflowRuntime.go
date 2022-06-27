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

	wfr := &WorkflowRuntime{
		runConfig:            rc,
		entryPointsCtx:       entryCtx,
		entryPointsctxCancel: entryCtxCancel,
		postProcessPointsCtx: postCtx,
		postProcessctxCancel: postCtxCancel,
		EventChan:            EventChan,
		scheduleLock:         sync.Mutex{},
	}

	epName := wfr.generateEntryPointFullName()
	entryPoints := NewDagRuntime(epName, &rc.WorkflowSource.EntryPoints, 0, entryCtx, failureOptionsCtx,
		EventChan, rc, "")

	wfr.entryPoints = entryPoints
	wfr.status = common.StatusRunPending

	wfr.callback("finished init, update status to pending")

	return wfr
}

func (wfr *WorkflowRuntime) generateEntryPointFullName() string {
	return wfr.WorkflowSource.Name + ".entry_points"
}

func (wfr *WorkflowRuntime) generatePostProcessFullName(name string) string {
	return wfr.WorkflowSource.Name + ".post_process." + name
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

	// 2. 这一部分没有意义
	need, err := wfr.entryPoints.needRestart(&dagView)
	if err != nil {
		wfr.status = common.StatusRunFailed
		wfr.callback("cannot decide to whether to restart entryPoints: " + err.Error())
	}

	// 无论 need 是否为True， 调用 Restart 函数来更新 entrypoing 的状态
	go wfr.entryPoints.Restart(&dagView)

	// 只有在 不需要重启 entryPoint 的时候才需要重启 postProcess。
	// 当 entryPoint 需要重启的时候，postProcess 节点，无论如何都需要重新运行一次，此时应该有 processEvent 函数触发
	if need {

		// 如果此时有 postProcess 的节点有对应的job 存在，此时应该尝试终止该job(不保证终止成功), 任务终止相关的信息不会同步值 workflowRuntime，
		// 也不会同步至数据库
		if wfr.runConfig.WorkflowSource.PostProcess != nil {
			for name, view := range postProcessView {
				if view.Status == StatusRuntimeRunning && view.JobID != "" {
					failureOptionsCtx, _ := context.WithCancel(context.Background())
					postName := wfr.generatePostProcessFullName(name)
					postProcess := NewStepRuntime(postName, wfr.WorkflowSource.PostProcess[name], 0, wfr.postProcessPointsCtx,
						failureOptionsCtx, make(chan<- WorkflowEvent), wfr.runConfig, "")
					go postProcess.StopByView(view)
				}
			}
		}

	} else {
		// 理论上不会存在这种情况，因为此时的run 的状态应该是 succeeded
		if wfr.runConfig.WorkflowSource.PostProcess == nil {
			return nil
		} else {
			for name, step := range wfr.WorkflowSource.PostProcess {
				failureOptionsCtx, _ := context.WithCancel(context.Background())
				postName := wfr.generatePostProcessFullName(name)
				postProcess := NewStepRuntime(postName, step, 0, wfr.postProcessPointsCtx, failureOptionsCtx, wfr.EventChan,
					wfr.runConfig, "")
				wfr.postProcess = postProcess

				view := postProcessView[name]
				wfr.postProcess.Restart(view)
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
				postName := wfr.generatePostProcessFullName(name)
				wfr.postProcess = newStepRuntimeWithStatus(postName, step, 0, wfr.postProcessPointsCtx, failureOptionsCtx,
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

	wfr.logger.Debugf("begin to start postProcess")
	if wfr.postProcess != nil {
		wfr.logger.Warningf("the postProcess step[%s] has been scheduled", wfr.postProcess.name)
		return
	} else if len(wfr.WorkflowSource.PostProcess) != 0 {
		for name, step := range wfr.WorkflowSource.PostProcess {
			failureOptionsCtx, _ := context.WithCancel(context.Background())
			postName := wfr.generatePostProcessFullName(name)
			postProcess := NewStepRuntime(postName, step, 0, wfr.postProcessPointsCtx, failureOptionsCtx, wfr.EventChan,
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
	if wfr.IsCompleted() {
		wfr.logger.Errorf("cannot update status for run, because it is already in status[%s]", wfr.status)
		return
	}

	if !wfr.entryPoints.isDone() {
		return
	}

	if wfr.WorkflowSource.PostProcess != nil {
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
