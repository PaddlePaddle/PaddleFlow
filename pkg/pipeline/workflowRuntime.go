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
}

// 生成 entryPoint 所对应的 DAG 的名字，因为用户无法给其指定名字
func generateEntrPointName(pplName string) string {
	// 使用 pipeline 的名字 + "-" + "entry-points"
	return pplName + "-entry-points"
}

// TODO: 将创建 Step 的逻辑迁移至此处，以合理的设置 step 的ctx，和 nodeType 等属性
func NewWorkflowRuntime(rc *runConfig) *WorkflowRuntime {
	entryCtx, entryCtxCancel := context.WithCancel(context.Background())
	postCtx, postCtxCancel := context.WithCancel(context.Background())

	EventChan := make(chan WorkflowEvent)

	entryPointName := generateEntrPointName(rc.WorkflowSource.Name)
	entryPoint := NewDagRuntime(entryPointName, &rc.WorkflowSource.EntryPoints, 0, entryCtx,
		EventChan, rc)

	wfr := &WorkflowRuntime{
		runConfig:            rc,
		entryPointsCtx:       entryCtx,
		entryPointsctxCancel: entryCtxCancel,
		postProcessPointsCtx: postCtx,
		postProcessctxCancel: postCtxCancel,
		EventChan:            EventChan,
		entryPoints:          entryPoint,
	}

	if len(rc.WorkflowSource.PostProcess) != 0 {
		for name, componet := range rc.WorkflowSource.PostProcess {
			postProcess := NewStepRuntime(name, componet, 0, postCtx, EventChan, rc)
			wfr.postProcess = postProcess
		}
	}
	return wfr
}

// 根据 runtimeview 来实例化 WorkflowRuntime, 主要用于 Restart 或者 Resume 的时候调用
// TODO:
func NewWorkflowRuntimeWithRuntimeView(view schema.RuntimeView) *WorkflowRuntime {
	return &WorkflowRuntime{}
}

// 运行
func (wfr *WorkflowRuntime) Start() error {
	wfr.status = common.StatusRunRunning

	wfr.entryPoints.Start()
	go wfr.Listen()
	return nil
}

// Restart 从 DB 中恢复重启
// TODO: 进一步思考重启逻辑
func (wfr *WorkflowRuntime) Restart() error {
	wfr.status = common.StatusRunRunning

	// 1. 如果 entryPoints 中的有节点尚未处于终态，则需要处理 entryPoints 中的节点，此时 PostProcess 中的节点会在 processEvent 中进行调度
	// 2. 如果 entryPoints 中所有节点都处于终态，且 PostProcess 中有节点未处于终态，此时直接 处理 PostProcess 中的 节点
	// 3. 如果 entryPoints 和 PostProcess 所有节点均处于终态，则直接更新 run 的状态即可, 并调用回调函数，传给 Server 入库
	// PS：第3种发生的概率很少，用于兜底
	if !wfr.entryPoints.isDone() {
		wfr.entryPoints.Resume()
		go wfr.Listen()
	} else if !wfr.postProcess.isDone() {
		wfr.postProcess.Resume()
		go wfr.Listen()
	} else {
		wfr.updateStatus()
		message := "run has been finished"
		wfEvent := NewWorkflowEvent(WfEventRunUpdate, message, nil)
		wfr.callback(*wfEvent)
	}
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

	wfr.entryPointsctxCancel()

	if force {
		wfr.postProcessctxCancel()
	}

	wfr.status = common.StatusRunTerminating

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

// processEvent 处理 job 推送到 run 的事件
// 对于异常处理的情况
// 1. 提交失败，job id\status 都为空，视为 job 失败，更新 run message 字段
// 2. watch 失败，状态不更新，更新 run message 字段；等 job 恢复服务之后，job watch 恢复，run 自动恢复调度
// 3. stop 失败，状态不更新，run message 字段；需要用户根据提示再次调用 stop
// 4. 如果有 job 的状态异常，将会走 FailureOptions 的处理逻辑
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
	if wfr.entryPoints.isDone() && wfr.postProcess != nil {
		if !wfr.postProcess.isStarted() {
			wfr.postProcess.Start()
		} else if wfr.postProcess.isDone() {
			wfr.updateStatus()
		}
	}

	wfr.callback(event)

	return nil
}

func (wfr *WorkflowRuntime) updateStatus() {
	// 只有所有step运行结束后会，才更新run为终止状态
	// 有failed step，run 状态为failed
	// 如果当前状态为 terminating，存在有 cancelled step 或者 terminated step，run 状态为terminated
	// 其余情况都为succeeded，因为：
	// - 有step为 cancelled 状态，要么是因为有节点失败了，要么是用户终止了 Run
	// - 另外skipped 状态的节点也视作运行成功（目前运行所有step都skip，此时run也是为succeeded）
	// - 如果有 Step 的状态为 terminated，但是 run 的状态不为 terminating, 则说明改step 是意外终止，此时 run 的状态应该Failed
	hasFailedComponent := wfr.entryPoints.isFailed() || wfr.postProcess.isFailed()
	hasTerminatedComponent := wfr.entryPoints.isTerminated() || wfr.postProcess.isTerminated()
	hasCancelledComponent := wfr.entryPoints.isCancelled() || wfr.postProcess.isCancelled()

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

func (wfr *WorkflowRuntime) callback(event WorkflowEvent) {
	extra := map[string]interface{}{
		common.WfEventKeyRunID:  wfr.runID,
		common.WfEventKeyStatus: wfr.status,
	}

	// TODO: 梳理这个 message 信息
	message := ""
	if event.isJobStopErr() && wfr.status == common.StatusRunTerminating {
		message = fmt.Sprintf("stop runfailed because of %s. please retry it.", event.Message)
	} else if event.isJobStopErr() {
		message = fmt.Sprintf("run has failed. but cannot stop related job because of %s.", event.Message)
	} else if event.isJobSubmitErr() {
		message = fmt.Sprintf("submit job in run error because of %s.", event.Message)
	} else if event.isJobWatchErr() {
		message = fmt.Sprintf("watch job in run error because of %s.", event.Message)
	} else {
		message = event.Message
	}

	wfEvent := NewWorkflowEvent(WfEventRunUpdate, message, extra)
	for i := 0; i < 3; i++ {
		wfr.logger.Infof("callback event [%+v]", wfEvent)
		if _, success := wfr.callbacks.UpdateRuntimeCb(wfr.runID, wfEvent); success {
			break
		}
	}
}
