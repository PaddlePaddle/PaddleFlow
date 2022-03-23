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
	"strings"
	"sync"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/common/schema"
)

// 工作流运行时
type WorkflowRuntime struct {
	wf               *Workflow
	ctx              context.Context
	ctxCancel        context.CancelFunc
	steps            map[string]*Step
	event            chan WorkflowEvent // 用来从 job 传递事件
	concurrentJobs   chan struct{}
	concurrentJobsMx sync.Mutex
	status           string
}

func NewWorkflowRuntime(wf *Workflow, parallelism int) *WorkflowRuntime {
	ctx, ctxCancel := context.WithCancel(context.Background())
	wfr := &WorkflowRuntime{
		wf:             wf,
		ctx:            ctx,
		ctxCancel:      ctxCancel,
		steps:          map[string]*Step{},
		event:          make(chan WorkflowEvent, parallelism),
		concurrentJobs: make(chan struct{}, parallelism),
	}
	return wfr
}

// 运行
func (wfr *WorkflowRuntime) Start() error {
	wfr.status = common.StatusRunRunning

	for st_name, st := range wfr.steps {
		if st.done {
			wfr.wf.log().Debugf("Skip step: %s", st_name)
			continue
		}

		wfr.wf.log().Debugf("Start Execute step: %s", st_name)
		go st.Execute()

		if wfr.isDepsReady(st) && !st.submitted {
			wfr.wf.log().Debugf("Step %s has ready to start job", st_name)
			st.update(st.done, true, st.job)
			st.ready <- true
		}
	}

	go wfr.Listen()
	return nil
}

// Restart 从 DB 中恢复重启
func (wfr *WorkflowRuntime) Restart() error {
	wfr.status = common.StatusRunRunning
	for _, step := range wfr.steps {
		if step.done {
			continue
		}
		go step.Execute()
	}

	// 如果在服务异常过程中，刚好 step 中的任务已经完成，而新的任务还没有开始，此时会导致调度逻辑永远不会 watch 到新的 event
	// 不在上面直接判断 step.depsReady 的原因：wfr.steps 是 map，遍历顺序无法确定，必须保证已提交的任务先占到槽位，才能保证并发数控制正确
	for stepName, step := range wfr.steps {
		if step.done || step.submitted {
			continue
		}
		if wfr.isDepsReady(step) {
			wfr.wf.log().Debugf("Step %s has ready to start run", stepName)
			step.update(step.done, true, step.job)
			step.ready <- true
		}
	}

	go wfr.Listen()

	return nil
}

// Stop 停止 Workflow
// do not call ctx_cancel(), which will be called when all steps has terminated eventually.
func (wfr *WorkflowRuntime) Stop() error {
	if wfr.IsCompleted() {
		wfr.wf.log().Debugf("workflow has finished.")
		return nil
	}

	wfr.ctxCancel()

	wfr.status = common.StatusRunTerminating

	return nil
}

func (wfr *WorkflowRuntime) Status() string {
	return wfr.status
}

func (wfr *WorkflowRuntime) Listen() {
	for {
		select {
		case event := <-wfr.event:
			if err := wfr.processEvent(event); err != nil {
				// how to read event?
				wfr.wf.log().Debugf("process event failed %s", err.Error())
			}
			if wfr.IsCompleted() {
				return
			}
		}
	}
}

// 增加多个并行Job
func (wfr *WorkflowRuntime) IncConcurrentJobs(slots int) {
	wfr.concurrentJobsMx.Lock()
	for i := 0; i < slots; i++ {
		wfr.concurrentJobs <- struct{}{}
		wfr.wf.log().Debugf("Increased concurrent jobs")
	}
	wfr.concurrentJobsMx.Unlock()
}

// 减少多个并行Job
func (wfr *WorkflowRuntime) DecConcurrentJobs(slots int) {
	for i := 0; i < slots; i++ {
		<-wfr.concurrentJobs
		wfr.wf.log().Debugf("Decreased concurrent jobs")
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
func (wfr *WorkflowRuntime) processEvent(event WorkflowEvent) error {
	if wfr.IsCompleted() {
		wfr.wf.log().Debugf("workflow has completed. skip event")
		return nil
	}
	wfr.wf.log().Infof("process event: [%+v]", event)

	wfr.updateStatus()

	wfr.callback(event)

	return nil
}

func (wfr *WorkflowRuntime) updateStatus() {
	stepDone := 0
	hasFailedStep := false
	hasTerminatedStep := false
	for st_name, st := range wfr.steps {
		if st.job.Succeeded() {
			stepDone++
			wfr.wf.log().Infof("has succeeded step: %s", st_name)
			continue
		} else if st.job.Failed() {
			stepDone++
			hasFailedStep = true
			wfr.wf.log().Infof("has failed step: %s", st_name)
			continue
		} else if st.job.Terminated() {
			stepDone++
			hasTerminatedStep = true
			wfr.wf.log().Infof("has terminated step: %s", st_name)
			continue
		} else if st.done {
			// 两种情况：cancellled，skipped
			// - cancellled：job has not submitted, but has stopped by ctxCancel
			// - skipped：job disabled
			stepDone++
			wfr.wf.log().Infof("has done step: %s", st_name)
			continue
		}

		if wfr.isDepsReady(st) && !st.submitted {
			wfr.wf.log().Infof("Step %s has ready to start job", st_name)
			st.update(st.done, true, st.job)
			st.ready <- true
		}
	}

	// 只有所有step运行结束后会，才更新run为终止状态
	// 有failed step，run 状态为failed
	// terminated step，run 状态为terminated
	// 其余情况都为succeeded，因为：
	// - 有step为 cancelled 状态，那就肯定有有step为terminated
	// - 另外skipped 状态的节点也视作运行成功（目前运行所有step都skip，此时run也是为succeeded）
	if stepDone == len(wfr.steps) {
		if hasFailedStep {
			wfr.status = common.StatusRunFailed
		} else if hasTerminatedStep {
			if wfr.status == common.StatusRunTerminating {
				wfr.status = common.StatusRunTerminated
			} else {
				wfr.status = common.StatusRunFailed
			}
		} else {
			wfr.status = common.StatusRunSucceeded
		}

		wfr.wf.log().Debugf("workflow %s finished", wfr.wf.Name)
		return
	}

	// 如果有step运行未结束 + 有失败的 step + run未发起停止操作，就得发起停止run操作
	if (hasFailedStep || hasTerminatedStep) && wfr.ctx.Err() == nil {
		wfr.wf.log().Infof("workflow %s has failed or terminated step, begin to cancel it ", wfr.wf.Name)
		wfr.ctxCancel()
	}
}

func (wfr *WorkflowRuntime) isDepsReady(step *Step) bool {
	depsReady := true
	deps := strings.Split(step.info.Deps, ",")
	for _, ds := range deps {
		ds = strings.Trim(ds, " ")
		if len(ds) <= 0 {
			continue
		}
		if !wfr.steps[ds].job.Succeeded() && !wfr.steps[ds].job.Skipped() {
			depsReady = false
		}
	}
	return depsReady
}

func (wfr *WorkflowRuntime) callback(event WorkflowEvent) {
	runtimeView := make(schema.RuntimeView, 0)
	for name, st := range wfr.steps {
		job := st.job.Job()
		jobView := schema.JobView{
			JobID:      job.Id,
			JobName:    job.Name,
			Command:    job.Command,
			Parameters: job.Parameters,
			Env:        job.Env,
			StartTime:  job.StartTime,
			EndTime:    job.EndTime,
			Status:     job.Status,
			Deps:       job.Deps,
			DockerEnv:  st.info.DockerEnv,
			Artifacts:  job.Artifacts,
			Cache:		st.info.Cache,
			JobMessage: job.Message,
			CacheRunID: st.CacheRunID,
		}
		runtimeView[name] = jobView
	}
	extra := map[string]interface{}{
		common.WfEventKeyRunID:   wfr.wf.RunID,
		common.WfEventKeyStatus:  wfr.status,
		common.WfEventKeyRuntime: runtimeView,
	}

	message := ""
	if event.isJobStopErr() && wfr.status == common.StatusRunTerminating {
		message = fmt.Sprintf("stop runfailed because of %s. please retry it.", event.Message)
	} else if event.isJobStopErr() {
		message = fmt.Sprintf("run has failed. but cannot stop related job because of %s.", event.Message)
	} else if event.isJobSubmitErr() {
		message = fmt.Sprintf("submit job in run error because of %s.", event.Message)
	} else if event.isJobWatchErr() {
		message = fmt.Sprintf("watch job in run error because of %s.", event.Message)
	}

	wfEvent := NewWorkflowEvent(WfEventRunUpdate, message, extra)
	for i := 0; i < 3; i++ {
		wfr.wf.log().Infof("callback event [%+v]", wfEvent)
		if success := wfr.wf.callbacks.UpdateRunCb(wfr.wf.RunID, wfEvent); success {
			break
		}
	}
	// todo: how to handle retry failed
}
