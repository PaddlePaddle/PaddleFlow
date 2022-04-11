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
	. "paddleflow/pkg/pipeline/common"
)

type StatusToSteps struct {
	SucceededSteps  map[string]*Step
	FailedSteps     map[string]*Step
	TerminatedSteps map[string]*Step
	CanelledSteps   map[string]*Step
	SkippedSteps    map[string]*Step

	// runtime 已经向 step的 ready channel 发送了数据, 同时 Step 尚未处于终态
	SubmittedSteps map[string]*Step

	// runtime 还未向 step 的 ready channel 发送数据， 同时 Step 也未处于终态
	PengdingSteps map[string]*Step
}

func NewStatusToSteps() StatusToSteps {
	return StatusToSteps{
		SucceededSteps:  map[string]*Step{},
		FailedSteps:     map[string]*Step{},
		TerminatedSteps: map[string]*Step{},
		CanelledSteps:   map[string]*Step{},
		SkippedSteps:    map[string]*Step{},
		SubmittedSteps:  map[string]*Step{},
		PengdingSteps:   map[string]*Step{},
	}
}

// 统计已经处于终态的 Step
func (sts *StatusToSteps) statFinshedSteps() int {
	return len(sts.SucceededSteps) + len(sts.FailedSteps) + len(sts.TerminatedSteps) + len(sts.CanelledSteps) + len(sts.SkippedSteps)
}

// 工作流运行时
type WorkflowRuntime struct {
	wf               *Workflow
	ctx              context.Context
	ctxCancel        context.CancelFunc
	entryPoints      map[string]*Step
	postProcess      map[string]*Step
	event            chan WorkflowEvent // 用来从 job 传递事件
	concurrentJobs   chan struct{}
	concurrentJobsMx sync.Mutex
	status           string
	runtimeView      schema.RuntimeView
	postProcessView  schema.PostProcessView
}

func NewWorkflowRuntime(wf *Workflow, parallelism int) *WorkflowRuntime {
	ctx, ctxCancel := context.WithCancel(context.Background())
	wfr := &WorkflowRuntime{
		wf:              wf,
		ctx:             ctx,
		ctxCancel:       ctxCancel,
		entryPoints:     map[string]*Step{},
		postProcess:     map[string]*Step{},
		event:           make(chan WorkflowEvent, parallelism),
		concurrentJobs:  make(chan struct{}, parallelism),
		runtimeView:     schema.RuntimeView{},
		postProcessView: schema.PostProcessView{},
	}
	return wfr
}

// 运行
func (wfr *WorkflowRuntime) Start() error {
	wfr.status = common.StatusRunRunning

	wfr.triggerSteps(wfr.entryPoints, NodeTypeEntrypoint)

	go wfr.Listen()
	return nil
}

// 触发step运行
func (wfr *WorkflowRuntime) triggerSteps(steps map[string]*Step, nodeType NodeType) error {
	for st_name, st := range steps {
		st.NodeType = nodeType
		if st.done {
			wfr.wf.log().Debugf("Skip step: %s", st_name)
			continue
		}

		wfr.wf.log().Debugf("Start Execute step: %s", st_name)
		go st.Execute()

		if wfr.isDepsReady(st, steps) && !st.submitted {
			wfr.wf.log().Debugf("Step %s has ready to start job", st_name)
			st.update(st.done, true, st.job)
			st.ready <- true
		}
	}
	return nil
}

// restartSteps
// 不能直接复用 triggerSteps 的原因是，重启时需要考虑step 已经 submitted，但是对应的job 还没有运行结束的情况，需要给这些步骤优先占到卡槽
func (wfr *WorkflowRuntime) restartSteps(steps map[string]*Step) error {
	for _, step := range steps {
		if step.done {
			continue
		}
		go step.Execute()
	}

	// 如果在服务异常过程中，刚好 step 中的任务已经完成，而新的任务还没有开始，此时会导致调度逻辑永远不会 watch 到新的 event
	// 不在上面直接判断 step.depsReady 的原因：wfr.steps 是 map，遍历顺序无法确定，必须保证已提交的任务先占到槽位，才能保证并发数控制正确
	for stepName, step := range steps {
		if step.done || step.submitted {
			continue
		}
		if wfr.isDepsReady(step, steps) {
			wfr.wf.log().Debugf("Step %s has ready to start run", stepName)
			step.update(step.done, true, step.job)
			step.ready <- true
		}
	}

	return nil
}

// Restart 从 DB 中恢复重启
func (wfr *WorkflowRuntime) Restart() error {
	wfr.status = common.StatusRunRunning

	statusToEntrySteps := wfr.statStepStatus(wfr.entryPoints)
	statusToPostSteps := wfr.statStepStatus(wfr.postProcess)

	// 1. 如果 entryPoints 中的有节点尚未处于终态，则需要处理 entryPoints 中的节点，此时 PostProcess 中的节点会在 processEvent 中进行调度
	// 2. 如果 entryPoints 中所有节点都处于终态，且 PostProcess 中有节点未处于终态，此时直接 处理 PostProcess 中的 节点
	// 3. 如果 entryPoints 和 PostProcess 所有节点均处于终态，则直接更新 run 的状态即可, 并调用回调函数，传给 Server 入库
	// PS：第3种发生的概率很少，用于兜底
	if statusToEntrySteps.statFinshedSteps() != len(wfr.entryPoints) {
		wfr.restartSteps(wfr.entryPoints)
	} else if statusToPostSteps.statFinshedSteps() != len(wfr.postProcess) {
		wfr.restartSteps(wfr.postProcess)
	} else {
		wfr.updateStatus(statusToEntrySteps, statusToPostSteps)

		message := "run has been finished"
		wfEvent := NewWorkflowEvent(WfEventRunUpdate, message, nil)
		wfr.callback(*wfEvent)
	}

	go wfr.Listen()

	return nil
}

// Stop 停止 Workflow
// do not call ctx_cancel(), which will be called when all steps has terminated eventually.
// 注意: stop 时暂时不会stop PostProcess 中的节点
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

// 触发 post_process 中的步骤
func (wfr *WorkflowRuntime) triggerPostPorcess() error {
	for _, step := range wfr.postProcess {
		// postProcess 中的节点不支持 cache 操作
		step.info.Cache.Enable = false
	}
	return wfr.triggerSteps(wfr.postProcess, NodeTypePostProcess)
}

// processEvent 处理 job 推送到 run 的事件
// 对于异常处理的情况
// 1. 提交失败，job id\status 都为空，视为 job 失败，更新 run message 字段
// 2. watch 失败，状态不更新，更新 run message 字段；等 job 恢复服务之后，job watch 恢复，run 自动恢复调度
// 3. stop 失败，状态不更新，run message 字段；需要用户根据提示再次调用 stop
// 4. 如果有 job 的状态异常，将会走 FailureOptions 的处理逻辑
func (wfr *WorkflowRuntime) processEvent(event WorkflowEvent) error {
	if wfr.IsCompleted() {
		wfr.wf.log().Debugf("workflow has completed. skip event")
		return nil
	}
	wfr.wf.log().Infof("process event: [%+v]", event)

	// 判断是节点处于异常状态： Failed 和 Terminated，是的话， 则开始执行 FailureOptions 相关的逻辑
	// 当前有 WfEventJobSubmitErr 和 WfEventJobUpdate 会两种类型的 event 中可能会包含的 Failed 或者 Terminated 状态
	// TODO: 考虑处理 PostProcess 中节点的情况, 还有需要考虑并发性
	if event.isJobSubmitErr() || event.isJobUpdate() {
		status, ok := event.Extra["status"]
		if ok {
			jobStatus := status.(schema.JobStatus)
			jobFaield := jobStatus == schema.StatusJobFailed
			jobUnexpectedTerinated := jobStatus == schema.StatusJobTerminated && wfr.status != common.StatusRunTerminating

			if jobFaield || jobUnexpectedTerinated {
				wfr.ProcessFailureOptions(event)
			}
		}
	}

	// 1. 如果有entryPoints 中的 step 处于 pending 状态，则尝试触发 step
	// 2. 如果 entryPoints 中所有 steps 都处于 终态，但是有 postProcess 处于 Pending 状态，则触发 PostProcesss 中的 steps
	// 3. 如果 entryPoints 和 postProcess 中所有的 steps 均处于终态，则会更新 Run 的状态
	statusToEntrySteps := wfr.statStepStatus(wfr.entryPoints)
	statusToPostSteps := wfr.statStepStatus(wfr.postProcess)

	if len(statusToEntrySteps.PengdingSteps) != 0 {
		wfr.triggerSteps(wfr.entryPoints, NodeTypeEntrypoint)
	} else if statusToEntrySteps.statFinshedSteps() == len(wfr.entryPoints) && len(statusToPostSteps.PengdingSteps) != 0 {
		wfr.triggerPostPorcess()
	} else if statusToEntrySteps.statFinshedSteps() == len(wfr.entryPoints) && statusToPostSteps.statFinshedSteps() == len(wfr.postProcess) {
		wfr.updateStatus(statusToEntrySteps, statusToPostSteps)
	}

	wfr.callback(event)

	return nil
}

func (wfr *WorkflowRuntime) statStepStatus(steps map[string]*Step) StatusToSteps {
	status := NewStatusToSteps()
	for st_name, st := range steps {
		switch st.job.(*PaddleFlowJob).Status {
		case schema.StatusJobSucceeded:
			status.SucceededSteps[st_name] = st
		case schema.StatusJobFailed:
			status.FailedSteps[st_name] = st
		case schema.StatusJobTerminated:
			status.TerminatedSteps[st_name] = st
		case schema.StatusJobCancelled:
			status.CanelledSteps[st_name] = st
		case schema.StatusJobSkipped:
			status.SkippedSteps[st_name] = st
		default:
			if st.submitted {
				status.SubmittedSteps[st_name] = st
			} else {
				status.PengdingSteps[st_name] = st
			}
		}
	}
	return status
}

func (wfr *WorkflowRuntime) updateStatus(entryPointsStatus, postProcessStatus StatusToSteps) {
	// 只有所有step运行结束后会，才更新run为终止状态
	// 有failed step，run 状态为failed
	// terminated step，run 状态为terminated
	// 其余情况都为succeeded，因为：
	// - 有step为 cancelled 状态，那就肯定有有step为terminated
	// - 另外skipped 状态的节点也视作运行成功（目前运行所有step都skip，此时run也是为succeeded）
	if len(entryPointsStatus.FailedSteps)+len(postProcessStatus.FailedSteps) != 0 {
		wfr.status = common.StatusRunFailed
	} else if len(entryPointsStatus.TerminatedSteps)+len(postProcessStatus.TerminatedSteps) != 0 {
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

func (wfr *WorkflowRuntime) getDirectDownstreamStep(upstreamStep *Step) (steps map[*Step]string) {
	// 因为golang 没有 set，所以使用 map 模拟一个 set，steps 的value 并没有实际意义
	for _, step := range wfr.entryPoints {
		deps := strings.Split(step.info.Deps, ",")
		for _, ds := range deps {
			ds = strings.Trim(ds, " ")
			if ds == upstreamStep.name {
				steps[step] = step.name
			}
		}
	}
	return steps
}

func (wfr *WorkflowRuntime) getAllDownstreamSteps(upstreamStep *Step) (steps map[*Step]string) {
	// 深度优先遍历或者广度优先遍历？
	toVisiteStep := wfr.getDirectDownstreamStep(upstreamStep)

	// 循环获取下游节点的下游下游节点，直至叶子节点
	for {
		downstreamSteps := map[*Step]string{}
		for step, _ := range toVisiteStep {
			downstreamStep := wfr.getDirectDownstreamStep(step)
			steps[step] = step.name

			for downStep, _ := range downstreamStep {
				// 判断 downStep 是否已经解析过其下游节点
				_, ok := steps[downStep]
				if !ok {
					downstreamSteps[downStep] = downStep.name
				}
			}
		}

		if len(downstreamSteps) == 0 {
			break
		} else {
			toVisiteStep = downstreamSteps
		}
	}
	return steps
}

func (wfr *WorkflowRuntime) ProcessFailureOptionsWithContinue(step *Step) {
	// 失败节点的所有下游节点都将会置为failed

	needCancelSteps := wfr.getAllDownstreamSteps(step)

	for needCancelStep, _ := range needCancelSteps {
		if !needCancelStep.done {
			needCancelStep.cancel <- true
		}
	}
}

func (wfr *WorkflowRuntime) ProcessFailureOptionsWithFailFast(step *Step) {
	// 1. 终止所有运行的 Job
	// 2. 将所有为调度的 Job 设置为 cancelled 状态
	wfr.ctxCancel()
}

func (wfr *WorkflowRuntime) ProcessFailureOptions(event WorkflowEvent) {
	st, ok := event.Extra["step"]
	if !ok {
		wfr.wf.log().Errorf("cannot get the step info of envent for run[%s], begin to stop run: %v", wfr.wf.RunID, event)

		// 防止下游节点无法调度，导致 run 被 hang 住，终止所有任务
		wfr.ctxCancel()
	}

	step, ok := st.(*Step)
	if !ok {
		wfr.wf.log().Errorf("cannot get the step info of envent for run[%s], begin to stop run: %v", wfr.wf.RunID, event)

		// 防止下游节点无法调度，导致 run 被 hang 住，终止所有任务
		wfr.ctxCancel()
	}

	// FailureOptions 不处理 PostProcess 中的节点
	if step.NodeType == NodeTypePostProcess {
		return
	}

	// 策略的合法性由 workflow 保证
	if wfr.wf.Source.FailureOptions.Strategy == schema.FailureStrategyContinue {
		wfr.ProcessFailureOptionsWithContinue(step)
	} else {
		wfr.ProcessFailureOptionsWithFailFast(step)
	}
}

func (wfr *WorkflowRuntime) isDepsReady(step *Step, steps map[string]*Step) bool {
	depsReady := true
	deps := strings.Split(step.info.Deps, ",")
	for _, ds := range deps {
		ds = strings.Trim(ds, " ")
		if len(ds) <= 0 {
			continue
		}

		if !steps[ds].job.Succeeded() && !steps[ds].job.Skipped() {
			depsReady = false
		}
	}
	return depsReady
}

// update RuntimeView or PostProcessView
func (wfr *WorkflowRuntime) updateView(viewType ViewType) {
	var steps map[string]*Step
	if viewType == ViewTypeEntrypoint {
		steps = wfr.entryPoints
	} else if viewType == ViewTypePostProcess {
		steps = wfr.postProcess
	}

	for name, st := range steps {
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
			Cache:      st.info.Cache,
			JobMessage: job.Message,
			CacheRunID: st.CacheRunID,
		}
		if viewType == ViewTypeEntrypoint {
			wfr.runtimeView[name] = jobView
		} else if viewType == ViewTypePostProcess {
			wfr.postProcessView[name] = jobView
		}
	}
}

func (wfr *WorkflowRuntime) callback(event WorkflowEvent) {
	// 1. 更新 runtimeview
	wfr.updateView(ViewTypeEntrypoint)

	// 2. 更新 postProcessView
	wfr.updateView(ViewTypePostProcess)

	extra := map[string]interface{}{
		common.WfEventKeyRunID:       wfr.wf.RunID,
		common.WfEventKeyStatus:      wfr.status,
		common.WfEventKeyRuntime:     wfr.runtimeView,
		common.WfEventKeyPostProcess: wfr.postProcessView,
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
