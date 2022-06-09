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
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type DagRuntime struct {
	*baseComponentRuntime

	// 一个节点可以运行多次
	subComponentRumtimes map[string][]componentRuntime

	// 用于解析依赖参数模板
	*DependencySolver

	// 用于解析 reference 字段
	*referenceSolver

	startTime string

	endTime string
}

// TODO
func NewDagRuntime(name, fullName string, component schema.Component, seq int, ctx context.Context,
	eventChannel chan<- WorkflowEvent, config *runConfig) *DagRuntime {

	nrt := NewBaseComponentRuntime(name, fullName, component, seq, ctx, eventChannel, config)

	res := NewReferenceSolver(config.WorkflowSource)
	drt := &DagRuntime{
		baseComponentRuntime: nrt,
		referenceSolver:      res,
	}

	drt.updateStatus(StatusRuntimeInit)

	ds := NewDependencySolver(drt)
	drt.DependencySolver = ds

	return drt
}

func (drt *DagRuntime) generateSubComponentFullName(subComponentName string) string {
	return strings.Join([]string{drt.fullName, subComponentName}, string('.'))
}

// TODO: 需要注意上上游节点 skipped， failed， cancelled 时， 导致上游节点没有生成 runtime 的情况？
func (drt *DagRuntime) getReadyComponent() map[string]schema.Component {
	readyComponent := map[string]schema.Component{}
	for name, subComponent := range drt.component.(*schema.WorkflowSourceDag).EntryPoints {
		// 如果已经生成了对应的 Runtime，则说明对应该 Component 已经被调度了
		if _, ok := drt.subComponentRumtimes[name]; ok {
			continue
		}

		// 判读所有的上游节点是否处于终态，是则说明当前已经处于可调度的状态
		for _, depComponentName := range subComponent.GetDeps() {
			// 若节点没有对应的runtime，则说明该节点尚未运行
			if _, ok := drt.subComponentRumtimes[depComponentName]; !ok {
				continue
			}

			// 如果存在了 runtime， 需要满足以下条件：
			// runtime 的数目需要等于 loopArgument 的项数， 最低为1，且所有的runtime 均需处于 succeeded 或者 skipped 状态
			if subComponent.GetLoopArgument() != nil &&
				len(subComponent.GetLoopArgument().([]interface{})) != len(drt.subComponentRumtimes[depComponentName]) {
				continue
			}

			for _, depRuntime := range drt.subComponentRumtimes[depComponentName] {
				if !depRuntime.isSucceeded() && !depRuntime.isSkipped() {
					continue
				}
			}
		}

		readyComponent[name] = subComponent

	}
	return readyComponent
}

// resolveReference: 主要用于解析 reference 字段
func (drt *DagRuntime) resolveReference(subComponentName string, subComponent schema.Component) error {
	subFullName := drt.generateSubComponentFullName(subComponentName)

	newComponent, err := drt.referenceSolver.resolveComponentReference(subComponent)
	if err != nil {
		return err
	}

	drt.logger.Debugln("after resolve reference, component[%s] is:\n %v", subFullName, newComponent)
	drt.component.(*schema.WorkflowSourceDag).EntryPoints[subComponentName] = newComponent

	return nil
}

// createAndStartSubComponentRuntime: 创建并运行子节点 runtime
// 无需返回 error 原因是将通过 event 来进行同步
func (drt *DagRuntime) createAndStartSubComponentRuntime(subComponentName string, subComponent schema.Component) {
	subFullName := drt.generateSubComponentFullName(subComponentName)
	drt.logger.Debugln("begin to create runtime for component[%s]:\n%v", subFullName, subComponent)

	// 1. 获取 loop_arguemnt, 确定需要创建多少次runtime
	isv := NewInnerSolver(subComponent, subFullName, drt.runConfig)
	err := isv.resolveLoopArugment()
	if err != nil {
		err := fmt.Errorf("cannot get the value of loop_arugment for component[%s]", subFullName)
		drt.logger.Errorln(err.Error())
		drt.processSubRuntimeError(err, subComponent, subComponentName)
	}

	loop_argument := subComponent.GetLoopArgument().([]interface{})

	step, ok := subComponent.(*schema.WorkflowSourceStep)
	if ok {
		for index := range loop_argument {
			srt := NewStepRuntime(subComponentName, subFullName, step, index, drt.ctx,
				drt.receiveEventChildren, drt.runConfig)
			drt.subComponentRumtimes[subComponentName] = append(drt.subComponentRumtimes[subComponentName], srt)
			go srt.Start()
		}

	} else {
		dag, _ := subComponent.(*schema.WorkflowSourceDag)
		for index := range loop_argument {
			drt := NewDagRuntime(subComponentName, subFullName, dag, index, drt.ctx, drt.receiveEventChildren, drt.runConfig)
			drt.subComponentRumtimes[subComponentName] = append(drt.subComponentRumtimes[subComponentName], drt)
			go drt.Start()
		}
	}
}

// 开始执行 runtime
// 不返回error，直接通过 event 向上冒泡
// TODO: 处理在这期间任务被终止的情况
func (drt *DagRuntime) Start() {
	drt.started = true
	drt.updateStatus(StatusRuntimeRunning)
	drt.startTime = time.Now().Format("2006-01-02 15:04:05")

	// 1、替换 condition，loop_argument 中的模板，将其替换成具体真实值
	conditon, err := drt.CalculateCondition()
	if err != nil {
		errMsg := fmt.Sprintf("caculate the condition field for component[%s] faild:\n%s",
			drt.fullName, err.Error())
		drt.logger.Errorln(errMsg)
		drt.processStartAbnormalStatus(errMsg, StatusRuntimeSkipped)
	}

	if !conditon {
		skipMsg := fmt.Sprintf("the result of condition for Component [%s] is false, skip running", drt.fullName)
		drt.logger.Infoln(skipMsg)
		drt.processStartAbnormalStatus(skipMsg, StatusRuntimeSkipped)
		return
	}

	if drt.isDisabled() {
		skipMsg := fmt.Sprintf("Component [%s] is disabled, skip running", drt.fullName)
		drt.logger.Infoln(skipMsg)
		drt.processStartAbnormalStatus(skipMsg, StatusRuntimeSkipped)
	}

	// 监听子节点传递过来的事件
	go drt.Listen()

	// 开始调度子节点
	drt.scheduleSubComponent(true)
}

// scheduleSubComponent: 调度子节点运行
// 不返回error，直接通过 event 向上冒泡
func (drt *DagRuntime) scheduleSubComponent(mustSchedule bool) {
	// 1、获取可以进行调度的节点
	readyComponent := drt.getReadyComponent()

	// 如果 mustSchedule 为True, 说明此时必须要调度某些子节点运行，否则便是有bug
	if len(readyComponent) == 0 && mustSchedule {
		err := fmt.Errorf("cannot find any ready subComponent for Component[%s] while mustSchedule is True", drt.fullName)
		drt.logger.Errorln(err.Error())

		drt.updateStatus(StatusRuntimeFailed)
		dagView := newDagViewFromDagRuntime(drt, err.Error())
		drt.syncToApiServerAndParent(WfEventDagUpdate, dagView, err.Error())
		return
	}

	for subComponentName, subComponent := range readyComponent {
		// 2. Component 替换： 主要是处理 reference 字段
		err := drt.resolveReference(subComponentName, subComponent)
		if err != nil {
			drt.logger.Errorln(err.Error())
			drt.processSubRuntimeError(err, subComponent, subComponentName)
		}

		// 3. 替换子节点 parameter，artifact 字段中的模板
		err = drt.DependencySolver.ResolveBeforeRun(subComponentName)
		if err != nil {
			drt.logger.Errorln(err.Error())
			drt.processSubRuntimeError(err, subComponent, subComponentName)
		}

		// 4. 创建 runtime 并运行 runtime
		drt.createAndStartSubComponentRuntime(subComponentName, subComponent)
	}
}

// 监听由子节点发送过来的信号
func (drt *DagRuntime) Listen() {
	for {
		event := <-drt.receiveEventChildren
		if err := drt.processEvent(event); err != nil {
			// how to read event?
			drt.logger.Debugf("process event failed %s", err.Error())
		}
		if drt.isDone() {
			return
		}
	}
}

// 重新执行
// TODO
func (drt *DagRuntime) Resume() error {
	return nil
}

func (drt *DagRuntime) GetSubComponentParameterValue(componentName string, paramName string) (interface{}, error) {
	var value interface{}
	var err error
	subComponentsRuntime, ok := drt.subComponentRumtimes[componentName]
	if !ok {
		err := fmt.Errorf("cannot get the value of parameter[%s] from component[%s], because there is no component named [%s]",
			paramName, drt.fullName+"."+componentName, drt.fullName+"."+componentName)
		return nil, err
	} else {
		// 对于同一个节点的多次运行，其 parameter 的值都是一样的。
		value, err = subComponentsRuntime[0].getComponent().GetParameterValue(paramName)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}

func (drt *DagRuntime) GetSubComponentArtifactPaths(componentName string, artName string) (string, error) {
	var value string
	subComponents, ok := drt.subComponentRumtimes[componentName]
	if !ok {
		err := fmt.Errorf("cannot get the value of parameter[%s] from component[%s], because there is no component named [%s]",
			artName, drt.fullName+"."+componentName, drt.fullName+"."+componentName)
		return "", err
	} else {
		for index := range subComponents {
			p, err := subComponents[index].getComponent().GetArtifactPath(artName)
			if err != nil {
				return "", err
			}

			value = strings.Join([]string{value, p}, ",")
		}
	}
	return value, nil
}

// resume, 主要用于从宕机中恢复
// restartSteps
// 不能直接复用 triggerSteps 的原因是，重启时需要考虑step 已经 submitted，但是对应的job 还没有运行结束的情况，需要给这些步骤优先占到卡槽
func (wfr *WorkflowRuntime) reStart(steps map[string]*Step) error {
	for _, step := range steps {
		if step.done {
			continue
		} else {
			go step.Execute()
		}
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

// processEvent 处理 stepRuntime 推送过来的 run 的事件
// TODO: 进一步完善具体的逻辑
// 对于异常处理的情况
// 1. 提交失败，job id\status 都为空，视为 job 失败，更新 run message 字段
// 2. watch 失败，状态不更新，更新 run message 字段；等 job 恢复服务之后，job watch 恢复，run 自动恢复调度
// 3. stop 失败，状态不更新，run message 字段；需要用户根据提示再次调用 stop
// 4. 如果有 job 的状态异常，将会走 FailureOptions 的处理逻辑
func (drt *DagRuntime) processEvent(event WorkflowEvent) error {
	if drt.isDone() {
		drt.logger.Debugf("workflow has completed. skip event")
		return nil
	}
	drt.logger.Infof("process event: [%+v]", event)

	// 判断节点处于异常状态： Failed 和 Terminated（但是 dag 状态不是terminated 也不是terminating），是的话，则开始执行 FailureOptions 相关的逻辑

	status, ok := event.Extra["status"]
	if ok {
		subRuntimeStatus := status.(schema.JobStatus)
		isFailed := subRuntimeStatus == StatusRuntimeFailed
		isUnexpectedTerminated := subRuntimeStatus == StatusRuntimeTerminated && drt.status != StatusRuntimeTerminating

		if isFailed || isUnexpectedTerminated {
			drt.ProcessFailureOptions(event)
		}
	}

	drt.updateStatusAccordingSubComponentRuntimeStatus()

	drt.syncToApiServerAndParent(event)

	// 如果 dagRuntime 未处于终态，则需要判断是否有新的子节点可以运行

	return nil
}

func (drt *DagRuntime) isDepsReady(step *StepRuntime) bool {
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

func (wfr *WorkflowRuntime) getDirectDownstreamStep(upstreamStep *Step) (steps map[*Step]string) {
	// 因为golang 没有 set，所以使用 map 模拟一个 set，steps 的value 并没有实际意义
	steps = map[*Step]string{}
	for _, step := range wfr.entryPoints {
		deps := strings.Split(step.info.Deps, ",")
		for _, ds := range deps {
			ds = strings.Trim(ds, " ")
			if ds == upstreamStep.name {
				steps[step] = step.name
				wfr.wf.log().Infof("step[%s] is the downstream of step[%s] ", step.name, upstreamStep.name)
			}
		}
	}
	return steps
}

func (wfr *WorkflowRuntime) getAllDownstreamSteps(upstreamStep *Step) (steps map[*Step]string) {
	steps = map[*Step]string{}
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
			wfr.wf.log().Infof("step[%s] would be cancelled, because it upstream step[%s] failed", needCancelStep.name, step.name)
			needCancelStep.cancel <- true
		}
	}
}

func (wfr *WorkflowRuntime) ProcessFailureOptionsWithFailFast(step *Step) {
	// 1. 终止所有运行的 Job
	// 2. 将所有为调度的 Job 设置为 cancelled 状态
	wfr.entryPointsctxCancel()
}

func (wfr *WorkflowRuntime) ProcessFailureOptions(event WorkflowEvent) {
	wfr.wf.log().Infof("begin to process failure options. trigger event is: %v", event)
	st, ok := event.Extra["step"]

	if !ok {
		wfr.wf.log().Errorf("cannot get the step info of event for run[%s], begin to stop run: %v", wfr.wf.RunID, event)

		// 防止下游节点无法调度，导致 run 被 hang 住，终止所有任务
		wfr.entryPointsctxCancel()
	}

	step, ok := st.(*Step)
	if !ok {
		wfr.wf.log().Errorf("cannot get the step info of envent for run[%s], begin to stop run: %v", wfr.wf.RunID, event)

		// 防止下游节点无法调度，导致 run 被 hang 住，终止所有任务
		wfr.entryPointsctxCancel()
	}

	// FailureOptions 不处理 PostProcess 中的节点
	if step.nodeType == NodeTypePostProcess {
		return
	}

	// 策略的合法性由 workflow 保证
	if wfr.wf.Source.FailureOptions.Strategy == schema.FailureStrategyContinue {
		wfr.ProcessFailureOptionsWithContinue(step)
	} else {
		wfr.ProcessFailureOptionsWithFailFast(step)
	}
}

// processSkipped: 处理节点 skiped 的情况
func (drt *DagRuntime) processStartAbnormalStatus(msg string, status RuntimeStatus) {
	drt.updateStatus(status)
	dagView := newDagViewFromDagRuntime(drt, msg)
	drt.syncToApiServerAndParent(WfEventDagUpdate, dagView, msg)
}

func (drt *DagRuntime) processSubStepRuntimeError(err error, step *schema.WorkflowSourceStep, StepName string) {
	stepView := newStepViewFromWorkFlowSourceStep(step, StepName, err.Error(), StatusRuntimeFailed, drt.runConfig)

	extra := map[string]interface{}{
		common.WfEventKeyRunID:  drt.runID,
		common.WfEventKeyPK:     drt.pk,
		common.WfEventKeyStatus: StatusRuntimeFailed,
		common.WfEventKeyView:   stepView,
	}

	event := NewWorkflowEvent(WfEventJobSubmitErr, err.Error(), extra)
	drt.processEvent(*event)
}

func (drt *DagRuntime) processSubDagRuntimeError(err error, dag *schema.WorkflowSourceDag, dagName string) {
	dagView := newDagViewFromWorkFlowSourceDag(dag, dagName, err.Error(), StatusRuntimeFailed)
	extra := map[string]interface{}{
		common.WfEventKeyRunID:  drt.runID,
		common.WfEventKeyPK:     drt.pk,
		common.WfEventKeyStatus: StatusRuntimeFailed,
		common.WfEventKeyView:   dagView,
	}

	event := NewWorkflowEvent(WfEventDagUpdate, err.Error(), extra)
	drt.processEvent(*event)
}

// processSubRuntimeError： 处理调度子节点失败的情况，通过调用processEvent()函数来进行同步
func (drt *DagRuntime) processSubRuntimeError(err error, cp schema.Component, componentName string) {
	step, ok := cp.(*schema.WorkflowSourceStep)
	if ok {
		drt.processSubStepRuntimeError(err, step, componentName)
	} else {
		drt.processSubDagRuntimeError(err, cp.(*schema.WorkflowSourceDag), componentName)
	}
}
