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
	. "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

// TODO: 思考并发是否有影响

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

	// dagruntime 的全局唯一标识符
	ID string
}

func generateDagID(runID string) string {
	return "dag-" + runID + "-" + GetRandID(DagIDRandCodeNum)
}

// TODO
func NewDagRuntime(name, fullName string, component schema.Component, seq int, ctx context.Context,
	eventChannel chan<- WorkflowEvent, config *runConfig, parentDagID string) *DagRuntime {

	nrt := NewBaseComponentRuntime(name, fullName, component, seq, ctx, eventChannel, config, parentDagID)
	res := NewReferenceSolver(config.WorkflowSource)

	ID := generateDagID(config.runID)

	drt := &DagRuntime{
		baseComponentRuntime: nrt,
		referenceSolver:      res,
		ID:                   ID,
	}

	drt.updateStatus(StatusRuntimeInit)

	ds := NewDependencySolver(drt)
	drt.DependencySolver = ds

	return drt
}

func (drt *DagRuntime) generateSubComponentFullName(subComponentName string, seq int) string {
	if seq == 0 {
		return strings.Join([]string{drt.fullName, subComponentName}, string('.'))
	} else {
		return strings.Join([]string{drt.fullName, subComponentName, string(seq)}, string('.'))
	}
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
	subFullName := drt.generateSubComponentFullName(subComponentName, 0)

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
	subFullName := drt.generateSubComponentFullName(subComponentName, 0)
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
			subFullName := drt.generateSubComponentFullName(subComponentName, index)
			srt := NewStepRuntime(subComponentName, subFullName, step, index, drt.ctx,
				drt.receiveEventChildren, drt.runConfig, drt.ID)
			drt.subComponentRumtimes[subComponentName] = append(drt.subComponentRumtimes[subComponentName], srt)
			go srt.Start()
		}

	} else {
		dag, _ := subComponent.(*schema.WorkflowSourceDag)
		for index := range loop_argument {
			subFullName := drt.generateSubComponentFullName(subComponentName, index)
			drt := NewDagRuntime(subComponentName, subFullName, dag, index, drt.ctx, drt.receiveEventChildren, drt.runConfig, drt.ID)
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

	// 处理任务被终止的情况
	if drt.ctx.Err() != nil {
		drt.prcessCancelledSignall()
	}

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
		// 处理任务被终止的情况
		if drt.ctx.Err() != nil {
			drt.prcessCancelledSignall()
		}

		// 2. Component 替换： 主要是处理 reference 字段
		err := drt.resolveReference(subComponentName, subComponent)
		if err != nil {
			drt.logger.Errorln(err.Error())

			// 创建占位用 runtime, 并将相关信息
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

	status, ok := event.Extra[common.WfEventKeyStatus]
	if ok {
		subRuntimeStatus := status.(schema.JobStatus)
		isFailed := subRuntimeStatus == StatusRuntimeFailed
		isUnexpectedTerminated := subRuntimeStatus == StatusRuntimeTerminated && drt.status != StatusRuntimeTerminating

		if isFailed || isUnexpectedTerminated {
			drt.ProcessFailureOptions(event)
		}
	}

	StatusMsg := drt.updateStatusAccordingSubComponentRuntimeStatus()

	drt.syncToApiServerAndParent(event)

	// 如果 dagRuntime 未处于终态，则需要判断是否有新的子节点可以运行

	return nil
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

func (wfr *WorkflowRuntime) getAllDownstreamSteps(component schema.Component) (steps map[string]string) {
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

func (drt *DagRuntime) ProcessFailureOptionsWithContinue(component schema.Component) {
	// 失败节点的所有下游节点都将会置为failed
	needCancelComponent := drt.getAllDownstreamSteps(component)
	for needCancelStep, _ := range needCancelComponent {
		if !needCancelStep.done {
			drt.logger.Infof("step[%s] would be cancelled, because it upstream step[%s] failed", needCancelStep.name, step.name)
			needCancelStep.cancel <- true
		}
	}
}

func (drt *DagRuntime) ProcessFailureOptionsWithFailFast() {
	// 1. 终止所有运行的 Job
	// 2. 将所有为调度的 Job 设置为 cancelled 状态
	// drt.ctx.Done()
	// TODO
}

func (drt *DagRuntime) ProcessFailureOptions(event WorkflowEvent) {
	drt.logger.Infof("begin to process failure options. trigger event is: %v", event)
	name, ok := event.Extra[common.WfEventKeyComponentName]
	componentName := name.(string)

	if !ok {
		drt.logger.Errorf("cannot get the step info of event for run[%s], begin to stop run: %v", wfr.wf.RunID, event)

		// 防止下游节点无法调度，导致 run 被 hang 住，终止所有任务
		drt.ctx.Done()
	}

	component := drt.subComponentRumtimes[componentName][0].getComponent()

	// 策略的合法性由 workflow 保证
	if drt.FailureOptions.Strategy == schema.FailureStrategyContinue {
		drt.ProcessFailureOptionsWithContinue(component)
	} else {
		drt.ProcessFailureOptionsWithFailFast()
	}
}

// processSkipped: 处理节点 skiped 的情况
func (drt *DagRuntime) processStartAbnormalStatus(msg string, status RuntimeStatus) {
	drt.updateStatus(status)
	dagView := newDagViewFromDagRuntime(drt, msg)
	drt.syncToApiServerAndParent(WfEventDagUpdate, dagView, msg)
}

func (drt *DagRuntime) processSubStepRuntimeError(err error, step *schema.WorkflowSourceStep, StepName string) {
	stepView := newStepViewFromWorkFlowSourceStep(step, StepName, err.Error(), StatusRuntimeFailed, drt.runConfig, drt.ID)

	extra := map[string]interface{}{
		common.WfEventKeyRunID:         drt.runID,
		common.WfEventKeyPK:            drt.pk,
		common.WfEventKeyStatus:        StatusRuntimeFailed,
		common.WfEventKeyView:          stepView,
		common.WfEventKeyComponentName: drt.componentName,
	}

	event := NewWorkflowEvent(WfEventJobSubmitErr, err.Error(), extra)
	drt.processEvent(*event)
}

func (drt *DagRuntime) processSubDagRuntimeError(err error, dag *schema.WorkflowSourceDag, dagName string) {
	dagView := newDagViewFromWorkFlowSourceDag(dag, dagName, err.Error(), StatusRuntimeFailed, drt.ID)
	extra := map[string]interface{}{
		common.WfEventKeyRunID:         drt.runID,
		common.WfEventKeyPK:            drt.pk,
		common.WfEventKeyStatus:        StatusRuntimeFailed,
		common.WfEventKeyView:          dagView,
		common.WfEventKeyComponentName: dagName,
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

func (drt *DagRuntime) prcessCancelledSignall() {
	return
}

// updateStatusAccordingSubComponentRuntimeStatus: 根据子节点的状态来更新
func (drt *DagRuntime) updateStatusAccordingSubComponentRuntimeStatus() string {
	// 1. 如果有子节点还没有调度，且节点本身的状态不为 Terminating， 则状态必定为running
	if len(drt.subComponentRumtimes) != len(drt.getComponent().(*schema.WorkflowSourceDag).EntryPoints) &&
		drt.isTerminating() {
		drt.updateStatus(StatusRuntimeRunning)
		return ""
	}

	terminatedComponentNames := []string{}
	faieldComponentNames := []string{}
	succeededComponentNames := []string{}
	cancelledComponentNames := []string{}
	skippedComponentNames := []string{}

	for _, cps := range drt.subComponentRumtimes {
		// 2. 判断 cps 的数目是否和 loop_argument 相同，如果不相同，则说明有 runtime 还没有被创建
		// 2.1、如果 loop_argument 字段为 nil，则表明其不是 loop_arugment, 无需考虑上面所说情况
		loop_argument := drt.getComponent().GetLoopArgument()
		if loop_argument != nil {
			loop_args, ok := loop_argument.([]interface{})
			// 2.2. 如果loop_argument 不能转换成 splice, 则该子节点的loop_argument 有问题，且必然已经被置为 failed 状态
			if !ok {
				faieldComponentNames = append(faieldComponentNames, cps[0].getFullName())
			} else if len(cps) != len(loop_args) {
				if !drt.isTerminating() {
					drt.updateStatus(StatusRuntimeRunning)
					return ""
				}
			}
		}

		for index := range cps {
			if cps[index].isFailed() {
				faieldComponentNames = append(faieldComponentNames, cps[index].getFullName())
			} else if cps[index].isTerminated() {
				terminatedComponentNames = append(terminatedComponentNames, cps[index].getFullName())
			} else if cps[index].isCancelled() {
				cancelledComponentNames = append(cancelledComponentNames, cps[index].getFullName())
			} else if cps[index].isSucceeded() {
				succeededComponentNames = append(succeededComponentNames, cps[index].getFullName())
			} else if cps[index].isSkipped() {
				skippedComponentNames = append(skippedComponentNames, cps[index].getFullName())
			} else if !drt.isTerminating() {
				drt.updateStatus(StatusRuntimeRunning)
				return ""
			}
		}
	}

	var msg string
	if len(faieldComponentNames) != 0 {
		drt.updateStatus(StatusRuntimeFailed)
		msg = fmt.Sprintf("update Compoent[%s]'s status to [%s] due to subcomponents[%s] faield",
			drt.fullName, strings.Join(faieldComponentNames, string(',')))
	} else if len(terminatedComponentNames) != 0 {
		if drt.status != StatusRuntimeTerminating {
			drt.updateStatus(StatusRuntimeFailed)
			msg = fmt.Sprintf("update Compoent[%s]'s status to [%s] due to subcomponents[%s] faield",
				drt.fullName, StatusRuntimeFailed, strings.Join(terminatedComponentNames, string(',')))
		} else {
			drt.updateStatus(StatusRuntimeTerminated)
			msg = fmt.Sprintf("update Compoent[%s]'s status to [%s] due to subcomponents[%s] terminated",
				drt.fullName, StatusRuntimeFailed, strings.Join(terminatedComponentNames, string(',')))
		}
	} else if len(cancelledComponentNames) != 0 {
		// 如果节点的状态是 cancelled，只有两种情况：
		// 1、有节点运行失败，触发了 FailureOptions 机制，这种情况在上面已经处理
		// 2、收到终止信号
		drt.updateStatus(StatusRuntimeTerminated)
		msg = fmt.Sprintf("update Compoent[%s]'s status to [%s] due to subcomponents[%s] cancelled",
			drt.fullName, StatusRuntimeFailed, strings.Join(terminatedComponentNames, string(',')))
	} else {
		drt.updateStatus(StatusRuntimeSucceeded)
	}

	if msg != "" {
		drt.logger.Infoln(msg)
	}

	return msg
}
