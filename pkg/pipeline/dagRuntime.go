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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	. "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

type CtxAndCancel struct {
	ctx    context.Context
	cancel context.CancelFunc
}

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

	// 需要使用锁的原因： 避免在调度循环结构的时候，此时收到终止信号，出现一个协程在创建 runtime， 另一个协程在终止runtime的情况。
	processSubComponentLock sync.Mutex

	failureOptionsCtxAndCancels map[string]CtxAndCancel
	hasFailureOptionsTriggered  bool
}

func generateDagID(runID string) string {
	return "dag-" + runID + "-" + GetRandID(DagIDRandCodeNum)
}

func NewDagRuntime(name, fullName string, dag *schema.WorkflowSourceDag, seq int, ctx context.Context, failureOpitonsCtx context.Context,
	eventChannel chan<- WorkflowEvent, config *runConfig, parentDagID string) *DagRuntime {
	nrt := NewBaseComponentRuntime(name, fullName, dag, seq, ctx, failureOpitonsCtx, eventChannel, config, parentDagID)

	res := NewReferenceSolver(config.WorkflowSource)

	ID := generateDagID(config.runID)

	drt := &DagRuntime{
		baseComponentRuntime:        nrt,
		referenceSolver:             res,
		ID:                          ID,
		subComponentRumtimes:        make(map[string][]componentRuntime),
		failureOptionsCtxAndCancels: make(map[string]CtxAndCancel),
	}

	err := drt.updateStatus(StatusRuntimeInit)
	if err != nil {
		// 理论上不会出现这种情况，主要是为了承接 err，对齐进行判断
		errMsg := fmt.Sprintf("update status for dag[%s] failed: %s", drt.name, err.Error())
		drt.logger.Infof(errMsg)
		drt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
		return nil
	}

	ds := NewDependencySolver(drt)
	drt.DependencySolver = ds

	return drt
}

// NewDagRuntimeWithStatus: 在创建Runtime 的同时，指定runtime的状态
// 主要用于重启或者父节点调度子节点的失败时调用， 将相关信息通过evnet 的方式同步给其父节点， 并同步至数据库中
func newDagRuntimeWithStatus(name, fullName string, dag *schema.WorkflowSourceDag, seq int, ctx context.Context, failureOpitonsCtx context.Context,
	eventChannel chan<- WorkflowEvent, config *runConfig, parentDagID string, status RuntimeStatus, msg string) *DagRuntime {
	// 调用方在调用本函数前，需要保证 component 是一个 dag 类型的节点，所以此时NewDagRuntime 不应该会报错，故忽略该错误信息
	drt := NewDagRuntime(name, fullName, dag, seq, ctx, failureOpitonsCtx, eventChannel, config, parentDagID)

	drt.processStartAbnormalStatus(msg, status)
	return drt
}

func (drt *DagRuntime) generateSubRuntimeName(subComponentName string, seq int) string {
	if seq == 0 {
		return strings.Join([]string{drt.name, subComponentName}, ".")
	} else {
		runtimeName := strings.Join([]string{drt.name, subComponentName}, ".")
		return strings.Join([]string{runtimeName, strconv.Itoa(seq)}, "-")
	}
}

func (drt *DagRuntime) generateSubComponentFullName(subComponentName string) string {
	return strings.Join([]string{drt.componentFullName, subComponentName}, ".")
}

func (drt *DagRuntime) getReadyComponent() map[string]schema.Component {
	readyComponent := map[string]schema.Component{}
	for name, subComponent := range drt.getworkflowSouceDag().EntryPoints {
		// 如果已经生成了对应的 Runtime，则说明对应该 Component 已经被调度了
		if _, ok := drt.subComponentRumtimes[name]; ok {
			continue
		}

		// 判读所有的上游节点是否处于终态，是则说明当前已经处于可调度的状态
		isReady := true
		for _, depComponentName := range subComponent.GetDeps() {
			// 若节点没有对应的runtime，则说明该节点尚未运行
			depComponentRuntimes, ok := drt.subComponentRumtimes[depComponentName]
			if !ok {
				isReady = false
				break
			}

			// 如果存在了 runtime， 需要满足以下条件：
			// runtime 的数目需要等于 loopArgument 的项数， 最低为1，且所有的runtime 均需处于 succeeded 或者 skipped 状态
			lp := depComponentRuntimes[0].getComponent().GetLoopArgument()
			if lp != nil {
				// 此时无需在校验 loopArgument 的类型，因为其在创建 对应的 runtime 必然已经经过校验
				v := reflect.ValueOf(lp)

				if v.Len() != len(depComponentRuntimes) {
					isReady = false
					break
				}
			}

			for _, depRuntime := range depComponentRuntimes {
				if !depRuntime.isSucceeded() && !depRuntime.isSkipped() {
					isReady = false
					break
				}
			}
		}

		if isReady {
			readyComponent[name] = subComponent
		}
	}

	drt.logger.Infof("get ready subStep or subDag[%v] for dag[%s]", readyComponent, drt.name)
	return readyComponent
}

// resolveReference: 主要用于解析 reference 字段
func (drt *DagRuntime) resolveReference(subComponentName string, subComponent schema.Component) (schema.Component, error) {
	subName := drt.generateSubRuntimeName(subComponentName, 0)
	drt.logger.Debugf("begin to resolve reference for subStep or subDag[%s", subName)

	newComponent, err := drt.referenceSolver.resolveComponentReference(subComponent)
	if err != nil {
		return nil, err
	}

	drt.component.(*schema.WorkflowSourceDag).EntryPoints[subComponentName] = newComponent

	return newComponent, nil
}

// createAndStartSubComponentRuntime: 创建并运行子节点 runtime
// 无需返回 error 原因是将通过 event 来进行同步
func (drt *DagRuntime) createAndStartSubComponentRuntime(subComponentName string, subComponent schema.Component,
	exceptSeq map[int]int) {

	subName := drt.generateSubRuntimeName(subComponentName, 0)
	subFullName := drt.generateSubComponentFullName(subComponentName)
	drt.logger.Infof("begin to create runtime for %s[%s]", subComponent.GetType(), subName)

	// 如果已经有子节点对应的 runtime, 则说明该节点已经被调度过了. 此时终止任务
	// PS: 理论上不会出现在这种情况，用于兜底
	_, ok := drt.subComponentRumtimes[subComponentName]
	if ok && len(exceptSeq) == 0 {
		drt.logger.Errorf("%s[%s] has been scheduled", subComponent.GetType(), subComponentName)

		drt.ctx.Done()
		return
	}

	// 1. 获取 新的副本，避免循环结构的多次运行访问了同一个对象, 因为子节点是以指针形式存储的
	newSubComponent := subComponent.DeepCopy()

	// 2. 替换上下游参数模板
	err := drt.DependencySolver.ResolveBeforeRun(newSubComponent)
	if err != nil {
		drt.logger.Errorln(err.Error())
		drt.processSubRuntimeError(err, newSubComponent, StatusRuntimeFailed)
		return
	}

	// 1. 获取 loop_arguemnt, 确定需要创建多少次runtime
	isv := NewInnerSolver(newSubComponent, subName, drt.runConfig)
	err = isv.resolveLoopArugment()
	if err != nil {
		err := fmt.Errorf("cannot get the value of loop_arugment for %s[%s]", newSubComponent.GetType(), subName)
		drt.logger.Errorln(err.Error())
		drt.processSubRuntimeError(err, newSubComponent, StatusRuntimeFailed)
		return
	}

	ll := newSubComponent.GetLoopArgumentLength()
	if ll == 0 {
		ll = 1
	}

	// 同一个节点的多次运行，共享同一个 failureOptionsCtx
	ctxAndCc := drt.getfailureOptionsCtxAndCF(subComponentName)
	step, isStep := newSubComponent.(*schema.WorkflowSourceStep)
	dag, _ := newSubComponent.(*schema.WorkflowSourceDag)

	for index := 0; index < ll; index++ {
		if _, ok := exceptSeq[index]; ok {
			continue
		}

		subName = drt.generateSubRuntimeName(subComponentName, index)
		var subRuntime componentRuntime
		if isStep {
			// 这里需要对 step 进行复制， 避免多个subRuntime 使用了同一个 component， 导致并发问题
			subRuntime = NewStepRuntime(subName, subFullName, step.DeepCopy().(*schema.WorkflowSourceStep), index,
				drt.ctx, ctxAndCc.ctx, drt.receiveEventChildren, drt.runConfig, drt.ID)
		} else {
			subRuntime = NewDagRuntime(subName, subFullName, dag.DeepCopy().(*schema.WorkflowSourceDag), index,
				drt.ctx, ctxAndCc.ctx, drt.receiveEventChildren, drt.runConfig, drt.ID)
		}
		drt.subComponentRumtimes[subComponentName] = append(drt.subComponentRumtimes[subComponentName], subRuntime)

		drt.logger.Infof("begion to run %s[%s]", newSubComponent.GetType(), subRuntime.getName())
		go subRuntime.Start()
	}
}

func (drt *DagRuntime) getworkflowSouceDag() *schema.WorkflowSourceDag {
	dag := drt.getComponent().(*schema.WorkflowSourceDag)
	return dag
}

// 开始执行 runtime
// 不返回error，直接通过 event 向上冒泡
func (drt *DagRuntime) Start() {
	drt.logger.Infof("begin to run dag[%s]", drt.name)

	defer drt.processSubComponentLock.Unlock()
	drt.processSubComponentLock.Lock()

	err := drt.updateStatus(StatusRuntimeRunning)
	if err != nil {
		// 理论上不会出现这种情况，主要是为了承接 err，对齐进行判断
		errMsg := fmt.Sprintf("update status for dag[%s] failed: %s", drt.name, err.Error())
		drt.logger.Infof(errMsg)
		drt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
		return
	}

	drt.startTime = time.Now().Format("2006-01-02 15:04:05")

	// TODO: 此时是否需要同步至数据库？

	// 1、 更新系统变量
	err = drt.setSysParams()
	if err != nil {
		errMsg := fmt.Sprintf("set the sysparams for dag[%s] failed: %s", drt.name, err.Error())
		drt.logger.Errorln(errMsg)
		drt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
		return
	}

	// 2、替换 condition，loop_argument 中的模板，将其替换成具体真实值
	condition, err := drt.CalculateCondition()
	if err != nil {
		errMsg := fmt.Sprintf("caculate the condition field for dag[%s] faild:\n%s",
			drt.name, err.Error())
		drt.logger.Errorln(errMsg)
		drt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
		return
	}

	if !condition {
		skipMsg := fmt.Sprintf("the result of condition for  dag[%s] is false, skip running", drt.name)
		drt.logger.Infoln(skipMsg)
		drt.processStartAbnormalStatus(skipMsg, StatusRuntimeSkipped)
		return
	}

	if drt.isDisabled() {
		skipMsg := fmt.Sprintf("dag[%s] is disabled, skip running", drt.name)
		drt.logger.Infoln(skipMsg)
		drt.processStartAbnormalStatus(skipMsg, StatusRuntimeSkipped)
		return
	}

	view := drt.newView("begin to run")
	drt.syncToApiServerAndParent(WfEventDagUpdate, &view, "begin to run")
	// 监听子节点已经父节点传递过来的事件或者信号
	go drt.Listen()
	go drt.Stop()

	// 开始调度子节点
	drt.scheduleSubComponent()
}

// scheduleSubComponent: 调度子节点运行
// 不返回error，直接通过 event 向上冒泡
func (drt *DagRuntime) scheduleSubComponent() {
	// 1、获取可以进行调度的节点
	readyComponent := drt.getReadyComponent()

	for subComponentName, subComponent := range readyComponent {
		// 如果此时收到了终止信号，则无需调度子节点
		if drt.ctx.Err() != nil || drt.failureOpitonsCtx.Err() != nil {
			drt.logger.Infof("dag[%s] receives temination signal, "+
				"so it's subStep or subDag wouldn't be scheduled anymore",
				drt.name)
			return
		}

		drt.logger.Infof("begin to schedule sub%s[%s] of dag[%s]",
			subComponent.GetType(), subComponentName, drt.name)

		// 如果此时的状态为 terminating 或者处于终态， 也不应该在调度子节点
		if drt.isTerminating() || drt.isDone() {
			drt.logger.Infof("the status of dag[%s] is [%s], so it's subStep or subDag wouldn't be scheduled anymore",
				drt.name, drt.status)
			return
		}

		// 2. Component 替换： 主要是处理 reference 字段
		newSubCp, err := drt.resolveReference(subComponentName, subComponent)
		if err != nil {
			drt.logger.Errorln(err.Error())

			// 创建占位用 runtime
			drt.processSubRuntimeError(err, subComponent, StatusRuntimeFailed)
			continue
		}

		// 4. 创建 runtime 并运行 runtime
		drt.createAndStartSubComponentRuntime(subComponentName, newSubCp, map[int]int{})
	}
}

// 监听由子节点发送过来的信号
func (drt *DagRuntime) Listen() {
	// drt 的状态处于终态时，一定要return
	for {
		event := <-drt.receiveEventChildren
		if drt.done {
			return
		}
		if err := drt.processEventFromSubComponent(event); err != nil {
			// how to read event?
			drt.logger.Infof("process event failed %s", err.Error())
		}
	}
}

func (drt *DagRuntime) Resume(dagView *schema.DagView) {
	drt.logger.Infof("resume dag[%s]", drt.name)

	// 1、从 DagView 中获取必要的信息
	defer drt.processSubComponentLock.Unlock()
	drt.processSubComponentLock.Lock()

	drt.pk = dagView.PK
	drt.ID = dagView.DagID
	drt.startTime = dagView.StartTime

	err := drt.setSysParams()
	if err != nil {
		errMsg := fmt.Sprintf("set the sysparams for dag[%s] failed: %s", drt.name, err.Error())
		drt.logger.Errorln(errMsg)
		drt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
		return
	}

	// 如果dagview 的EntryPoint为一个空map， 则直接走 start 逻辑
	if len(dagView.EntryPoints) == 0 {
		// 这里使用协程主要是为了避免死锁
		go drt.Start()
		return
	}

	err = drt.updateStatus(dagView.Status)
	if err != nil {
		// 理论上不会出现这种情况，主要是为了承接 err，对齐进行判断
		errMsg := fmt.Sprintf("update status for dag[%s] failed: %s", drt.name, err.Error())
		drt.logger.Infof(errMsg)
		drt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
		return
	}

	// 相关校验逻辑由 parser 模块负责，此处不做校验
	sorted, _ := TopologicalSort(drt.getworkflowSouceDag().EntryPoints)

	for _, name := range sorted {
		views, ok := dagView.EntryPoints[name]
		if !ok {
			// 说面当前节点还没有运行，在此处不进行处理
			continue
		}

		_, ok = drt.subComponentRumtimes[name]
		if ok {
			// 此时说明该节点已经被 failureOptions机制处理过了，直接跳过
			continue
		}

		if drt.ctx.Err() != nil || drt.failureOpitonsCtx.Err() != nil {
			drt.logger.Infof("received terminating signal, stop resume")
			return
		}

		component := drt.getworkflowSouceDag().EntryPoints[name].DeepCopy()
		// 替换 reference 字段
		newCp, err := drt.resolveReference(name, component)
		if err != nil {
			drt.logger.Errorln(err.Error())
			drt.processSubRuntimeError(err, component, StatusRuntimeFailed)
			continue
		}

		component = newCp

		// 替换 parameter 与 artifact 中的模板
		err = drt.DependencySolver.ResolveBeforeRun(component)
		if err != nil {
			drt.logger.Errorln("ResolveBeforeRun failed:", err.Error())
			drt.processSubRuntimeError(err, component, StatusRuntimeFailed)
			continue
		}

		// 解析loop_argument
		subName := drt.generateSubRuntimeName(name, 0)
		isv := NewInnerSolver(component, subName, drt.runConfig)
		err = isv.resolveLoopArugment()
		if err != nil {
			err := fmt.Errorf("cannot get the value of loop_arugment for %s[%s]", component.GetType(), subName)
			drt.logger.Errorln(err.Error())
			drt.processSubRuntimeError(err, component, StatusRuntimeFailed)
			continue
		}

		// exceptSeq 的value 无实义，仿set
		exceptSeq := map[int]int{}
		_, isStep := component.(*schema.WorkflowSourceStep)
		for _, view := range views {
			exceptSeq[view.GetSeq()] = 1
			status := view.GetStatus()

			runtime := drt.CreateSubRuntimeAccordingView(view, name)
			err := runtime.updateStatus(status)
			if err != nil {
				drt.logger.Errorln(err.Error())
				drt.processSubRuntimeError(err, component, StatusRuntimeFailed)
				continue
			}

			drt.subComponentRumtimes[name] = append(drt.subComponentRumtimes[name], runtime)
			drt.logger.Infof("recreated runtime for %s[%s] with status[%s]",
				component.GetType(), runtime.getName(), runtime.getStatus())

			if runtime.isDone() {
				// 如果已经处于终态，则无需 resume, 也无需在向数据库同步
				// 但是对于 failed 节点，需要处理failureOptions。 以避免子节点的状态同步问题
				if runtime.isFailed() {
					msg := fmt.Sprintf("sub%s[%s] already in status[failed]", component.GetType(), runtime.getName())
					extra := map[string]interface{}{
						common.WfEventKeyRunID:         drt.runID,
						common.WfEventKeyStatus:        drt.status,
						common.WfEventKeyComponentName: drt.getComponent().GetName(),
						common.WfEventKeyView:          view,
					}

					ev := NewWorkflowEvent(WfEventFailureOptionsTriggered, msg, extra)
					drt.ProcessFailureOptions(*ev)
				} else {
					if isStep {
						go runtime.(*StepRuntime).Resume(view.(*schema.JobView))
					} else {
						go runtime.(*DagRuntime).Resume(view.(*schema.DagView))
					}
				}
			}
		}

		ll := component.GetLoopArgumentLength()
		if ll == 0 {
			ll = 1
		}

		if len(exceptSeq) < ll {
			drt.createAndStartSubComponentRuntime(name, component, exceptSeq)
		}
	}

	// 在最后才进入listen状态，为了避免在resume A 子节点的过程中，监听到了B 发过来的事件，有一次调度了子节点A
	go drt.Listen()

	// 避免子节点恰好都是终态的情况，导致没有step 运行，run 卡住的情况
	drt.scheduleSubComponent()
}

// 重新执行
// TODO
func (drt *DagRuntime) Restart(dagView *schema.DagView) {
	drt.logger.Infof("restart dag[%s]", drt.name)

	defer drt.processSubComponentLock.Unlock()
	drt.processSubComponentLock.Lock()

	drt.logger.Infof("pk in dagView for dag[%s]: %d", drt.name, dagView.PK)
	drt.logger.Infof("DagID in dagView for dag[%s]: %s", drt.name, dagView.DagID)

	drt.pk = dagView.PK
	drt.ID = dagView.DagID
	drt.startTime = dagView.StartTime

	err := drt.setSysParams()
	if err != nil {
		errMsg := fmt.Sprintf("set the sysparams for dag[%s] failed: %s", drt.name, err.Error())
		drt.logger.Errorln(errMsg)
		drt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
		return
	}

	// 如果dagview 的EntryPoint为一个空map， 则直接走 start 逻辑
	if len(dagView.EntryPoints) == 0 {
		// 这里使用协程主要是为了避免死锁
		go drt.Start()
		return
	}

	// 2、 对于已经有处于 succeeded 、 running、 skipped 状态的runtime的节点，说明其一定是处于可调度的状态，
	// 此时需要判断其对应的节点是否为 循环结构，是的话，可能有某几次运行失败，或者还没有来的及发起，此时我们需要补齐缺失的运行
	drt.scheduleSubComponentAccordingView(dagView)

	// 3、处理完所有的view 后 才开始 监听信号, 主要是为了在还没有处理完 view 中新，便接受到了事件， 导致在 view 中存在的节点再次被调度
	go drt.Listen()
	go drt.Stop()

	// 4、这里做一次调度的原因是，避免 3 中没有发起任何任务，导致永远监听不到信息，导致任务 hang 住的情况出现
	drt.scheduleSubComponent()

	return
}

func (drt *DagRuntime) CreateSubRuntimeAccordingView(view schema.ComponentView, name string) componentRuntime {
	JobView, ok := view.(*schema.JobView)
	if ok {
		return drt.creatStepRuntimeAccordingView(JobView, name)
	}

	return drt.createDagRuntimeAccordingView(view.(*schema.DagView), name)
}

func (drt *DagRuntime) creatStepRuntimeAccordingView(view *schema.JobView, name string) componentRuntime {
	runtimeName := drt.generateSubRuntimeName(name, view.LoopSeq)
	fullName := drt.generateSubComponentFullName(name)

	ctxAndcc := drt.getfailureOptionsCtxAndCF(name)

	step := *drt.getworkflowSouceDag().EntryPoints[name].DeepCopy().(*schema.WorkflowSourceStep)
	stepPtr := &step
	srt := NewStepRuntime(runtimeName, fullName, stepPtr,
		view.LoopSeq, drt.ctx, ctxAndcc.ctx, drt.receiveEventChildren, drt.runConfig, drt.ID)

	return srt
}

func (drt *DagRuntime) createDagRuntimeAccordingView(view *schema.DagView, name string) componentRuntime {
	runtimeName := drt.generateSubRuntimeName(name, view.LoopSeq)
	fullName := drt.generateSubComponentFullName(name)

	ctxAndcc := drt.getfailureOptionsCtxAndCF(name)

	dag := *drt.getworkflowSouceDag().EntryPoints[name].DeepCopy().(*schema.WorkflowSourceDag)
	dagPtr := &dag
	sDrt := NewDagRuntime(runtimeName, fullName, dagPtr,
		view.LoopSeq, drt.ctx, ctxAndcc.ctx, drt.receiveEventChildren, drt.runConfig, drt.ID)

	return sDrt
}

func (drt *DagRuntime) scheduleSubComponentAccordingView(dagView *schema.DagView) {
	sorted, _ := TopologicalSort(drt.getworkflowSouceDag().EntryPoints)
	drt.logger.Infof("toposort in dag[%s] is %v", drt.name, sorted)

	err := drt.updateStatus(StatusRuntimeRunning)
	if err != nil {
		// 理论上不会出现这种情况，主要是为了承接 err，对齐进行判断
		errMsg := fmt.Sprintf("update status for dag[%s] failed: %s", drt.name, err.Error())
		drt.logger.Infof(errMsg)
		drt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
		return
	}

	view := drt.newView("begin to restart")
	drt.syncToApiServerAndParent(WfEventDagUpdate, &view, "begin to restart")

	// 这里需要重置 subComponentRumtimes 原因是在 needRestarted 函数中，并没有保存完整的view信息
	drt.subComponentRumtimes = map[string][]componentRuntime{}

	for _, name := range sorted {
		// 如果此时收到了终止信号，则无需调度子节点
		if drt.ctx.Err() != nil || drt.failureOpitonsCtx.Err() != nil {
			drt.logger.Infof("dag[%s] receives temination signal, so it's subStep or subDag wouldn't be scheduled anymore",
				drt.name)
			return
		}

		views, ok := dagView.EntryPoints[name]
		if !ok {
			continue
		}

		// restart 时，所有子节点rumtine都处于终态，可以分成三类：
		// succeeded, skipped: 对于这类runtime无需重启，在 subruntime 中记录即可
		// failed， terminated: 需要重启
		// cancelled: 分两种情况：
		// - 如果一个节点 已有的所有运行都是 cancelled 状态（说明该节点此前没有被正常调度），此时该节点应该被重新调度，不应该在这里处理
		// - 如果该节点 有一次运行不是 cancelled，则说明该节点之前已经被调度，此时需要重跑
		needRestart := false
		for _, view := range views {
			if view.GetStatus() != StatusRuntimeCancelled {
				needRestart = true
				break
			}
		}

		if !needRestart {
			continue
		}

		drt.logger.Infof("begin to restart subStep or subDag[%s] for dag[%s]", name, drt.name)

		component := drt.getworkflowSouceDag().EntryPoints[name].DeepCopy()
		// 替换 reference 字段
		newCp, err := drt.resolveReference(name, component)
		if err != nil {
			drt.logger.Errorln(err.Error())
			drt.processSubRuntimeError(err, component, StatusRuntimeFailed)
			continue
		}

		component = newCp

		// 替换 parameter 与 artifact 中的模板
		err = drt.DependencySolver.ResolveBeforeRun(component)
		if err != nil {
			drt.logger.Errorln("ResolveBeforeRun failed:", err.Error())
			drt.processSubRuntimeError(err, component, StatusRuntimeFailed)
			continue
		}

		// 解析loop_argument
		subName := drt.generateSubRuntimeName(name, 0)
		isv := NewInnerSolver(component, subName, drt.runConfig)
		err = isv.resolveLoopArugment()
		if err != nil {
			err := fmt.Errorf("cannot get the value of loop_arugment for %s[%s]", component.GetType(), subName)
			drt.logger.Errorln(err.Error())
			drt.processSubRuntimeError(err, component, StatusRuntimeFailed)
			continue
		}

		// 对于restart 的场景，节点的 loop_argument 的长度一定与view的数量 相等
		_, isStep := component.(*schema.WorkflowSourceStep)
		for _, view := range views {
			status := view.GetStatus()

			runtime := drt.CreateSubRuntimeAccordingView(view, name)
			drt.subComponentRumtimes[name] = append(drt.subComponentRumtimes[name], runtime)

			if status == StatusRuntimeSucceeded || status == StatusRuntimeSkipped {
				// 对于step, 此时不能降低并发度
				if isStep {
					err := runtime.(*StepRuntime).baseComponentRuntime.updateStatus(view.GetStatus())
					if err != nil {
						drt.logger.Errorln(err.Error())
						drt.processSubRuntimeError(err, component, StatusRuntimeFailed)
						continue
					}
				} else {
					err := runtime.updateStatus(view.GetStatus())
					if err != nil {
						drt.logger.Errorln(err.Error())
						drt.processSubRuntimeError(err, component, StatusRuntimeFailed)
						continue
					}
				}

				drt.logger.Infof("sub%s[%s] don't need restart, because it's already in status[%s]",
					component.GetType(), runtime.getName(), runtime.getStatus())
				continue
			}

			if isStep {
				go runtime.(*StepRuntime).Restart(view.(*schema.JobView))
			} else {
				go runtime.(*DagRuntime).Restart(view.(*schema.DagView))
			}
		}
	}

	return
}

func (drt *DagRuntime) GetSubComponentParameterValue(componentName string, paramName string) (interface{}, error) {
	var value interface{}
	var err error
	subComponentsRuntime, ok := drt.subComponentRumtimes[componentName]
	if !ok {
		err := fmt.Errorf("cannot get the value of parameter[%s] from subDag or subStep[%s], "+
			"because there is no runtime for that subDag or subStep in dag[%s]", paramName, drt.name+"."+componentName,
			drt.name)
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
	var value []string
	subComponents, ok := drt.subComponentRumtimes[componentName]
	if !ok {
		err := fmt.Errorf("cannot get the value of artifact[%s] from subDag or subStep[%s], "+
			"because there is no runtime for that subDag or subStep in dag[%s]",
			artName, drt.name+"."+componentName, drt.name)
		return "", err
	} else {
		for index := range subComponents {
			p, err := subComponents[index].getComponent().GetArtifactPath(artName)
			if err != nil {
				return "", err
			}

			value = append(value, p)
		}
	}

	return strings.Join(value, ","), nil
}

// processEventFromSubComponent 处理 stepRuntime 推送过来的 run 的事件
// 对于异常处理的情况
// 1. 提交失败，job id\status 都为空，视为 job 失败，更新 run message 字段
// 2. watch 失败，状态不更新，更新 run message 字段；等 job 恢复服务之后，job watch 恢复，run 自动恢复调度
// 3. stop 失败，状态不更新，run message 字段；需要用户根据提示再次调用 stop
// 4. 如果有 job 的状态异常，将会走 FailureOptions 的处理逻辑
func (drt *DagRuntime) processEventFromSubComponent(event WorkflowEvent) error {
	defer drt.processSubComponentLock.Unlock()
	drt.processSubComponentLock.Lock()

	if drt.isDone() {
		drt.logger.Infof("workflow has completed. skip event")
		return nil
	}
	drt.logger.Infof("process event: [%+v]", event)

	// 判断事件类型是否为 failureOptionstriggered 类型，是的话，执行 processFailureOptions
	// 只有当 策略为 FailFast 的时候才会接受到该事件
	if event.isFailureOptionsTriggered() {
		drt.ProcessFailureOptions(event)
	} else {
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
	}

	StatusMsg := drt.updateStatusAccordingSubComponentRuntimeStatus()
	view := drt.newView(StatusMsg)
	drt.syncToApiServerAndParent(WfEventDagUpdate, &view, StatusMsg)

	// 如果 dagRuntime 未处于终态，则需要判断是否有新的子节点可以运行
	if !drt.done {
		drt.scheduleSubComponent()
	}

	return nil
}

func (drt *DagRuntime) getDirectDownstreamComponent(componentName string) (DowncomponentNames map[string]string) {
	// 因为golang 没有 set，所以使用 map 模拟一个 set，steps 的value 并没有实际意义
	DowncomponentNames = map[string]string{}

	for subName, subComponent := range drt.getworkflowSouceDag().EntryPoints {
		deps := subComponent.GetDeps()
		for _, dep := range deps {
			dep = strings.Trim(dep, " ")
			if dep == componentName {
				DowncomponentNames[subName] = subName
				break
			}
		}
	}
	return DowncomponentNames
}

func (drt *DagRuntime) getAllDownstreamComponents(component schema.Component) (allDowncomponentNames map[string]string) {
	allDowncomponentNames = map[string]string{}
	toVisiteComponents := drt.getDirectDownstreamComponent(component.GetName())

	// 循环获取下游节点的下游节点，直至叶子节点
	for {
		downstreamComponents := map[string]string{}
		for _, componentName := range toVisiteComponents {
			downstreamStep := drt.getDirectDownstreamComponent(componentName)
			allDowncomponentNames[componentName] = componentName

			for downComponent, _ := range downstreamStep {
				// 判断 downStep 是否已经解析过其下游节点
				_, ok := allDowncomponentNames[downComponent]
				if !ok {
					downstreamComponents[downComponent] = downComponent
				}
			}
		}

		if len(downstreamComponents) == 0 {
			break
		} else {
			toVisiteComponents = downstreamComponents
		}
	}
	return allDowncomponentNames
}

func (drt *DagRuntime) getfailureOptionsCtxAndCF(subComponentName string) CtxAndCancel {
	if ctxAndcc, ok := drt.failureOptionsCtxAndCancels[subComponentName]; ok {
		return ctxAndcc
	}

	failureOptionsctx, failureOptionsCancel := context.WithCancel(context.Background())
	drt.failureOptionsCtxAndCancels[subComponentName] = CtxAndCancel{
		ctx:    failureOptionsctx,
		cancel: failureOptionsCancel,
	}

	return drt.failureOptionsCtxAndCancels[subComponentName]
}

func (drt *DagRuntime) ProcessFailureOptionsWithContinue(component schema.Component) {
	// 对于 Continue 策略，需要支持多次触发（不同的分支节点都有可能会触发该策略）， 所以此处不设置标志位，也不对标志位进行判断

	// 失败节点的所有下游节点都将会置为failed, 此时其所有的下游节点都是没有开始执行的，
	needCancelComponentNames := drt.getAllDownstreamComponents(component)

	for name, _ := range needCancelComponentNames {
		// 1、 处理已经调度过的节点
		_, ok := drt.subComponentRumtimes[name]
		if ok {
			// 1、处理已经调度过的节点, 直接调用 failureoptionsCancel 结束运行。
			// PS: 对于已经处于终止态的 runtime, 其对应的协程也已经结束，不会监听 failureOptionsCtx 信号，所以不会有影响
			drt.getfailureOptionsCtxAndCF(name).cancel()
		} else {
			// 2、处理还没有调度的节点
			cancelComponent := drt.getworkflowSouceDag().EntryPoints[name]
			reason := fmt.Sprintf("begin to process FailureOptions, some step or dag run failed")
			drt.CancellNotReadyComponent(cancelComponent, reason)
		}
	}
}

func (drt *DagRuntime) ProcessFailureOptionsWithFailFast() {
	if drt.hasFailureOptionsTriggered {
		drt.logger.Info("failure options was processed already")
		return
	}

	drt.hasFailureOptionsTriggered = true

	for name, _ := range drt.getworkflowSouceDag().EntryPoints {
		_, ok := drt.subComponentRumtimes[name]
		if ok {
			drt.getfailureOptionsCtxAndCF(name).cancel()
		}
	}

	drt.cancellAllNotReadySubComponent("receive failure options signal")
}

func (drt *DagRuntime) ProcessFailureOptions(event WorkflowEvent) {
	drt.logger.Infof("begin to process failure options. trigger event is: %v", event)
	name, ok := event.Extra[common.WfEventKeyComponentName]
	if !ok {
		errMsg := fmt.Sprintf("cannot get the subStep or subDag info form event[%v] for dagRuntime[%s], begin to cancell all not ready step",
			drt.runID, event)
		drt.logger.Errorf(errMsg)

		// 理论上不会出现这种情况，用户兜底
		// 防止下游节点无法调度，导致任务被 hang 住，将所有还没有调度的节点置为 cancelled 状态
		// 或者直接终止 run？ 又或者终止当前的 dagRuntime？
		drt.cancellAllNotReadySubComponent(errMsg)
		return

	}

	componentName := name.(string)
	component := drt.subComponentRumtimes[componentName][0].getComponent()

	// 策略的合法性由 workflow 保证
	if drt.FailureOptions.Strategy == schema.FailureStrategyContinue {
		drt.ProcessFailureOptionsWithContinue(component)
	} else {
		// 通过事件通知其父节点处理开始处理 failureOptions
		drt.syncToApiServerAndParent(WfEventFailureOptionsTriggered, &schema.DagView{},
			fmt.Sprintf("failure options triggered by event: %v", event))

		drt.ProcessFailureOptionsWithFailFast()
	}
}

func (drt *DagRuntime) cancellAllNotReadySubComponent(errMsg string) {
	for subName, subComponent := range drt.getworkflowSouceDag().EntryPoints {
		_, ok := drt.subComponentRumtimes[subName]
		if !ok {
			drt.CancellNotReadyComponent(subComponent, errMsg)
		}
	}
}

func (drt *DagRuntime) CancellNotReadyComponent(subComponent schema.Component, reason string) {
	cancelComponentFullName := drt.generateSubComponentFullName(subComponent.GetName())
	drt.logger.Infof("begin to cancel %s[%s]: %s", subComponent.GetType(), cancelComponentFullName, reason)

	err := fmt.Errorf(reason)
	drt.processSubRuntimeError(err, subComponent, StatusRuntimeCancelled)
}

func (drt *DagRuntime) processStartAbnormalStatus(msg string, status RuntimeStatus) {
	err := drt.updateStatus(status)
	if err != nil {
		drt.logger.Errorf(err.Error())
	}

	dagView := drt.newView(msg)
	drt.syncToApiServerAndParent(WfEventDagUpdate, &dagView, msg)
}

// processSubRuntimeError： 处理调度子节点失败的情况，通过调用processEventFromSubComponent()函数来进行同步
func (drt *DagRuntime) processSubRuntimeError(err error, cp schema.Component, status RuntimeStatus) {
	componentName := cp.GetName()
	name := drt.generateSubRuntimeName(componentName, 0)
	fullName := drt.generateSubComponentFullName(componentName)

	// 将 loop_argument 置为nil，方便更新dag状态时使用
	// cp.UpdateLoopArguemt(nil)

	step, ok := cp.(*schema.WorkflowSourceStep)

	ctxAndCc := drt.getfailureOptionsCtxAndCF(componentName)

	var crt componentRuntime

	ll := cp.GetLoopArgumentLength()
	if ll == 0 {
		ll = 1
	}

	for i := 0; i < ll; i++ {
		if ok {
			crt = newStepRuntimeWithStatus(name, fullName, step, i, drt.ctx, ctxAndCc.ctx, drt.receiveEventChildren,
				drt.runConfig, drt.ID, status, err.Error())
		} else {
			dag := cp.(*schema.WorkflowSourceDag)
			crt = newDagRuntimeWithStatus(name, fullName, dag, i, drt.ctx, ctxAndCc.ctx, drt.receiveEventChildren,
				drt.runConfig, drt.ID, status, err.Error())
		}
		drt.subComponentRumtimes[componentName] = append(drt.subComponentRumtimes[componentName], crt)
	}
}

// updateStatusAccordingSubComponentRuntimeStatus: 根据子节点的状态来更新
func (drt *DagRuntime) updateStatusAccordingSubComponentRuntimeStatus() string {
	if drt.done {
		return fmt.Sprintf("already in status[%s]", drt.status)
	}

	drt.logger.Infof("begin to update status for dag[%s]", drt.name)
	// 1. 如果有子节点还没有调度，且节点本身的状态不为 Terminating， 则状态必定为running
	if len(drt.subComponentRumtimes) != len(drt.getworkflowSouceDag().EntryPoints) {
		drt.logger.Debugf("the lengthn of subruntime[%d] != the component in entrypoint[%d], current dag is: %s",
			len(drt.subComponentRumtimes), len(drt.getworkflowSouceDag().EntryPoints), drt.name)

		if !drt.isTerminating() {
			err := drt.updateStatus(StatusRuntimeRunning)
			if err != nil {
				// 理论上不会出现这种情况，主要是为了承接 err，对齐进行判断
				errMsg := fmt.Sprintf("update status for dag[%s] failed: %s", drt.name, err.Error())
				drt.logger.Infof(errMsg)
				drt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
				return errMsg
			}
		}
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
		cp := cps[0]
		loop_argument := cp.getComponent().GetLoopArgument()
		if loop_argument != nil {
			// 2.2. 如果loop_argument 不能转换成 slice, 则说明在创建节点失败，是否还没有正式运行，此时直接在下方进行统计即可
			t := reflect.TypeOf(loop_argument)
			if t.Kind() != reflect.Slice {
				continue
			} else {
				v := reflect.ValueOf(loop_argument)
				if len(cps) != v.Len() && v.Len() != 0 {
					// v.Len 为 0 时， 会有一个占位用户的 cp
					if !drt.isTerminating() {
						err := drt.updateStatus(StatusRuntimeRunning)
						if err != nil {
							// 理论上不会出现这种情况，主要是为了承接 err，对齐进行判断
							errMsg := fmt.Sprintf("update status for dag[%s] failed: %s", drt.name, err.Error())
							drt.logger.Infof(errMsg)
							drt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
							return errMsg
						}
					}
					drt.logger.Debugf("cpname: %s, len of cps: %d, len of lp: %d, ld:%v",
						cp.getName(), len(cps), v.Len(), loop_argument)
					return ""
				}
			}
		}

		for index := range cps {
			if cps[index].isFailed() {
				faieldComponentNames = append(faieldComponentNames, cps[index].getName())
			} else if cps[index].isTerminated() {
				terminatedComponentNames = append(terminatedComponentNames, cps[index].getName())
			} else if cps[index].isCancelled() {
				cancelledComponentNames = append(cancelledComponentNames, cps[index].getName())
			} else if cps[index].isSucceeded() {
				succeededComponentNames = append(succeededComponentNames, cps[index].getName())
			} else if cps[index].isSkipped() {
				skippedComponentNames = append(skippedComponentNames, cps[index].getName())
			} else {
				if !drt.isTerminating() {
					err := drt.updateStatus(StatusRuntimeRunning)
					if err != nil {
						// 理论上不会出现这种情况，主要是为了承接 err，对齐进行判断
						errMsg := fmt.Sprintf("update status for dag[%s] failed: %s", drt.name, err.Error())
						drt.logger.Infof(errMsg)
						drt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
						return errMsg
					}
				}
				return ""
			}
		}
	}

	var msg string
	var err error
	if len(faieldComponentNames) != 0 {
		err = drt.updateStatus(StatusRuntimeFailed)
		msg = fmt.Sprintf("update dag[%s]'s status to [%s] due to subSteps or subDags[%s] faield",
			drt.name, StatusRuntimeFailed, strings.Join(faieldComponentNames, ","))
	} else if len(terminatedComponentNames) != 0 {
		if drt.status != StatusRuntimeTerminating {
			err = drt.updateStatus(StatusRuntimeFailed)
			msg = fmt.Sprintf("update dag[%s]'s status to [%s] due to subSteps or subDags[%s] abnormally terminated",
				drt.name, StatusRuntimeFailed, strings.Join(terminatedComponentNames, ","))
		} else {
			err = drt.updateStatus(StatusRuntimeTerminated)
			msg = fmt.Sprintf("update dag[%s]'s status to [%s] due to subSteps or subDags[%s] terminated",
				drt.name, StatusRuntimeTerminated, strings.Join(terminatedComponentNames, ","))
		}
	} else if len(cancelledComponentNames) != 0 {
		// 如果节点的状态是 cancelled，只有两种情况：
		// 1、有节点运行失败，触发了 FailureOptions 机制，这种情况在上面已经处理
		// 2、收到终止信号
		err = drt.updateStatus(StatusRuntimeTerminated)
		msg = fmt.Sprintf("update dag[%s]'s status to [%s] due to subSteps or subDags[%s] cancelled",
			drt.name, StatusRuntimeTerminated, strings.Join(terminatedComponentNames, ","))
	} else {
		// 回填本节点的输出artifact
		names := append(succeededComponentNames, skippedComponentNames...)
		msg = fmt.Sprintf("all subDag or subStep run succeeded or skipped: %s", strings.Join(names, ","))
		err = drt.ResolveAfterDone()
		if err != nil {
			err = drt.updateStatus(StatusRuntimeFailed)
		} else {
			err = drt.updateStatus(StatusRuntimeSucceeded)
		}
	}

	if err != nil {
		// 理论上不会出现这种情况，为了保持完备性增加此处判断，因此此处只需打印日志即可
		drt.logger.Errorf(err.Error())
	}

	if msg != "" {
		drt.logger.Infoln(msg)
	}

	return msg
}

func (drt *DagRuntime) updateStatus(status RuntimeStatus) error {
	err := drt.baseComponentRuntime.updateStatus(status)
	if err != nil {
		return err
	}

	if drt.done {
		drt.endTime = time.Now().Format("2006-01-02 15:04:05")
	}

	return nil
}

func (drt *DagRuntime) newView(msg string) schema.DagView {
	deps := strings.Join(drt.component.GetDeps(), ",")

	paramters := map[string]string{}
	for name, value := range drt.component.GetParameters() {
		paramters[name] = fmt.Sprintf("%v", value)
	}

	var name string
	if drt.loopSeq == 0 {
		name = fmt.Sprintf("dag-%s-%s", drt.runID, drt.getComponent().GetName())
	} else {
		name = fmt.Sprintf("dag-%s-%s-%d", drt.runID, drt.getComponent().GetName(), drt.loopSeq)
	}

	// DAGID 在写库时生成，因此，此处并不会传递该参数, EntryPoints 在运行子节点时会同步至数据库，因此此处不包含这两个字段
	return schema.DagView{
		DagID:       drt.ID,
		Name:        name,
		DagName:     drt.getComponent().GetName(),
		Deps:        deps,
		Parameters:  paramters,
		Artifacts:   drt.component.GetArtifacts(),
		StartTime:   drt.startTime,
		EndTime:     drt.endTime,
		Status:      drt.status,
		Message:     msg,
		ParentDagID: drt.parentDagID,
		LoopSeq:     drt.loopSeq,
		PK:          drt.pk,
	}
}

// stopByCtx: 在监测到底 ctx 的信号后，开始终止逻辑
func (drt *DagRuntime) stopByCtx() {
	// 对于已经调度了节点，其本身也会监听 ctx 信号, 执行终止相关的逻辑，因此，此处只需要处理还未被调度的节点
	drt.cancellAllNotReadySubComponent("receive stop signall")
}

func (drt *DagRuntime) Stop() {
	select {
	case <-drt.ctx.Done():
		drt.logger.Infof("dag[%s] receive termination signal, begin to stop it", drt.name)
		if drt.done {
			return
		}
		defer drt.processSubComponentLock.Unlock()
		drt.processSubComponentLock.Lock()

		err := drt.updateStatus(StatusRuntimeTerminating)
		if err != nil {
			drt.logger.Errorf(err.Error())
		}
		drt.stopByCtx()

	case <-drt.failureOpitonsCtx.Done():
		drt.logger.Infof("dag[%s] receive failureOptions signal, begin to stop it", drt.name)
		if drt.done {
			return
		}
		defer drt.processSubComponentLock.Unlock()
		drt.processSubComponentLock.Lock()

		err := drt.updateStatus(StatusRuntimeTerminating)
		if err != nil {
			drt.logger.Errorf(err.Error())
		}

		// 此时 failureOptions的策略必然是 fail_fast
		drt.ProcessFailureOptionsWithFailFast()
	}
}
