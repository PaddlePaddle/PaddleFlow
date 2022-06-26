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
}

func generateDagID(runID string) string {
	return "dag-" + runID + "-" + GetRandID(DagIDRandCodeNum)
}

func NewDagRuntime(fullName string, dag *schema.WorkflowSourceDag, seq int, ctx context.Context, failureOpitonsCtx context.Context,
	eventChannel chan<- WorkflowEvent, config *runConfig, parentDagID string) *DagRuntime {
	nrt := NewBaseComponentRuntime(fullName, dag, seq, ctx, failureOpitonsCtx, eventChannel, config, parentDagID)

	res := NewReferenceSolver(config.WorkflowSource)

	ID := generateDagID(config.runID)

	drt := &DagRuntime{
		baseComponentRuntime:        nrt,
		referenceSolver:             res,
		ID:                          ID,
		subComponentRumtimes:        make(map[string][]componentRuntime),
		failureOptionsCtxAndCancels: make(map[string]CtxAndCancel),
	}

	drt.updateStatus(StatusRuntimeInit)

	ds := NewDependencySolver(drt)
	drt.DependencySolver = ds

	return drt
}

// NewDagRuntimeWithStatus: 在创建Runtime 的同时，指定runtime的状态
// 主要用于重启或者父节点调度子节点的失败时调用， 将相关信息通过evnet 的方式同步给其父节点， 并同步至数据库中
func newDagRuntimeWithStatus(fullName string, dag *schema.WorkflowSourceDag, seq int, ctx context.Context, failureOpitonsCtx context.Context,
	eventChannel chan<- WorkflowEvent, config *runConfig, parentDagID string, status RuntimeStatus, msg string) *DagRuntime {
	// 调用方在调用本函数前，需要保证 component 是一个 dag 类型的节点，所以此时NewDagRuntime 不应该会报错，故忽略该错误信息
	drt := NewDagRuntime(fullName, dag, seq, ctx, failureOpitonsCtx, eventChannel, config, parentDagID)

	drt.processStartAbnormalStatus(msg, status)
	return drt
}

func (drt *DagRuntime) generateSubComponentFullName(subComponentName string) string {
	// 当前dag 为 workflowSource.EntryPoints 时
	if drt.componentFullName == "" {
		return subComponentName
	}

	return strings.Join([]string{drt.componentFullName, subComponentName}, ".")
}

func (drt *DagRuntime) getReadyComponent() map[string]schema.Component {
	defer drt.processSubComponentLock.Unlock()
	drt.processSubComponentLock.Lock()

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
	return readyComponent
}

// resolveReference: 主要用于解析 reference 字段
func (drt *DagRuntime) resolveReference(subComponentName string, subComponent schema.Component) error {
	subFullName := drt.generateSubComponentFullName(subComponentName)

	newComponent, err := drt.referenceSolver.resolveComponentReference(subComponent)
	if err != nil {
		return err
	}

	drt.logger.Debugf("after resolve reference, component[%s] is:\n %v", subFullName, newComponent)
	drt.component.(*schema.WorkflowSourceDag).EntryPoints[subComponentName] = newComponent

	return nil
}

// createAndStartSubComponentRuntime: 创建并运行子节点 runtime
// 无需返回 error 原因是将通过 event 来进行同步
func (drt *DagRuntime) createAndStartSubComponentRuntime(subComponentName string, subComponent schema.Component,
	exceptSeq map[int]int) {
	subFullName := drt.generateSubComponentFullName(subComponentName)
	drt.logger.Debugf("begin to create runtime for component[%s]:\n%v", subFullName, subComponent)

	// 如果已经有子节点对应的 runtime, 则说明该节点已经被调度过了
	// PS: 理论上不会出现在这种情况，用于兜底
	_, ok := drt.subComponentRumtimes[subComponentName]
	if ok && len(exceptSeq) == 0 {
		drt.logger.Errorf("component [%s] has been scheduled", subComponentName)
		return
	}
	fmt.Println(subFullName, exceptSeq, 8888)
	// 1. 获取 loop_arguemnt, 确定需要创建多少次runtime
	isv := NewInnerSolver(subComponent, subFullName, drt.runConfig)
	err := isv.resolveLoopArugment()
	if err != nil {
		err := fmt.Errorf("cannot get the value of loop_arugment for component[%s]", subFullName)
		drt.logger.Errorln(err.Error())
		drt.processSubRuntimeError(err, subComponent, StatusRuntimeFailed)
		return
	}

	fmt.Println(subFullName, exceptSeq, 1111)
	var ll int
	lp := subComponent.GetLoopArgument()
	if lp != nil {
		v := reflect.ValueOf(lp)
		ll = v.Len()
		if ll == 0 {
			err := fmt.Errorf("component[%s] wouldn't be scheduled, because the lenth of it's loop_argument is 0",
				subFullName)
			drt.logger.Errorln(err.Error())
			drt.processSubRuntimeError(err, subComponent, StatusRuntimeSkipped)
			return
		}
	} else {
		ll = 1
	}

	// 同一个节点的多次运行，共享同一个 failureOptionsCtx
	ctxAndCc := drt.getfailureOptionsCtxAndCF(subComponentName)

	step, isStep := subComponent.(*schema.WorkflowSourceStep)
	fmt.Println(subFullName, exceptSeq, 222)
	dag, _ := subComponent.(*schema.WorkflowSourceDag)
	for index := 0; index < ll; index++ {
		if _, ok := exceptSeq[index]; ok {
			continue
		}
		fmt.Println(subFullName, exceptSeq, 333)

		var subRuntime componentRuntime
		if isStep {
			subRuntime = NewStepRuntime(subFullName, step, index, drt.ctx, ctxAndCc.ctx,
				drt.receiveEventChildren, drt.runConfig, drt.ID)
		} else {
			subRuntime = NewDagRuntime(subFullName, dag, index, drt.ctx, ctxAndCc.ctx,
				drt.receiveEventChildren, drt.runConfig, drt.ID)
		}
		drt.subComponentRumtimes[subComponentName] = append(drt.subComponentRumtimes[subComponentName], subRuntime)

		drt.logger.Infof("begion to run Component[%s]", subRuntime.getName())
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
	drt.logger.Debugf("begin to run dagp[%s]", drt.name)

	drt.updateStatus(StatusRuntimeRunning)
	drt.startTime = time.Now().Format("2006-01-02 15:04:05")

	// TODO: 此时是否需要同步至数据库？

	// 1、 更新系统变量
	drt.setSysParams()

	// 2、替换 condition，loop_argument 中的模板，将其替换成具体真实值
	conditon, err := drt.CalculateCondition()
	if err != nil {
		errMsg := fmt.Sprintf("caculate the condition field for component[%s] faild:\n%s",
			drt.componentFullName, err.Error())
		drt.logger.Errorln(errMsg)
		drt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
		return
	}

	if conditon {
		skipMsg := fmt.Sprintf("the result of condition for Component [%s] is false, skip running", drt.componentFullName)
		drt.logger.Infoln(skipMsg)
		drt.processStartAbnormalStatus(skipMsg, StatusRuntimeSkipped)
		return
	}

	if drt.isDisabled() {
		skipMsg := fmt.Sprintf("Component [%s] is disabled, skip running", drt.componentFullName)
		drt.logger.Infoln(skipMsg)
		drt.processStartAbnormalStatus(skipMsg, StatusRuntimeSkipped)
		return
	}

	// 监听子节点已经父节点传递过来的事件或者信号
	go drt.Listen()

	// 开始调度子节点
	drt.scheduleSubComponent(true)
}

// scheduleSubComponent: 调度子节点运行
// 不返回error，直接通过 event 向上冒泡
func (drt *DagRuntime) scheduleSubComponent(mustSchedule bool) {
	// 1、获取可以进行调度的节点
	readyComponent := drt.getReadyComponent()
	fmt.Println("in restart", readyComponent)

	// 如果 mustSchedule 为True, 说明此时必须要调度某些子节点运行，否则便是有bug
	if len(readyComponent) == 0 && mustSchedule {
		err := fmt.Errorf("cannot find any ready subComponent for Component[%s] while mustSchedule is True", drt.componentFullName)
		drt.logger.Errorln(err.Error())

		drt.updateStatus(StatusRuntimeFailed)
		dagView := drt.newView(err.Error())
		drt.syncToApiServerAndParent(WfEventDagUpdate, dagView, err.Error())
		return
	}

	defer drt.processSubComponentLock.Unlock()
	drt.processSubComponentLock.Lock()
	for subComponentName, subComponent := range readyComponent {
		drt.logger.Infof("begin to schedule subcompoent[%s] of component[%s]", subComponentName, drt.name)
		// 如果此时收到了终止信号，则无需调度子节点
		if drt.ctx.Err() != nil || drt.failureOpitonsCtx.Err() != nil {
			drt.logger.Infof("componentRuntime[%s] receives temination signal, so it's subComponent wouldn't be scheduled anymore",
				drt.name)
			return
		}

		// 如果此时的状态为 terminating 或者处于终态， 也不应该在调度子节点
		if drt.isTerminating() || drt.isDone() {
			drt.logger.Infof("the status of componentRuntime[%s] is [%s], so it's subComponent wouldn't be scheduled anymore",
				drt.name, drt.status)
			return
		}

		// 2. Component 替换： 主要是处理 reference 字段
		err := drt.resolveReference(subComponentName, subComponent)
		if err != nil {
			drt.logger.Errorln(err.Error())

			// 创建占位用 runtime
			drt.processSubRuntimeError(err, subComponent, StatusRuntimeFailed)
			continue
		}

		// 3. 替换子节点 parameter，artifact 字段中的模板
		err = drt.DependencySolver.ResolveBeforeRun(subComponentName)
		if err != nil {
			drt.logger.Errorln(err.Error())
			drt.processSubRuntimeError(err, subComponent, StatusRuntimeFailed)
			continue
		}

		// 4. 创建 runtime 并运行 runtime
		drt.createAndStartSubComponentRuntime(subComponentName, subComponent, map[int]int{})
	}
}

// 监听由子节点发送过来的信号
func (drt *DagRuntime) Listen() {
	// drt 的状态处于终态时，一定要return
	for {
		select {
		case event := <-drt.receiveEventChildren:
			if drt.done {
				return
			}
			if err := drt.processEventFromSubComponent(event); err != nil {
				// how to read event?
				drt.logger.Debugf("process event failed %s", err.Error())
			}

		case <-drt.ctx.Done():
			if drt.done {
				return
			}
			drt.updateStatus(StatusRuntimeTerminating)
			drt.stopByCtx()
		case <-drt.failureOpitonsCtx.Done():
			if drt.done {
				return
			}
			drt.updateStatus(StatusRuntimeTerminating)

			// 此时 failureOptions的策略必然是 fail_fast
			drt.ProcessFailureOptionsWithFailFast()
		}

		if drt.isDone() {
			return
		}
	}
}

// 重新执行
// TODO
func (drt *DagRuntime) Restart(dagView schema.DagView) {
	drt.logger.Infof("restart dag[%s]", drt.name)

	need, err := drt.needRestart(&dagView)
	if err != nil {
		msg := fmt.Sprintf("cannot decide to whether to restart step[%s]: %s", drt.name, err.Error())
		drt.logger.Errorf(msg)

		drt.processStartAbnormalStatus(msg, StatusRuntimeFailed)
		return
	}
	if !need {
		msg := fmt.Sprintf("dag [%s] is already in status[%s], no restart required", drt.name, dagView.Status)
		drt.processStartAbnormalStatus(msg, StatusRuntimeSucceeded)
		return
	}

	drt.setSysParams()
	// 2、 对于已经有处于 succeeded 、 running、 skipped 状态的runtime的节点，说明其一定是处于可调度的状态，
	// 此时需要判断其对应的节点是否为 循环结构，是的话，可能有某几次运行失败，或者还没有来的及发起，此时我们需要补齐缺失的运行
	hasSchedule, err := drt.scheduleSubComponentAccordingView(dagView)
	if err != nil {
		err = fmt.Errorf("restart failed: %s", err.Error())
		drt.logger.Error(err.Error())

		drt.processStartAbnormalStatus(err.Error(), StatusRuntimeFailed)
		return
	}

	for _, cps := range drt.subComponentRumtimes {
		for _, cp := range cps {
			fmt.Println("after scheduleSubComponentAccordingView", cp.getName(), cp.getStatus())
		}
	}

	// 3、处理完所有的view 后 才开始 监听信号, 主要是为了在还没有处理完 view 中新，便接受到了事件， 导致在 view 中存在的节点再次被调度
	go drt.Listen()

	// 4、根据节点依赖关系，来开始调度此时可运行的节点。
	// 这里做一次调度的原因是，避免 3 中没有发起任何任务，导致永远监听不到信息，导致任务 hang 住的情况出现
	var mustSchedule bool
	if hasSchedule {
		mustSchedule = false
	} else {
		mustSchedule = true
	}

	drt.scheduleSubComponent(mustSchedule)
	return
}

func (drt *DagRuntime) needRestart(dagView *schema.DagView) (bool, error) {
	// 避免在重试过程中的收到 stop 信号，出现数据，状态不一致的情况
	defer drt.processSubComponentLock.Unlock()
	drt.processSubComponentLock.Lock()

	if drt.status != StatusRuntimeInit {
		err := fmt.Errorf("inner error: cannot restart dag[%s], because it's already in status[%s], "+
			"maybe multi gorutine process this dag", drt.name, drt.status)
		return false, err
	}

	if dagView.Status == StatusRuntimeSucceeded || dagView.Status == StatusRuntimeSkipped {
		return false, nil
	}

	if len(dagView.EntryPoints) != len(drt.getworkflowSouceDag().EntryPoints) {
		drt.logger.Debugf("dag[%s] need restart because len(dagView.EntryPoints)"+
			" != len(drt.getworkflowSouceDag().EntryPoints)", drt.name)
		return true, nil
	}

	sorted, err := TopologicalSort(drt.getworkflowSouceDag().EntryPoints)
	if err != nil {
		err = fmt.Errorf("get topo sort failed: %s", err.Error())
		return false, err
	}

	for _, name := range sorted {
		views, ok := dagView.EntryPoints[name]
		if !ok {
			// TODO: 或者选择报错？ 理论上不会出现这种情况，
			drt.logger.Errorf("cannot find view for subcomponent[%s] of dag[%s]", name, drt.name)
			return true, nil
		}

		// 1、如果有view 的状态不是的 StatusRuntimeSucceeded 和 StatusRuntimeSkipped， 则需要重启
		for _, view := range views {
			status := view.GetStatus()
			if status != StatusRuntimeSucceeded && status != StatusRuntimeSkipped {
				drt.logger.Debugf("dag[%s.%d] need restart because status of view[%s] is %s",
					view.GetComponentName(), view.GetSeq(), view.GetComponentName(), status)
				return true, nil
			}
		}

		component := drt.getworkflowSouceDag().EntryPoints[name]
		// 替换 reference 字段
		err := drt.resolveReference(name, component)
		if err != nil {
			drt.logger.Errorln(err.Error())
			drt.processSubRuntimeError(err, component, StatusRuntimeFailed)
			return false, err
		}

		fmt.Println("DependencySolver.ResolveBeforeRun", name)

		// 替换 parameter 与 artifact 中的模板
		err = drt.DependencySolver.ResolveBeforeRun(name)
		if err != nil {
			drt.logger.Errorln("ResolveBeforeRun failed:", err.Error())
			drt.processSubRuntimeError(err, component, StatusRuntimeFailed)
			return false, err
		}

		// 解析loop_argument
		subFullName := drt.generateSubComponentFullName(name)
		isv := NewInnerSolver(component, subFullName, drt.runConfig)
		err = isv.resolveLoopArugment()
		if err != nil {
			err := fmt.Errorf("cannot get the value of loop_arugment for component[%s]", subFullName)
			drt.logger.Errorln(err.Error())
			drt.processSubRuntimeError(err, component, StatusRuntimeFailed)
			return false, err
		}

		lp := component.GetLoopArgument()
		if lp != nil {
			v := reflect.ValueOf(lp)
			if len(views) < v.Len() {
				drt.logger.Debugf("dag[%s] need restart because the num of views[%d] is less than loop_argument[%d]",
					drt.name, len(views), v.Len())
				return true, nil
			}
		}

		for _, view := range views {
			// 这里需要创建 runtime 的原因是在解析上下游参数依赖的时候需要用到
			runtime := drt.CreateSubRuntimeAccordingView(view, name)
			drt.subComponentRumtimes[name] = append(drt.subComponentRumtimes[name], runtime)
			drt.logger.Debugf("recreated runtime for component[%s] with status[%s]",
				runtime.getName(), runtime.getStatus())
		}
	}

	return false, nil
}

func (drt *DagRuntime) updateSubCompoentRuntimeByView(dagView schema.DagView) {
	for name, views := range dagView.EntryPoints {
		for _, view := range views {
			status := view.GetStatus()
			if status != StatusRuntimeRunning || status != StatusRuntimeSucceeded || status != StatusRuntimeSkipped {
				continue
			} else {
				subRuntime := drt.CreateSubRuntimeAccordingView(view, name)
				drt.subComponentRumtimes[name] = append(drt.subComponentRumtimes[name], subRuntime)
			}
		}
	}
}

func (drt *DagRuntime) CreateSubRuntimeAccordingView(view schema.ComponentView, name string) componentRuntime {
	JobView, ok := view.(schema.JobView)
	if ok {
		return drt.creatStepRuntimeAccordingView(JobView, name)
	}

	return drt.createDagRuntimeAccordingView(view.(schema.DagView), name)
}

func (drt *DagRuntime) creatStepRuntimeAccordingView(view schema.JobView, name string) componentRuntime {
	fullName := drt.generateSubComponentFullName(name)

	ctxAndcc := drt.getfailureOptionsCtxAndCF(name)

	srt := NewStepRuntime(fullName, drt.getworkflowSouceDag().EntryPoints[name].(*schema.WorkflowSourceStep),
		view.Seq, drt.ctx, ctxAndcc.ctx, drt.receiveEventChildren, drt.runConfig, drt.ID)

	return srt
}

func (drt *DagRuntime) createDagRuntimeAccordingView(view schema.DagView, name string) componentRuntime {
	fullName := drt.generateSubComponentFullName(name)

	ctxAndcc := drt.getfailureOptionsCtxAndCF(name)

	sDrt := NewDagRuntime(fullName, drt.getworkflowSouceDag().EntryPoints[name].(*schema.WorkflowSourceDag),
		view.Seq, drt.ctx, ctxAndcc.ctx, drt.receiveEventChildren, drt.runConfig, drt.ID)

	sDrt.updateStatus(view.GetStatus())
	return sDrt
}

func (drt *DagRuntime) getViewAccordingSeq(views []schema.ComponentView, seq int) (view schema.ComponentView, err error) {
	for _, v := range views {
		if v.GetSeq() == seq {
			return v, nil
		}
	}

	err = fmt.Errorf("cannot get view with seq[%d]", seq)
	return schema.DagView{}, err
}

func (drt *DagRuntime) scheduleSubComponentAccordingView(dagView schema.DagView) (hasSchedule bool, err error) {
	hasSchedule = false
	err = nil

	sorted, err := TopologicalSort(drt.getworkflowSouceDag().EntryPoints)
	if err != nil {
		err = fmt.Errorf("get topo sort failed: %s", err.Error())
		return
	}
	drt.logger.Debugf("toposort in dag[%s] is %v", drt.name, sorted)

	defer drt.processSubComponentLock.Unlock()
	drt.processSubComponentLock.Lock()

	if drt.status != StatusRuntimeInit {
		err = fmt.Errorf("inner error: cannot restart dag[%s], because it's already in status[%s], "+
			"maybe multi gorutine process this dag", drt.name, drt.status)
		return
	}

	drt.updateStatus(StatusRuntimeRunning)

	// 这里需要重置 subComponentRumtimes 原因是在 needRestarted 函数中，并没有保存完整的view信息
	drt.subComponentRumtimes = map[string][]componentRuntime{}

	for _, name := range sorted {
		// 如果此时收到了终止信号，则无需调度子节点
		if drt.ctx.Err() != nil || drt.failureOpitonsCtx.Err() != nil {
			drt.logger.Infof("componentRuntime[%s] receives temination signal, so it's subComponent wouldn't be scheduled anymore",
				drt.name)
			return
		}

		views, ok := dagView.EntryPoints[name]
		fmt.Println("views:", name, ok)
		if !ok {
			continue
		}

		drt.logger.Debugf("begin to restart subcomponent[%s] for dag[%s]", name, drt.name)

		// 1、判断当前节点的处理方式： 1）状态恢复， 2）重新运行
		needRecover := false
		for _, view := range views {
			status := view.GetStatus()
			if status != StatusRuntimeRunning || status != StatusRuntimeSucceeded || status != StatusRuntimeSkipped {
				needRecover = true
			}
		}

		if !needRecover {
			// 对于 重新运行的节点，则会在 scheduleSubComponent() 函数中本调度，此处不对其进行处理
			// 这里没有break 的原因：考虑多个分支的情况
			continue
		}

		fmt.Println("need recover:", name)
		component := drt.getworkflowSouceDag().EntryPoints[name]

		// exceptSeq 的value 无实义，仿set
		exceptSeq := map[int]int{}
		_, isStep := component.(*schema.WorkflowSourceStep)
		for _, view := range views {
			status := view.GetStatus()
			if status != StatusRuntimeRunning && status != StatusRuntimeSucceeded && status != StatusRuntimeSkipped {
				continue
			}

			exceptSeq[view.GetSeq()] = 1

			runtime := drt.CreateSubRuntimeAccordingView(view, name)
			drt.subComponentRumtimes[name] = append(drt.subComponentRumtimes[name], runtime)
			drt.logger.Debugf("recreated runtime for component[%s] with status[%s]",
				runtime.getName(), runtime.getStatus())

			if status == StatusRuntimeRunning {
				hasSchedule = true
			} else {
				drt.logger.Debugf("component[%s] don't need restart, because it's already in status[%s]",
					runtime.getName(), runtime.getStatus())
			}

			if isStep {
				go runtime.(*StepRuntime).Restart(view.(schema.JobView))
			} else {
				go runtime.(*DagRuntime).Restart(view.(schema.DagView))
			}
		}

		lp := component.GetLoopArgument()
		if lp != nil {
			fmt.Println(name, 1234455, lp, len(exceptSeq))
			v := reflect.ValueOf(lp)
			if len(exceptSeq) < v.Len() {
				fmt.Println(name, "call createAndStartSubComponentRuntime")
				drt.createAndStartSubComponentRuntime(name, component, exceptSeq)
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
		err := fmt.Errorf("cannot get the value of parameter[%s] from component[%s], because there is no runtime for that component",
			paramName, drt.componentFullName+"."+componentName)
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
		err := fmt.Errorf("cannot get the value of parameter[%s] from component[%s], because there is no component named [%s] in dag[%s]",
			artName, drt.componentFullName+"."+componentName, componentName, drt.componentFullName)
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
// TODO: 进一步完善具体的逻辑
// 对于异常处理的情况
// 1. 提交失败，job id\status 都为空，视为 job 失败，更新 run message 字段
// 2. watch 失败，状态不更新，更新 run message 字段；等 job 恢复服务之后，job watch 恢复，run 自动恢复调度
// 3. stop 失败，状态不更新，run message 字段；需要用户根据提示再次调用 stop
// 4. 如果有 job 的状态异常，将会走 FailureOptions 的处理逻辑
func (drt *DagRuntime) processEventFromSubComponent(event WorkflowEvent) error {
	if drt.isDone() {
		drt.logger.Debugf("workflow has completed. skip event")
		return nil
	}
	drt.logger.Infof("process event: [%+v]", event)

	// 判断事件类型是否为 failureOptionstriggered 类型，是的话，执行 processFailureOptions
	if event.isFailureOptionsTriggered() {
		drt.ProcessFailureOptions(event, false)
	} else {
		// 判断节点处于异常状态： Failed 和 Terminated（但是 dag 状态不是terminated 也不是terminating），是的话，则开始执行 FailureOptions 相关的逻辑
		status, ok := event.Extra[common.WfEventKeyStatus]
		if ok {
			subRuntimeStatus := status.(schema.JobStatus)
			isFailed := subRuntimeStatus == StatusRuntimeFailed
			isUnexpectedTerminated := subRuntimeStatus == StatusRuntimeTerminated && drt.status != StatusRuntimeTerminating

			if isFailed || isUnexpectedTerminated {
				drt.ProcessFailureOptions(event, true)
			}
		}
	}

	StatusMsg := drt.updateStatusAccordingSubComponentRuntimeStatus()
	fmt.Println(1111, StatusMsg)
	view := drt.newView(StatusMsg)
	fmt.Println("22222222222222")
	drt.syncToApiServerAndParent(WfEventDagUpdate, view, StatusMsg)

	// 如果 dagRuntime 未处于终态，则需要判断是否有新的子节点可以运行

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
	// 失败节点的所有下游节点都将会置为failed, 此时其所有的下游节点都是没有开始执行的，
	defer drt.processSubComponentLock.Unlock()
	drt.processSubComponentLock.Lock()

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
			reason := fmt.Sprintf("begin to process FailureOptions, there are some component run failed")
			drt.CancellNotReadyComponent(cancelComponent, reason)
		}
	}
}

func (drt *DagRuntime) ProcessFailureOptionsWithFailFast() {
	defer drt.processSubComponentLock.Unlock()
	drt.processSubComponentLock.Lock()

	for name, component := range drt.getworkflowSouceDag().EntryPoints {
		_, ok := drt.subComponentRumtimes[name]
		if ok {
			drt.getfailureOptionsCtxAndCF(name).cancel()
		}

		drt.CancellNotReadyComponent(component, "receive failure options signal")
	}
}

func (drt *DagRuntime) ProcessFailureOptions(event WorkflowEvent, needSync bool) {
	drt.logger.Infof("begin to process failure options. trigger event is: %v", event)
	name, ok := event.Extra[common.WfEventKeyComponentName]

	if !ok {
		errMsg := fmt.Sprintf("cannot get the Component info of event[%v] for dagRuntime[%s], begin to cancell all not ready step",
			drt.runID, event)
		drt.logger.Errorf(errMsg)

		// 理论上不会出现这种情况，用户兜底
		// 防止下游节点无法调度，导致任务被 hang 住，将所有还没有调度的节点置为 cancelled 状态
		// 或者直接终止 run？ 又或者终止当前的 dagRuntime？
		drt.cancellAllNotReadySubComponent(errMsg)

	}
	componentName := name.(string)
	component := drt.subComponentRumtimes[componentName][0].getComponent()

	// 通过时间通知其父节点处理开始处理 failureOptions
	if needSync {
		drt.syncToApiServerAndParent(WfEventFailureOptionsTriggered, schema.DagView{},
			fmt.Sprintf("failure options triggered by event: %v", event))
	}
	// 策略的合法性由 workflow 保证
	if drt.FailureOptions.Strategy == schema.FailureStrategyContinue {
		drt.ProcessFailureOptionsWithContinue(component)
	} else {
		drt.ProcessFailureOptionsWithFailFast()
	}
}

func (drt *DagRuntime) cancellAllNotReadySubComponent(errMsg string) {
	defer drt.processSubComponentLock.Unlock()
	drt.processSubComponentLock.Lock()

	for subName, subComponent := range drt.getworkflowSouceDag().EntryPoints {
		_, ok := drt.subComponentRumtimes[subName]
		if !ok {
			drt.CancellNotReadyComponent(subComponent, errMsg)
		}
	}
}

func (drt *DagRuntime) CancellNotReadyComponent(subComponent schema.Component, reason string) {
	cancelComponentFullName := drt.generateSubComponentFullName(subComponent.GetName())
	drt.logger.Infof("begin to cancel component[%s]: %s", cancelComponentFullName, reason)

	err := fmt.Errorf(reason)
	drt.processSubRuntimeError(err, subComponent, StatusRuntimeCancelled)
}

func (drt *DagRuntime) processStartAbnormalStatus(msg string, status RuntimeStatus) {
	drt.updateStatus(status)
	dagView := drt.newView(msg)
	drt.syncToApiServerAndParent(WfEventDagUpdate, dagView, msg)
}

// processSubRuntimeError： 处理调度子节点失败的情况，通过调用processEventFromSubComponent()函数来进行同步
func (drt *DagRuntime) processSubRuntimeError(err error, cp schema.Component, status RuntimeStatus) {
	componentName := cp.GetName()
	subFullName := drt.generateSubComponentFullName(componentName)
	step, ok := cp.(*schema.WorkflowSourceStep)

	ctxAndCc := drt.getfailureOptionsCtxAndCF(componentName)

	var crt componentRuntime
	// 使用协程 是为了避免主线程
	if ok {
		crt = newStepRuntimeWithStatus(subFullName, step, 0, drt.ctx, ctxAndCc.ctx, drt.receiveEventChildren,
			drt.runConfig, drt.ID, status, err.Error())
	} else {
		dag := cp.(*schema.WorkflowSourceDag)
		crt = newDagRuntimeWithStatus(subFullName, dag, 0, drt.ctx, ctxAndCc.ctx, drt.receiveEventChildren,
			drt.runConfig, drt.ID, status, err.Error())
	}
	drt.subComponentRumtimes[componentName] = append(drt.subComponentRumtimes[componentName], crt)
}

// updateStatusAccordingSubComponentRuntimeStatus: 根据子节点的状态来更新
func (drt *DagRuntime) updateStatusAccordingSubComponentRuntimeStatus() string {
	// 1. 如果有子节点还没有调度，且节点本身的状态不为 Terminating， 则状态必定为running
	if len(drt.subComponentRumtimes) != len(drt.getworkflowSouceDag().EntryPoints) {
		if !drt.isTerminating() {
			drt.updateStatus(StatusRuntimeRunning)
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
			// 2.2. 如果loop_argument 不能转换成 splice, 则该子节点的loop_argument 有问题，且必然已经被置为 failed 状态
			t := reflect.TypeOf(loop_argument)
			if t.Kind() != reflect.Slice {
				fmt.Println("failed,,1", t.Kind(), cps[0].getName())
				faieldComponentNames = append(faieldComponentNames, cps[0].getName())
			} else {
				v := reflect.ValueOf(loop_argument)
				if len(cps) != v.Len() && v.Len() != 0 {
					// v.Len 为 0 时， 会有一个占位用户的 cp
					if !drt.isTerminating() {
						drt.updateStatus(StatusRuntimeRunning)
					}
					return ""
				}
			}
		}

		for index := range cps {
			if cps[index].isFailed() {
				fmt.Println("failed 2", faieldComponentNames)
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
					drt.updateStatus(StatusRuntimeRunning)
				}
				return ""
			}
		}
	}
	var msg string
	if len(faieldComponentNames) != 0 {
		drt.updateStatus(StatusRuntimeFailed)
		msg = fmt.Sprintf("update Component[%s]'s status to [%s] due to subcomponents[%s] faield",
			drt.componentFullName, StatusRuntimeFailed, strings.Join(faieldComponentNames, ","))
	} else if len(terminatedComponentNames) != 0 {
		if drt.status != StatusRuntimeTerminating {
			drt.updateStatus(StatusRuntimeFailed)
			msg = fmt.Sprintf("update Component[%s]'s status to [%s] due to subcomponents[%s] abnormally terminated",
				drt.componentFullName, StatusRuntimeFailed, strings.Join(terminatedComponentNames, ","))
		} else {
			drt.updateStatus(StatusRuntimeTerminated)
			msg = fmt.Sprintf("update Component[%s]'s status to [%s] due to subcomponents[%s] terminated",
				drt.componentFullName, StatusRuntimeFailed, strings.Join(terminatedComponentNames, ","))
		}
	} else if len(cancelledComponentNames) != 0 {
		// 如果节点的状态是 cancelled，只有两种情况：
		// 1、有节点运行失败，触发了 FailureOptions 机制，这种情况在上面已经处理
		// 2、收到终止信号
		drt.updateStatus(StatusRuntimeTerminated)
		msg = fmt.Sprintf("update Component[%s]'s status to [%s] due to subcomponents[%s] cancelled",
			drt.componentFullName, StatusRuntimeFailed, strings.Join(terminatedComponentNames, ","))
	} else {
		// 回填本节点的输出artifact
		drt.ResolveAfterDone()
		drt.updateStatus(StatusRuntimeSucceeded)
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

	// DAGID 在写库时生成，因此，此处并不会传递该参数, EntryPoints 在运行子节点时会同步至数据库，因此此处不包含这两个字段
	return schema.DagView{
		DagName:     drt.getComponent().GetName(),
		Deps:        deps,
		Parameters:  paramters,
		Artifacts:   drt.component.GetArtifacts(),
		StartTime:   drt.startTime,
		EndTime:     drt.endTime,
		Status:      drt.status,
		Message:     msg,
		ParentDagID: drt.parentDagID,
		Seq:         drt.seq,
		PK:          drt.pk,
	}
}

// stopByCtx: 在监测到底 ctx 的信号后，开始终止逻辑
func (drt *DagRuntime) stopByCtx() {
	// 对于已经调度了节点，其本身也会监听 ctx 信号, 执行终止相关的逻辑，因此，此处只需要处理还未被调度的节点
	drt.cancellAllNotReadySubComponent("receive stop signall")
}
