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
	"fmt"
	"regexp"
	"strconv"

	. "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"

	"github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

// ----------------------------------------------------------------------------
//  BaseWorkflow 工作流基础结构
// ----------------------------------------------------------------------------

type BaseWorkflow struct {
	Name         string                                `json:"name,omitempty"`
	RunID        string                                `json:"runId,omitempty"`
	Desc         string                                `json:"desc,omitempty"`
	Entry        string                                `json:"entry,omitempty"`
	Params       map[string]interface{}                `json:"params,omitempty"`
	Extra        map[string]string                     `json:"extra,omitempty"` // 可以存放一些ID，fsId，userId等
	Source       schema.WorkflowSource                 `json:"-"`               // Yaml string
	runtimeSteps map[string]*schema.WorkflowSourceStep `json:"-"`
	postProcess  map[string]*schema.WorkflowSourceStep `json:"-"`
}

func NewBaseWorkflow(wfSource schema.WorkflowSource, runID, entry string, params map[string]interface{}, extra map[string]string) BaseWorkflow {
	// Todo: 设置默认值
	bwf := BaseWorkflow{
		RunID:  runID,
		Name:   "default_name",
		Desc:   "default_desc",
		Params: params,
		Extra:  extra,
		Source: wfSource,
		Entry:  entry,
	}

	bwf.runtimeSteps = bwf.getComponents()
	return bwf
}

func (bwf *BaseWorkflow) log() *logrus.Entry {
	return logger.LoggerForRun(bwf.RunID)
}

func (bwf *BaseWorkflow) checkDeps() error {
	for name, step := range bwf.Source.EntryPoints {
		for _, dep := range step.GetDeps() {
			if _, ok := bwf.Source.EntryPoints[dep]; !ok {
				return fmt.Errorf("step [%s] has an wrong dep [%s]", name, dep)
			}
		}
	}
	return nil
}

func (bwf *BaseWorkflow) getComponents() map[string]*schema.WorkflowSourceStep {
	/*
		根据传入的entry，获取此次run需要运行的step集合
		注意：此处返回的map中，每个schema.WorkflowSourceStep元素都是以指针形式返回（与BaseWorkflow中Source参数中的step指向相同的类对象），后续对与元素内容的修改，会直接同步到BaseWorkflow中Source参数
	*/
	entry := bwf.Entry
	if entry == "" {
		return bwf.Source.EntryPoints
	}

	runSteps := map[string]*schema.WorkflowSourceStep{}
	bwf.recursiveGetRunSteps(entry, runSteps)
	return runSteps
}

func (bwf *BaseWorkflow) recursiveGetRunSteps(entry string, steps map[string]*schema.WorkflowSourceStep) {
	// duplicated in result map
	if _, ok := steps[entry]; ok {
		return
	}

	if step, ok := bwf.Source.EntryPoints[entry]; ok {
		steps[entry] = step
		for _, dep := range step.GetDeps() {
			bwf.recursiveGetRunSteps(dep, steps)
		}
	}
}

// validate BaseWorkflow 校验合法性
func (bwf *BaseWorkflow) validate() error {

	// 2. 检验extra，保证FsID和FsName，要么都传，要么都不传
	if err := bwf.checkExtra(); err != nil {
		bwf.log().Errorf("check extra failed. err:%s", err.Error())
		return err
	}

	// 3. RunYaml 中pipeline name, 各step name是否合法; 可运行节点(runSteps)的结构是否有环等；
	if err := bwf.checkRunYaml(); err != nil {
		bwf.log().Errorf("check run yaml failed. err:%s", err.Error())
		return err
	}

	// 4. 校验PostProcess的设定是否合法
	if err := bwf.checkPostProcess(); err != nil {
		bwf.log().Errorf("check post process err: %s", err.Error())
		return err
	}

	// 5. 校验通过接口传入的Parameter参数(参数是否存在，以及参数值是否合法),
	if err := bwf.checkParams(); err != nil {
		bwf.log().Errorf("check run param err:%s", err.Error())
		return err
	}

	// 6. runtimeSteps和postProcess 中的 parameter artifact env command 是否合法
	if err := bwf.checkSteps(); err != nil {
		bwf.log().Errorf("check steps err:%s", err.Error())
		return err
	}

	// 7. cache配置是否合法
	if err := bwf.checkCache(); err != nil {
		bwf.log().Errorf("check cache failed. err:%s", err.Error())
		return err
	}

	// 8. 检查FailureOption
	if err := bwf.checkFailureOption(); err != nil {
		bwf.log().Errorf("check failue_option failed. err: %s", err.Error())
		return err
	}
	return nil
}

func (bwf *BaseWorkflow) checkFailureOption() error {
	switch bwf.Source.FailureOptions.Strategy {
	case schema.FailureStrategyFailFast:
		return nil
	case schema.FailureStrategyContinue:
		return nil
	default:
		return fmt.Errorf("failure strategy should be [fail_fast] or [continue], setted by [%s]", bwf.Source.FailureOptions.Strategy)
	}
}

// 校验extra字典
// 保证FsID和FsName，要么同时传（pipeline run需要挂载fs），要么都不传（pipeline run不需要挂载fs）
func (bwf *BaseWorkflow) checkExtra() error {
	FsId := bwf.Extra[WfExtraInfoKeyFsID]
	FsName := bwf.Extra[WfExtraInfoKeyFsName]
	if (FsId == "" && FsName != "") || (FsId != "" && FsName == "") {
		return fmt.Errorf("check extra failed: FsID[%s] and FsName[%s] can only both be empty or unempty", FsId, FsName)
	}

	return nil
}

func (bwf *BaseWorkflow) checkRunYaml() error {
	variableChecker := VariableChecker{}

	if err := variableChecker.CheckVarName(bwf.Source.Name); err != nil {
		return fmt.Errorf("format of pipeline name[%s] in run[%s] invalid", bwf.Source.Name, bwf.RunID)
	}

	// 检查名字命名规范，不涉及重名检查
	if err := bwf.checkAllComponentName(bwf.Source.EntryPoints.EntryPoints); err != nil {
		return err
	}
	postComponent := map[string]schema.Component{}
	for name, step := range bwf.Source.PostProcess {
		postComponent[name] = step
	}
	if err := bwf.checkAllComponentName(postComponent); err != nil {
		return err
	}

	if err := bwf.checkTopoSort(bwf.Source.EntryPoints.EntryPoints); err != nil {
		return err
	}

	return nil
}

func (bwf *BaseWorkflow) checkAllComponentName(components map[string]schema.Component) error {
	variableChecker := VariableChecker{}
	for name, component := range components {
		err := variableChecker.CheckStepName(name)
		if err != nil {
			return fmt.Errorf("format of stepName[%s] in run[%s] invalid", name, bwf.RunID)
		}
		if dag, ok := component.(*schema.WorkflowSourceDag); ok {
			if err := bwf.checkAllComponentName(dag.EntryPoints); err != nil {
				return err
			}
		}
	}
	return nil
}

func (bwf *BaseWorkflow) checkTopoSort(components map[string]schema.Component) error {
	if _, err := bwf.topologicalSort(components); err != nil {
		return err
	}
	for _, component := range components {
		if dag, ok := component.(*schema.WorkflowSourceDag); ok {
			if err := bwf.checkTopoSort(dag.EntryPoints); err != nil {
				return err
			}
		}
	}
	return nil
}

// topologicalSort step 拓扑排序
// todo: use map as return value type?
func (bwf *BaseWorkflow) topologicalSort(components map[string]schema.Component) ([]string, error) {
	// unsorted: unsorted graph
	// if we have dag:
	//     1 -> 2 -> 3
	// then unsorted as follow will be get:
	//     1 -> [2]
	//     2 -> [3]
	sortedSteps := make([]string, 0)
	unsorted := map[string][]string{}
	for name, step := range components {
		depsList := step.GetDeps()

		if len(depsList) == 0 {
			unsorted[name] = nil
			continue
		}
		for _, dep := range depsList {
			if _, ok := unsorted[name]; ok {
				unsorted[name] = append(unsorted[name], dep)
			} else {
				unsorted[name] = []string{dep}
			}
		}
	}

	// 通过判断入度，每一轮寻找入度为0（没有parent节点）的节点，从unsorted中移除，并添加到sortedSteps中
	// 如果unsorted长度被减少到0，说明无环。如果有一轮没有出现入度为0的节点，说明每个节点都有父节点，即有环。
	for len(unsorted) != 0 {
		acyclic := false
		for name, parents := range unsorted {
			parentExist := false
			for _, parent := range parents {
				if _, ok := unsorted[parent]; ok {
					parentExist = true
					break
				}
			}
			// if all the source nodes of this node has been removed,
			// consider it as sorted and remove this node from the unsorted graph
			if !parentExist {
				acyclic = true
				delete(unsorted, name)
				sortedSteps = append(sortedSteps, name)
			}
		}
		if !acyclic {
			// we have go through all the nodes and weren't able to resolve any of them
			// there must be cyclic edges
			return nil, fmt.Errorf("workflow is not acyclic")
		}
	}
	return sortedSteps, nil
}

func (bwf *BaseWorkflow) checkCache() error {
	/*
		校验yaml中cache各相关字段
	*/

	// 校验MaxExpiredTime，必须能够转换成数字。如果没传，默认为-1
	if bwf.Source.Cache.MaxExpiredTime == "" {
		bwf.Source.Cache.MaxExpiredTime = CacheExpiredTimeNever
	}
	_, err := strconv.Atoi(bwf.Source.Cache.MaxExpiredTime)
	if err != nil {
		return fmt.Errorf("MaxExpiredTime[%s] of cache not correct", bwf.Source.Cache.MaxExpiredTime)
	}

	// 校验FsScope。计算目录通过逗号分隔。
	// 此处不校验path格式是否valid，以及path是否存在（如果不valid或者不存在，在计算cache，查询FsScope更新时间时，会获取失败）
	if bwf.Extra[WfExtraInfoKeyFsID] == "" && bwf.Source.Cache.FsScope != "" {
		return fmt.Errorf("fs_scope of global cache should be empty if Fs is not used!")
	}

	for stepName, wfsStep := range bwf.Source.EntryPoints {
		if wfsStep.Cache.MaxExpiredTime == "" {
			wfsStep.Cache.MaxExpiredTime = CacheExpiredTimeNever
		}

		_, err := strconv.Atoi(wfsStep.Cache.MaxExpiredTime)
		if err != nil {
			return fmt.Errorf("MaxExpiredTime[%s] of cache in step[%s] not correct", wfsStep.Cache.MaxExpiredTime, stepName)
		}

		if bwf.Extra[WfExtraInfoKeyFsID] == "" && wfsStep.Cache.FsScope != "" {
			return fmt.Errorf("fs_scope of cache in step[%s] should be empty if Fs is not used!", stepName)
		}
	}

	return nil
}

func (bwf *BaseWorkflow) checkParams() error {
	for paramName, paramVal := range bwf.Params {
		if err := bwf.replaceRunParam(paramName, paramVal); err != nil {
			bwf.log().Errorf(err.Error())
			return err
		}
	}
	return nil
}

func (bwf *BaseWorkflow) replaceRunParam(param string, val interface{}) error {
	/*
		replaceRunParam 用传入的parameter参数值，替换yaml中的parameter默认参数值
		同时检查：参数是否存在，以及参数值是否合法。
		- stepName.param形式: 寻找对应step的对应parameter进行替换。如果step中没有该parameter，报错。
		- param: 寻找所有step内的parameter，并进行替换，没有则不替换。
		只检验命令行参数是否存在在 yaml 定义中，不校验是否必须是本次运行的 step
	*/
	nodesAndParam := ParseParamName(param)

	if len(nodesAndParam) > 1 {
		ok1, err := replaceNodeParam(bwf.Source.EntryPoints.EntryPoints, nodesAndParam, val)
		if err != nil {
			return err
		}
		// ok 为 false，且 err 为 nil，表示在entryPoints中没有找到要替换的节点，则去postProcess中寻找
		if !ok1 {
			postMap := map[string]schema.Component{}
			for name, component := range bwf.Source.PostProcess {
				postMap[name] = component
			}
			ok2, err2 := replaceNodeParam(postMap, nodesAndParam, val)
			if err2 != nil {
				return err2
			}
			if !ok2 {
				// 如果postProcess中也没有，则查找节点失败
				return fmt.Errorf("cannont find component to replace param with [%s]", param)
			}
		}
	} else if len(nodesAndParam) == 0 {
		paramName := nodesAndParam[0]
		if err := replaceAllNodeParam(bwf.Source.EntryPoints.EntryPoints, paramName, val); err != nil {
			return err
		}
	} else {
		fmt.Errorf("empty component list")
	}
	return nil
}

func replaceNodeParam(nodes map[string]schema.Component, nodesAndParam []string, value interface{}) (bool, error) {
	if len(nodesAndParam) > 2 {
		node, ok := nodes[nodesAndParam[0]].(*schema.WorkflowSourceDag)
		if !ok {
			return false, fmt.Errorf("replace param by request failed, node name list error")
		}
		ok, err := replaceNodeParam(node.EntryPoints, nodesAndParam[1:], value)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	} else if len(nodesAndParam) == 2 {
		nodeName := nodesAndParam[0]
		paramName := nodesAndParam[1]
		if dag, ok := nodes[nodeName].(*schema.WorkflowSourceDag); ok {
			orgVal, ok := dag.Parameters[paramName]
			if !ok {
				return false, nil
			}

			dictParam := DictParam{}
			if err := dictParam.From(orgVal); err == nil {
				if _, err := CheckDictParam(dictParam, paramName, value); err != nil {
					return false, err
				}
			}
			dag.Parameters[paramName] = value
		} else if step, ok := nodes[nodeName].(*schema.WorkflowSourceStep); ok {
			orgVal, ok := step.Parameters[paramName]
			if !ok {
				return false, nil
			}

			dictParam := DictParam{}
			if err := dictParam.From(orgVal); err == nil {
				if _, err := CheckDictParam(dictParam, paramName, value); err != nil {
					return false, err
				}
			}
			step.Parameters[paramName] = value
		}

	} else {
		return false, fmt.Errorf("length of node and param list error")
	}
	return true, nil
}

func replaceAllNodeParam(entryPoints map[string]schema.Component, paramName string, value interface{}) error {
	for _, node := range entryPoints {
		if dag, ok := node.(*schema.WorkflowSourceDag); ok {
			if orgVal, ok := dag.Parameters[paramName]; ok {
				dictParam := DictParam{}
				if err := dictParam.From(orgVal); err == nil {
					if _, err := CheckDictParam(dictParam, paramName, value); err != nil {
						return err
					}
				}
				dag.Parameters[paramName] = value
			}
			if err := replaceAllNodeParam(dag.EntryPoints, paramName, value); err != nil {
				return err
			}
		} else if step, ok := node.(*schema.WorkflowSourceStep); ok {
			if orgVal, ok := step.Parameters[paramName]; ok {
				dictParam := DictParam{}
				if err := dictParam.From(orgVal); err == nil {
					if _, err := CheckDictParam(dictParam, paramName, value); err != nil {
						return err
					}
				}
				step.Parameters[paramName] = value
			}
		}
	}
	return nil
}

// 将所有类型的steps合并到一起
func (bwf *BaseWorkflow) parseAllSteps() map[string]*schema.WorkflowSourceStep {
	steps := map[string]*schema.WorkflowSourceStep{}
	for name, step := range bwf.Source.EntryPoints {
		steps[name] = step
	}
	for name, step := range bwf.Source.PostProcess {
		// 虽然按照现有逻辑，在之前已经进行过查重，但考虑到后续该函数会在其他地方被调用，还是需要进行查重
		if _, ok := steps[name]; !ok {
			steps[name] = step
		}
	}
	return steps
}

func (bwf *BaseWorkflow) checkSteps() error {
	/*
		1. 检查disabled参数是否合法。
			- disabled节点以逗号分隔。
			- 所有disabled节点应该在entrypoints中，但是不要求必须在runSteps内

		2. 检查每个节点 env, command, parameters and artifacts 是否合法（包括模板引用，参数值）
			- 只检查此次运行中，可运行的节点集合(runSteps), 而不是yaml中定义的所有节点集合(Source)
			- 只校验，不替换任何参数
	*/

	disabledSteps, err := bwf.checkDisabled()
	if err != nil {
		bwf.log().Errorf("check disabled failed. err:%s", err.Error())
		return err
	}

	if err := bwf.checkDeps(); err != nil {
		bwf.log().Errorf("check deps failed. err: %s", err.Error())
		return err
	}

	useFs := true
	if bwf.Extra[WfExtraInfoKeyFsID] == "" {
		useFs = false
	}

	// 这里独立构建一个sysParamNameMap，因为有可能bwf.Extra传递的系统变量，数量或者名称有误
	// 这里只用于做校验，所以其值没有含义
	var sysParamNameMap = map[string]string{
		SysParamNamePFRunID:    "",
		SysParamNamePFFsID:     "",
		SysParamNamePFStepName: "",
		SysParamNamePFFsName:   "",
		SysParamNamePFUserName: "",
		SysParamNamePFRuntime:  "",
	}
	steps := map[string]*schema.WorkflowSourceStep{}
	for name, step := range bwf.runtimeSteps {
		steps[name] = step
	}
	for name, step := range bwf.postProcess {
		steps[name] = step
	}
	paramChecker := StepParamChecker{
		steps:         steps,
		sysParams:     sysParamNameMap,
		disabledSteps: disabledSteps,
		useFs:         useFs,
	}
	for _, step := range steps {
		bwf.log().Debugln(step)
	}
	for stepName, _ := range steps {
		isDisabled, err := bwf.Source.IsDisabled(stepName)
		if err != nil {
			return err
		}
		if isDisabled {
			continue
		}
		if err := paramChecker.Check(stepName); err != nil {
			bwf.log().Errorln(err.Error())
			return err
		}
	}

	return nil
}

// 检查PostProcess
func (bwf *BaseWorkflow) checkPostProcess() error {
	if len(bwf.Source.PostProcess) > 1 {
		return fmt.Errorf("post_process can only has 1 step at most")
	}

	for name, postStep := range bwf.Source.PostProcess {
		// 检查是否与EntryPoints中的step有重名
		if _, ok := bwf.Source.EntryPoints[name]; ok {
			return fmt.Errorf("a step in post_process has name [%s], which is same to name of a step in entry_points", name)
		}

		// 检查parameters、env、command中是否有引用上游parameters
		for _, param := range postStep.Parameters {
			if err := checkPostProcessParam(param); err != nil {
				return err
			}
		}
		for _, param := range postStep.Env {
			if err := checkPostProcessParam(param); err != nil {
				return err
			}
		}
		if err := checkPostProcessParam(postStep.Command); err != nil {
			return err
		}

		if len(postStep.Artifacts.Input) > 0 {
			return fmt.Errorf("step [%s] in post_process has input artifacts", name)
		}

		if len(postStep.Deps) > 0 {
			return fmt.Errorf("step [%s] in post_process has deps", name)
		}

		if postStep.Cache.Enable {
			return fmt.Errorf("step [%s] in post_process should not use cache", name)
		}
	}

	return nil
}

// 检查PostProcess是否引用了上游节点的Parameters
func checkPostProcessParam(param interface{}) error {
	switch param := param.(type) {
	case string:
		pattern := RegExpIncludingUpstreamTpl
		reg := regexp.MustCompile(pattern)
		matches := reg.FindStringSubmatch(param)
		if len(matches) > 0 {
			return fmt.Errorf("step in post_process can not use parameters of steps in entry_points")
		}
	}
	return nil
}

func (bwf *BaseWorkflow) checkDisabled() ([]string, error) {
	/*
		校验yaml中disabled字段是否合法
		- disabled节点以逗号分隔。
		- 所有disabled节点应该在entrypoints中，但是不要求必须在runSteps内。
		- 目前支持pipeline中所有节点都disabled
	*/
	tempMap := make(map[string]int)
	disabledSteps := bwf.Source.GetDisabled()
	for _, stepName := range disabledSteps {
		_, ok := tempMap[stepName]
		if ok {
			return nil, fmt.Errorf("disabled step[%s] is set repeatedly!", stepName)
		}
		tempMap[stepName] = 1
		if !bwf.Source.HasStep(stepName) {
			return nil, fmt.Errorf("disabled step[%s] not existed!", stepName)
		}
	}

	return disabledSteps, nil
}

// ----------------------------------------------------------------------------
// Workflow 工作流结构
// ----------------------------------------------------------------------------

type Workflow struct {
	BaseWorkflow
	runtime   *WorkflowRuntime
	callbacks WorkflowCallbacks
}

type WorkflowCallbacks struct {
	GetJobCb        func(runID string, stepName string) (schema.JobView, error)
	UpdateRuntimeCb func(string, interface{}) (int64, bool)
	LogCacheCb      func(req schema.LogRunCacheRequest) (string, error)
	ListCacheCb     func(firstFp, fsID, step, yamlPath string) ([]models.RunCache, error)
	LogArtifactCb   func(req schema.LogRunArtifactRequest) error
}

// 实例化一个Workflow，并返回
func NewWorkflow(wfSource schema.WorkflowSource, runID, entry string, params map[string]interface{}, extra map[string]string,
	callbacks WorkflowCallbacks) (*Workflow, error) {
	baseWorkflow := NewBaseWorkflow(wfSource, runID, entry, params, extra)

	wf := &Workflow{
		BaseWorkflow: baseWorkflow,
		callbacks:    callbacks,
	}
	if err := wf.validate(); err != nil {
		return nil, err
	}
	if err := wf.newWorkflowRuntime(); err != nil {
		return nil, err
	}
	return wf, nil
}

// 初始化 workflow runtime
func (wf *Workflow) newWorkflowRuntime() error {
	parallelism := wf.Source.Parallelism
	if parallelism <= 0 {
		parallelism = WfParallelismDefault
	} else if parallelism > WfParallelismMaximum {
		parallelism = WfParallelismMaximum
	}
	logger.LoggerForRun(wf.RunID).Debugf("initializing [%d] parallelism jobs", parallelism)
	wf.runtime = NewWorkflowRuntime(wf, parallelism)

	if err := wf.initRuntimeSteps(wf.runtime.entryPoints, wf.runtimeSteps, NodeTypeEntrypoint); err != nil {
		return err
	}

	if err := wf.initRuntimeSteps(wf.runtime.postProcess, wf.postProcess, NodeTypePostProcess); err != nil {
		return err
	}

	return nil
}

func (wf *Workflow) initRuntimeSteps(runtimeSteps map[string]*StepRuntime, steps map[string]*schema.WorkflowSourceStep, nodeType NodeType) error {
	// 此处topologicalSort不为了校验，而是为了排序，NewStep中会进行参数替换，必须保证上游节点已经替换完毕
	sortedSteps, err := wf.topologicalSort(steps)
	if err != nil {
		return err
	}
	wf.log().Debugf("get sorted run[%s] steps:[%+v]", wf.RunID, steps)
	for _, stepName := range sortedSteps {
		disabled, err := wf.Source.IsDisabled(stepName)
		if err != nil {
			return err
		}

		stepInfo := steps[stepName]
		runtimeSteps[stepName], err = NewStep(stepName, wf.runtime, stepInfo, disabled, nodeType)
		if err != nil {
			return err
		}
	}
	return nil
}

// set workflow runtime when server resuming
func (wf *Workflow) SetWorkflowRuntime(runtime schema.RuntimeView, postProcess schema.PostProcessView) error {
	wf.setRuntimeSteps(runtime, wf.runtime.entryPoints)
	wf.setRuntimeSteps(postProcess, wf.runtime.postProcess)
	return nil
}

func (wf *Workflow) setRuntimeSteps(runtime map[string]schema.JobView, steps map[string]*StepRuntime) {
	for name, step := range steps {
		jobView, ok := runtime[name]
		if !ok {
			continue
		}
		paddleflowJob := PaddleFlowJob{
			BaseJob: BaseJob{
				Id:         jobView.JobID,
				Name:       jobView.JobName,
				Command:    jobView.Command,
				Parameters: jobView.Parameters,
				Artifacts:  jobView.Artifacts,
				Env:        jobView.Env,
				StartTime:  jobView.StartTime,
				EndTime:    jobView.EndTime,
				Status:     jobView.Status,
			},
			Image: wf.Source.DockerEnv,
		}
		stepDone := false
		if !paddleflowJob.NotEnded() {
			stepDone = true
		}
		submitted := false
		if jobView.JobID != "" {
			submitted = true
		}
		step.update(stepDone, submitted, &paddleflowJob)
	}
}

// Start to run a workflow
func (wf *Workflow) Start() {
	wf.runtime.Start()
}

// Restart 从 DB 中恢复重启 workflow
// Restart 调用逻辑：1. NewWorkflow 2. SetWorkflowRuntime 3. Restart
func (wf *Workflow) Restart() {
	wf.runtime.Restart()
}

// Stop a workflow
func (wf *Workflow) Stop(force bool) {
	wf.runtime.Stop(force)
}

func (wf *Workflow) Status() string {
	return wf.runtime.Status()
}
