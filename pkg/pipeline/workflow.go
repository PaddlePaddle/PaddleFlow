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
	"errors"
	"fmt"
	"strconv"

	"github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
)

// ----------------------------------------------------------------------------
//  BaseWorkflow 工作流基础结构
// ----------------------------------------------------------------------------

type BaseWorkflow struct {
	Name     string                                `json:"name,omitempty"`
	RunID    string                                `json:"runId,omitempty"`
	Desc     string                                `json:"desc,omitempty"`
	Entry    string                                `json:"entry,omitempty"`
	Params   map[string]interface{}                `json:"params,omitempty"`
	Extra    map[string]string                     `json:"extra,omitempty"` // 可以存放一些ID，fsId，userId等
	Source   schema.WorkflowSource                 `json:"-"`               // Yaml string
	runSteps map[string]*schema.WorkflowSourceStep `json:"-"`
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
	bwf.runSteps = bwf.getRunSteps()
	return bwf
}

func (bwf *BaseWorkflow) log() *logrus.Entry {
	return logger.LoggerForRun(bwf.RunID)
}

// validate BaseWorkflow 校验合法性
func (bwf *BaseWorkflow) validate() error {
	// 校验 entry 是否存在
	if _, ok := bwf.Source.EntryPoints[bwf.Entry]; bwf.Entry != "" && !ok {
		err := fmt.Errorf("entry[%s] not exist in run", bwf.Entry)
		bwf.log().Errorln(err.Error())
		return err
	}

	// 2. RunYaml 中schema结构、流程是否合法，是否有环等；
	if err := bwf.checkRunYaml(); err != nil {
		bwf.log().Errorf("check run yaml failed. err:%s", err.Error())
		return err
	}
	// 3. Params 传参是否合法，是否有key不匹配的情况；
	if err := bwf.checkParams(); err != nil {
		bwf.log().Errorf("check run param err:%s", err.Error())
		return err
	}
	// 4. steps 中的 parameter artifact env 是否合法
	if err := bwf.checkSteps(); err != nil {
		bwf.log().Errorf("check run param err:%s", err.Error())
		return err
	}
	return nil
}

func (bwf *BaseWorkflow) checkRunYaml() error {
	for stepName := range bwf.Source.EntryPoints {
		if stepName == "" {
			return fmt.Errorf("stepName is not allowed to be empty in run[%s]", bwf.RunID)
		}
	}

	if _, err := bwf.topologicalSort(bwf.runSteps); err != nil {
		return err
	}

	if err := bwf.checkCache(); err != nil {
		return err
	}

	return nil
}

func (bwf *BaseWorkflow) checkCache() error {
	// 校验yaml中cache各相关字段

	// 校验MaxExpiredTime，必须能够转换成数字。如果没传，默认为-1
	if bwf.Source.Cache.MaxExpiredTime == "" {
		bwf.Source.Cache.MaxExpiredTime = CacheExpiredTimeNever
	}
	_, err := strconv.Atoi(bwf.Source.Cache.MaxExpiredTime)
	if err != nil {
		return fmt.Errorf("MaxExpiredTime[%s] of cache not correct", bwf.Source.Cache.MaxExpiredTime)
	}

	// 校验FsScope。计算目录通过逗号分隔。如果没传，默认更新为"/"。
	// 此处不校验path格式是否valid，以及path是否存在（如果不valid或者不存在，在计算cache，查询FsScope更新时间时，会获取失败）
	if bwf.Source.Cache.FsScope == "" {
		bwf.Source.Cache.FsScope = "/"
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

// topologicalSort step 拓扑排序
// todo: use map as return value type?
func (bwf *BaseWorkflow) topologicalSort(entrypoints map[string]*schema.WorkflowSourceStep) ([]string, error) {
	// unsorted: unsorted graph
	// if we have dag:
	//     1 -> 2 -> 3
	// then unsorted as follow will be get:
	//     1 -> [2]
	//     2 -> [3]
	sortedSteps := make([]string, 0)
	unsorted := map[string][]string{}
	for name, step := range entrypoints {
		if len(step.Deps) == 0 {
			unsorted[name] = nil
			continue
		}
		for _, dep := range step.GetDeps() {
			if _, ok := unsorted[name]; ok {
				unsorted[name] = append(unsorted[name], dep)
			} else {
				unsorted[name] = []string{dep}
			}
		}
	}
	for len(unsorted) != 0 {
		acyclic := false
		for name, parents := range unsorted {
			sorted := true
			for _, parent := range parents {
				if _, ok := unsorted[parent]; ok {
					sorted = false
					break
				}
			}
			// if all the source nodes of this node has been removed,
			// consider it as sorted and remove this node from the unsorted graph
			if sorted {
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

// replaceRunParam 用命令行参数替换启动的默认参数
// 只检验命令行参数是否存在在 yaml 定义中，不校验是否必须是本次运行的 step
func (bwf *BaseWorkflow) replaceRunParam(param string, val interface{}) error {
	stepName, paramName := parseParamName(param)

	if len(stepName) != 0 {
		step, ok1 := bwf.Source.EntryPoints[stepName]
		if !ok1 {
			errMsg := fmt.Sprintf("not found step[%s] in param[%s]", stepName, param)
			return errors.New(errMsg)
		}
		orgVal, ok2 := step.Parameters[paramName]
		if !ok2 {
			errMsg := fmt.Sprintf("param[%s] not exit in step[%s]", paramName, stepName)
			return errors.New(errMsg)
		}
		dictParam := DictParam{}
		if err := dictParam.From(orgVal); err == nil {
			if _, err := checkDictParam(dictParam, paramName, val); err != nil {
				return err
			}
		}
		step.Parameters[paramName] = val
		return nil
	}

	for _, step := range bwf.Source.EntryPoints {
		if orgVal, ok := step.Parameters[paramName]; ok {
			dictParam := DictParam{}
			if err := dictParam.From(orgVal); err == nil {
				if _, err := checkDictParam(dictParam, paramName, val); err != nil {
					return err
				}
			}
			step.Parameters[paramName] = val
			return nil
		}
	}
	return fmt.Errorf("param[%s] not exist", param)
}

// checkSteps check env, command, parameters and artifacts in every step
func (bwf *BaseWorkflow) checkSteps() error {
	var sysParamNameMap = map[string]string{
		SysParamNamePFRunID:    "",
		SysParamNamePFFsID:     "",
		SysParamNamePFStepName: "",
		SysParamNamePFFsName:   "",
		SysParamNamePFUserName: "",
	}
	paramSolver := StepParamSolver{steps: bwf.runSteps, sysParams: sysParamNameMap}
	for stepName, _ := range bwf.runSteps {
		if err := paramSolver.Solve(stepName); err != nil {
			bwf.log().Errorln(err.Error())
			return err
		}
	}

	return nil
}

func (bwf *BaseWorkflow) getRunSteps() map[string]*schema.WorkflowSourceStep {
	entry := bwf.Entry
	if entry == "" {
		return bwf.Source.EntryPoints
	}

	runSteps := map[string]*schema.WorkflowSourceStep{}
	bwf.recursiveGetRunSteps(entry, runSteps)
	return runSteps
}

func (bwf *BaseWorkflow) recursiveGetRunSteps(entry string, steps map[string]*schema.WorkflowSourceStep) {
	if _, ok := steps[entry]; ok {
		// duplicated in result map
		return
	}

	if step, ok := bwf.Source.EntryPoints[entry]; ok {
		steps[entry] = step
		for _, dep := range step.GetDeps() {
			bwf.recursiveGetRunSteps(dep, steps)
		}
	}
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
	UpdateRunCb   func(string, interface{}) bool
	LogCacheCb    func(req schema.LogRunCacheRequest) (string, error)
	ListCacheCb   func(firstFp, fsID, step, yamlPath string) ([]models.RunCache, error)
	LogArtifactCb func(req schema.LogRunArtifactRequest) error
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
	var err error
	parallelism := wf.Source.Parallelism
	if parallelism <= 0 {
		parallelism = WfParallelismDefault
	} else if parallelism > WfParallelismMaximum {
		parallelism = WfParallelismMaximum
	}
	logger.LoggerForRun(wf.RunID).Debugf("initializing [%d] parallelism jobs", parallelism)
	wf.runtime = NewWorkflowRuntime(wf, parallelism)

	sortedSteps, err := wf.topologicalSort(wf.runSteps)
	if err != nil {
		return err
	}
	wf.log().Debugf("get sorted run[%s] steps:[%+v]", wf.RunID, wf.runSteps)
	for _, stepName := range sortedSteps {
		stepInfo := wf.runSteps[stepName]
		if stepInfo.Image == "" {
			stepInfo.Image = wf.Source.DockerEnv
		}
		wf.runtime.steps[stepName], err = NewStep(stepName, wf.runtime, stepInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

// set workflow runtime when server resuming
func (wf *Workflow) SetWorkflowRuntime(runtime schema.RuntimeView) error {
	for name, step := range wf.runtime.steps {
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
				Env:        jobView.Env,
				StartTime:  jobView.StartTime,
				EndTime:    jobView.EndTime,
				Status:     jobView.Status,
				Deps:       jobView.Deps,
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
	return nil
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
func (wf *Workflow) Stop() {
	wf.runtime.Stop()
}

func (wf *Workflow) Status() string {
	return wf.runtime.Status()
}
