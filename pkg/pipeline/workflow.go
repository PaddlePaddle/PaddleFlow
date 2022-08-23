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
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Knetic/govaluate"
	"github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	. "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

// ----------------------------------------------------------------------------
//  BaseWorkflow 工作流基础结构
// ----------------------------------------------------------------------------

type BaseWorkflow struct {
	Name         string                                `json:"name,omitempty"`
	RunID        string                                `json:"runId,omitempty"`
	Desc         string                                `json:"desc,omitempty"`
	Params       map[string]interface{}                `json:"params,omitempty"`
	Extra        map[string]string                     `json:"extra,omitempty"` // 可以存放一些ID，fsId，userId等
	Source       schema.WorkflowSource                 `json:"-"`               // Yaml string
	runtimeDags  map[string]*schema.WorkflowSourceDag  `json:"-"`
	runtimeSteps map[string]*schema.WorkflowSourceStep `json:"-"`
	tmpDags      map[string]*schema.WorkflowSourceDag  `json:"-"`
	tmpSteps     map[string]*schema.WorkflowSourceStep `json:"-"`
	postProcess  map[string]*schema.WorkflowSourceStep `json:"-"`
}

func NewBaseWorkflow(wfSource schema.WorkflowSource, runID string, params map[string]interface{}, extra map[string]string) BaseWorkflow {
	// Todo: 设置默认值
	bwf := BaseWorkflow{
		RunID:       runID,
		Name:        "default_name",
		Desc:        "default_desc",
		Params:      params,
		Extra:       extra,
		Source:      wfSource,
		postProcess: wfSource.PostProcess,
	}

	bwf.runtimeDags = map[string]*schema.WorkflowSourceDag{}
	bwf.runtimeSteps = map[string]*schema.WorkflowSourceStep{}
	bwf.recursiveGetComponents(bwf.Source.EntryPoints.EntryPoints, "", bwf.runtimeDags, bwf.runtimeSteps)

	bwf.tmpDags = map[string]*schema.WorkflowSourceDag{}
	bwf.tmpSteps = map[string]*schema.WorkflowSourceStep{}
	bwf.recursiveGetComponents(bwf.Source.Components, "", bwf.tmpDags, bwf.tmpSteps)
	return bwf
}

func (bwf *BaseWorkflow) log() *logrus.Entry {
	return logger.LoggerForRun(bwf.RunID)
}

func (bwf *BaseWorkflow) checkCompAttrs() error {
	if err := bwf.checkAttrRecursively(bwf.Source.EntryPoints.EntryPoints); err != nil {
		return err
	}
	if err := bwf.checkAttrRecursively(bwf.Source.Components); err != nil {
		return err
	}
	postMap := map[string]schema.Component{}
	for k, v := range bwf.Source.PostProcess {
		postMap[k] = v
	}
	if err := bwf.checkAttrRecursively(postMap); err != nil {
		return err
	}
	return nil
}

// 校验Deps, Condition, LoopArguments
func (bwf *BaseWorkflow) checkAttrRecursively(components map[string]schema.Component) error {
	for name, component := range components {
		// deps
		for _, dep := range component.GetDeps() {
			if _, ok := components[dep]; !ok {
				return fmt.Errorf("component [%s] has an wrong dep [%s]", name, dep)
			}
		}

		// condition
		if err := bwf.checkCondition(component); err != nil {
			logger.LoggerForRun(bwf.RunID).Errorf("check condition failed, error: %s", err.Error())
			return err
		}

		// loopArgument
		if err := bwf.checkLoopArgument(component); err != nil {
			logger.LoggerForRun(bwf.RunID).Errorf("check loopArgument failed, error: %s", err.Error())
			return err
		}

		if dag, ok := component.(*schema.WorkflowSourceDag); ok {
			if err := bwf.checkAttrRecursively(dag.EntryPoints); err != nil {
				return err
			}
		} else if step, ok := component.(*schema.WorkflowSourceStep); ok {
			// step 特有的参数检查
			// dockerEnv
			if strings.HasSuffix(step.DockerEnv, ".tar") {
				return fmt.Errorf("image as tar file is not supported for now")
			}
		} else {
			return fmt.Errorf("component is not dag or step")
		}
	}
	return nil
}

func (bwf *BaseWorkflow) checkCondition(component schema.Component) error {
	condition := component.GetCondition()
	if condition == "" {
		return nil
	}
	pattern := RegExpIncludingCurTpl
	reg := regexp.MustCompile(pattern)
	matches := reg.FindAllStringSubmatch(condition, -1)
	for _, row := range matches {
		if len(row) != 4 {
			return MismatchRegexError(condition, pattern)
		}
		argName := row[2]
		_, ok1 := component.GetParameters()[argName]
		_, ok2 := component.GetArtifacts().Input[argName]

		sysParamNameMap := map[string]string{}
		for _, name := range SysParamNameList {
			sysParamNameMap[name] = ""
		}
		_, ok3 := sysParamNameMap[argName]

		if !ok1 && !ok2 && !ok3 {
			return fmt.Errorf("{{%s}} in condition is not parameter or input artifact or system templete", argName)
		}
	}

	newCondition := condition
	newCondition = strings.ReplaceAll(newCondition, "{{", "")
	newCondition = strings.ReplaceAll(newCondition, "}}", "")
	_, err := govaluate.NewEvaluableExpression(newCondition)
	if err != nil {
		return fmt.Errorf("condition[%s] is invalid, error: %s", condition, err.Error())
	}
	return nil
}

func (bwf *BaseWorkflow) checkLoopArgument(component schema.Component) error {
	loop := component.GetLoopArgument()
	if loop == nil {
		return nil
	}
	switch loop := loop.(type) {
	case []interface{}:
		for _, v := range loop {
			switch v := v.(type) {
			case int, int64, float32, float64:
				// do nothing
			case string:
				checker := VariableChecker{}
				// list中的元素不能为模板，如果使用了模板，则报错
				if err := checker.CheckRefArgument(v); err == nil {
					return fmt.Errorf("[%v]in loopArgument of component [%s] is invalid, each one in list loopArgument must not be templete",
						v, component.GetName())
				}
			case []interface{}:
				if err := CheckListParam(v); err != nil {
					return fmt.Errorf("component [%s] check loopArgument failed: %s", component.GetName(), err.Error())
				}
			default:
				return fmt.Errorf("[%v]in loopArgument  of component [%s] is invalid, each one with list type can only have int, float, string, list",
					v, component.GetName())
			}
		}
	case string:
		pattern := RegExpIncludingCurTpl
		reg := regexp.MustCompile(pattern)
		matches := reg.FindAllStringSubmatch(loop, -1)

		if len(matches) > 1 {
			// loopArgument不能用多余一个的模板
			return fmt.Errorf("loopArgument[%v] is invalid, using templates in loopArgument invalid", loop)
		} else if len(matches) == 1 {
			// 如果使用了模板，则只能有模板，不能再加其他内容
			pattern = RegExpCurTpl
			reg = regexp.MustCompile(pattern)
			if !reg.MatchString(loop) {
				return fmt.Errorf("loopArgument[%v] is invalid, using template and others in the sametime", loop)
			}

			for _, row := range matches {
				if len(row) != 4 {
					return MismatchRegexError(loop, pattern)
				}
				argName := row[2]
				_, ok1 := component.GetParameters()[argName]
				_, ok2 := component.GetArtifacts().Input[argName]
				if !ok1 && !ok2 {
					return fmt.Errorf("loopArgument {{%s}} is not parameter or input artifact", argName)
				}
			}
		} else {
			// 如果不是模板，那就必须是JsonList
			listArg := []interface{}{}
			if err := json.Unmarshal([]byte(loop), &listArg); err != nil {
				return fmt.Errorf("loopArgument [%s] unmarshal to list failed, error: %s", loop, err.Error())
			}
			for _, arg := range listArg {
				switch arg := arg.(type) {
				case int, int64, float32, float64, string:
					// do nothing
				case []interface{}:
					if err := CheckListParam(arg); err != nil {
						return fmt.Errorf("loopArgument of component[%s] check failed: %s", component.GetName(), err.Error())
					}
				default:
					return fmt.Errorf("each one in list loopArgument should be int or float or string")
				}
			}
		}
	default:
		return fmt.Errorf("loopArgument can only be string or list type")
	}
	return nil
}

func (bwf *BaseWorkflow) recursiveGetComponents(components map[string]schema.Component, prefix string, dags map[string]*schema.WorkflowSourceDag, steps map[string]*schema.WorkflowSourceStep) {
	for name, component := range components {
		var absoluteName string
		if prefix != "" {
			absoluteName = prefix + "." + name
		} else {
			absoluteName = name
		}

		if dag, ok := component.(*schema.WorkflowSourceDag); ok {
			dags[absoluteName] = dag
			bwf.recursiveGetComponents(dag.EntryPoints, absoluteName, dags, steps)
		} else {
			step := component.(*schema.WorkflowSourceStep)
			steps[absoluteName] = step
		}
	}
}

// validate BaseWorkflow 校验合法性
func (bwf *BaseWorkflow) validate() error {
	// 1. 检查Components/Reference，不包括parameters校验
	// 必须要先检查递归，否则后续的Disable的检查会有死循环的可能
	if err := bwf.checkComponents(); err != nil {
		bwf.log().Errorf("check components failed, err: %s", err.Error())
		return err
	}

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
	if err := bwf.checkComps(); err != nil {
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
		bwf.log().Errorf("check failure_option failed. err: %s", err.Error())
		return err
	}

	// 9. 检查mainFS、extraFS，并填充fsID
	if err := bwf.checkFS(); err != nil {
		bwf.log().Errorf("check fs failed. err: %s", err.Error())
		return err
	}

	return nil
}

// 检查fsScope的fsName是否在fsMount中，同时根据Username和FSName生成FSID
func (bwf *BaseWorkflow) checkFS() error {
	// 1. 校验并处理MainFS
	if bwf.Source.FsOptions.MainFS.ReadOnly {
		return fmt.Errorf("[read_only] in [main_fs] must be false")
	}
	if strings.HasPrefix(bwf.Source.FsOptions.MainFS.SubPath, "/") {
		return fmt.Errorf("[sub_path] in [main_fs] should not start with '/'")
	}

	if bwf.Source.FsOptions.MainFS.Name != "" {
		bwf.Source.FsOptions.MainFS.ID = common.ID(bwf.Extra[WfExtraInfoKeyFSUserName], bwf.Source.FsOptions.MainFS.Name)
	}

	// 2. 校验并处理ExtraFS
	if err := bwf.processExtraFS(bwf.Extra[WfExtraInfoKeyFSUserName], bwf.Extra[WfExtraInfoKeyFsName]); err != nil {
		return err
	}
	return nil
}

func (bwf *BaseWorkflow) processExtraFS(userName string, fsName string) error {
	if err := bwf.processFsByUserName(bwf.Source.EntryPoints.EntryPoints, userName); err != nil {
		return err
	}

	if err := bwf.processFsByUserName(bwf.Source.Components, userName); err != nil {
		return err
	}

	postMap := map[string]schema.Component{}
	for k, v := range bwf.Source.PostProcess {
		postMap[k] = v
	}
	if err := bwf.processFsByUserName(postMap, userName); err != nil {
		return err
	}

	return nil
}

func (bwf *BaseWorkflow) processFsByUserName(compMap map[string]schema.Component, userName string) error {
	for _, comp := range compMap {
		if dag, ok := comp.(*schema.WorkflowSourceDag); ok {
			if err := bwf.processFsByUserName(dag.EntryPoints, userName); err != nil {
				return err
			}
		} else if step, ok := comp.(*schema.WorkflowSourceStep); ok {
			// fsNameChecker用来检查FsScope中的FsName是否都在ExtraFS或MainFS中
			fsNameChecker := map[string]int{}
			if bwf.Source.FsOptions.MainFS.Name != "" {
				// 请求体中的MainFS会替换wfs中的MainFS，或者与wfs中的相同，所以无需检查
				fsNameChecker[bwf.Source.FsOptions.MainFS.Name] = 1
			}

			for i, mount := range step.ExtraFS {
				// ExtraFS中的name不能为空
				if mount.Name == "" {
					return fmt.Errorf("[name] in [extra_fs] or [main_fs] must not be empty")
				}
				// ExtraFS中subPath不能以 "/" 开头
				if strings.HasPrefix(mount.SubPath, "/") {
					return fmt.Errorf("[sub_path] in [extra_fs] should not start with '/'")
				}
				mount.ID = common.ID(userName, mount.Name)

				fsNameChecker[mount.Name] = 1
				step.ExtraFS[i] = mount
			}
			for i, scope := range step.Cache.FsScope {
				if scope.Name == "" {
					return fmt.Errorf("[fs_name] in fs_scope must not be empty")
				}
				scope.ID = common.ID(userName, scope.Name)

				// 检查FsScope中的FsName是否都在FsMount中
				if _, ok := fsNameChecker[scope.Name]; !ok {
					return fmt.Errorf("fs_name [%s] in fs_scope must also be in [extra_fs] or [main_fs]", scope.Name)
				}
				step.Cache.FsScope[i] = scope
			}
		} else {
			return fmt.Errorf("component not dag or step")
		}
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

func (bwf *BaseWorkflow) checkComponents() error {
	/*
		components不能有deps(最外层，不包括子节点)
		components/reference不支持递归
	*/
	for name, comp := range bwf.Source.Components {
		// component不能有deps
		if len(comp.GetDeps()) > 0 {
			return fmt.Errorf("components can not have deps")
		}

		// 递归检查
		visited := map[string]int{name: 1}
		if err := bwf.checkCyclicRef(comp, visited); err != nil {
			return err
		}
	}
	return nil
}

func (bwf *BaseWorkflow) checkCyclicRef(component schema.Component, visited map[string]int) error {
	if step, ok := component.(*schema.WorkflowSourceStep); ok {
		if step.Reference.Component != "" {
			refComp, ok := bwf.Source.Components[step.Reference.Component]
			if !ok {
				return fmt.Errorf("no component named %s", step.Reference)
			}

			// 如果visited已有将要reference的节点，则说明存在递归
			if _, ok := visited[step.Reference.Component]; ok {
				return fmt.Errorf("components reference is not acyclic")
			} else {
				visited[step.Reference.Component] = 1
			}
			if err := bwf.checkCyclicRef(refComp, visited); err != nil {
				return err
			}
			delete(visited, step.Reference.Component)
			return nil
		} else {
			return nil
		}
	} else if dag, ok := component.(*schema.WorkflowSourceDag); ok {
		for _, comp := range dag.EntryPoints {
			if err := bwf.checkCyclicRef(comp, visited); err != nil {
				return err
			}
		}
		return nil
	} else {
		return fmt.Errorf("component not dag or step")
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
		return fmt.Errorf("check pipelineName[%s] in run[%s] failed: %s", bwf.Source.Name, bwf.RunID, err.Error())
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
	if err := bwf.checkAllComponentName(bwf.Source.Components); err != nil {
		return err
	}

	if err := bwf.checkTopoSort(bwf.Source.EntryPoints.EntryPoints); err != nil {
		return err
	}
	if err := bwf.checkTopoSort(bwf.Source.Components); err != nil {
		return err
	}

	return nil
}

func (bwf *BaseWorkflow) checkAllComponentName(components map[string]schema.Component) error {
	variableChecker := VariableChecker{}
	for name, component := range components {
		err := variableChecker.CheckCompName(name)
		if err != nil {
			return err
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

	// FsScope 由于涉及到 Fs权限校验，FsID填充等操作，不便在此进行，在此前已经完成校验

	if err := bwf.checkStepCache(bwf.Source.EntryPoints.EntryPoints); err != nil {
		return err
	}
	return nil
}

func (bwf *BaseWorkflow) checkStepCache(components map[string]schema.Component) error {
	for name, component := range components {
		if dag, ok := component.(*schema.WorkflowSourceDag); ok {
			if err := bwf.checkStepCache(dag.EntryPoints); err != nil {
				return err
			}
		} else if step, ok := component.(*schema.WorkflowSourceStep); ok {
			if step.Reference.Component == "" {
				if step.Cache.MaxExpiredTime == "" {
					step.Cache.MaxExpiredTime = CacheExpiredTimeNever
				}
				_, err := strconv.Atoi(step.Cache.MaxExpiredTime)
				if err != nil {
					return fmt.Errorf("MaxExpiredTime[%s] of cache in step[%s] not correct", step.Cache.MaxExpiredTime, name)
				}
			}
		} else {
			return fmt.Errorf("component not step or dag")
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
	} else if len(nodesAndParam) == 1 {
		paramName := nodesAndParam[0]
		ok1, err := replaceAllNodeParam(bwf.Source.EntryPoints.EntryPoints, paramName, val)
		if err != nil {
			return err
		}
		postMap := map[string]schema.Component{}
		for name, component := range bwf.Source.PostProcess {
			postMap[name] = component
		}
		ok2, err := replaceAllNodeParam(postMap, paramName, val)
		if err != nil {
			return err
		}
		if !ok1 && !ok2 {
			return fmt.Errorf("param[%s] not exist", paramName)
		}
	} else {
		return fmt.Errorf("empty component list")
	}
	return nil
}

func replaceNodeParam(nodes map[string]schema.Component, compsAndParam []string, value interface{}) (bool, error) {
	if len(compsAndParam) > 2 {
		node, ok := nodes[compsAndParam[0]]
		if !ok {
			return false, nil
		}
		dag, ok := node.(*schema.WorkflowSourceDag)
		if !ok {
			return false, fmt.Errorf("replace param by request failed, component [%s] should be a dag, not step", compsAndParam[0])
		}
		ok, err := replaceNodeParam(dag.EntryPoints, compsAndParam[1:], value)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	} else if len(compsAndParam) == 2 {
		nodeName := compsAndParam[0]
		paramName := compsAndParam[1]

		comp, ok := nodes[nodeName]
		if !ok {
			return false, nil
		}
		if dag, ok := comp.(*schema.WorkflowSourceDag); ok {
			orgVal, ok := dag.Parameters[paramName]
			if !ok {
				return false, fmt.Errorf("no parameter named [%s] in dag [%s]", paramName, nodeName)
			}

			dictParam := DictParam{}
			if err := dictParam.From(orgVal); err == nil {
				if _, err := CheckDictParam(dictParam, paramName, value); err != nil {
					return false, err
				}
			}
			dag.Parameters[paramName] = value
		} else if step, ok := comp.(*schema.WorkflowSourceStep); ok {
			orgVal, ok := step.Parameters[paramName]
			if !ok {
				return false, fmt.Errorf("no parameter named [%s] in step [%s]", paramName, nodeName)
			}

			dictParam := DictParam{}
			if err := dictParam.From(orgVal); err == nil {
				if _, err := CheckDictParam(dictParam, paramName, value); err != nil {
					return false, err
				}
			}
			step.Parameters[paramName] = value
		} else {
			return false, fmt.Errorf("component not step or dag")
		}
	} else {
		return false, fmt.Errorf("length of node and param list error")
	}
	return true, nil
}

func replaceAllNodeParam(entryPoints map[string]schema.Component, paramName string, value interface{}) (bool, error) {
	// ok用于判断是否有过替换操作，如果遍历所有节点都没有替换，那最外层需要报错
	isReplace := false

	for _, node := range entryPoints {
		if dag, ok := node.(*schema.WorkflowSourceDag); ok {
			if orgVal, ok := dag.Parameters[paramName]; ok {
				dictParam := DictParam{}
				if err := dictParam.From(orgVal); err == nil {
					if _, err := CheckDictParam(dictParam, paramName, value); err != nil {
						return false, err
					}
				}
				dag.Parameters[paramName] = value
				isReplace = true
			}
			isReplaceSub, err := replaceAllNodeParam(dag.EntryPoints, paramName, value)
			if err != nil {
				return false, err
			}
			isReplace = isReplace || isReplaceSub
		} else if step, ok := node.(*schema.WorkflowSourceStep); ok {
			if orgVal, ok := step.Parameters[paramName]; ok {
				dictParam := DictParam{}
				if err := dictParam.From(orgVal); err == nil {
					if _, err := CheckDictParam(dictParam, paramName, value); err != nil {
						return false, err
					}
				}
				step.Parameters[paramName] = value
				isReplace = true
			}
		}
	}
	return isReplace, nil
}

func (bwf *BaseWorkflow) checkComps() error {
	/*
		该函数检查了Components、EntryPoints、PostProcess

		1. 检查disabled参数是否合法。
			- disabled节点以逗号分隔。
			- 所有disabled节点应该在entrypoints/components/postProcess中，但是不要求必须在runSteps内

		2. 检查每个节点 env, command, parameters and artifacts 是否合法（包括模板引用，参数值）
			- 只检查此次运行中，可运行的节点集合(runSteps), 而不是yaml中定义的所有节点集合(Source)
			- 只校验，不替换任何参数
	*/

	_, err := bwf.checkDisabled()
	if err != nil {
		bwf.log().Errorf("check disabled failed. err:%s", err.Error())
		return err
	}

	if err := bwf.checkCompAttrs(); err != nil {
		bwf.log().Errorf("check components' attribute failed. err: %s", err.Error())
		return err
	}

	useFs := true
	if bwf.Extra[WfExtraInfoKeyFsID] == "" {
		useFs = false
	}

	// 这里独立构建一个sysParamNameMap，用于做校验，其value没有含义
	sysParamNameMap := map[string]string{}
	for _, name := range SysParamNameList {
		sysParamNameMap[name] = ""
	}

	// 同时检查entryPoints、postProcess
	runComponents := map[string]schema.Component{}
	for name, step := range bwf.runtimeSteps {
		runComponents[name] = step
	}
	for name, dag := range bwf.runtimeDags {
		runComponents[name] = dag
	}
	for name, step := range bwf.postProcess {
		runComponents[name] = step
	}
	runParamChecker := ComponentParamChecker{
		Components:    runComponents,
		SysParams:     sysParamNameMap,
		UseFs:         useFs,
		CompTempletes: bwf.Source.Components,
	}

	for name, _ := range runComponents {
		isDisabled, err := bwf.Source.IsDisabled(name)
		if err != nil {
			return err
		}
		if isDisabled {
			continue
		}
		if err := runParamChecker.Check(name, false); err != nil {
			bwf.log().Errorln(err.Error())
			return err
		}
	}

	tmplComps := map[string]schema.Component{}
	for name, dag := range bwf.tmpDags {
		tmplComps[name] = dag
	}
	for name, step := range bwf.tmpSteps {
		tmplComps[name] = step
	}
	tmplParamChecker := ComponentParamChecker{
		Components:    tmplComps,
		SysParams:     sysParamNameMap,
		UseFs:         useFs,
		CompTempletes: bwf.Source.Components,
	}

	for name, _ := range tmplComps {
		if err := tmplParamChecker.Check(name, true); err != nil {
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
		if _, ok := bwf.Source.EntryPoints.EntryPoints[name]; ok {
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

		if postStep.Condition != "" {
			return fmt.Errorf("step [%s] in post_process should not have condition", name)
		}

		if postStep.LoopArgument != nil {
			return fmt.Errorf("step [%s] in post_process should not have loop_argument", name)
		}

		// postProcess必须是step，不能是dag
		if postStep.Reference.Component != "" {
			for {
				refComp, ok := bwf.Source.Components[postStep.Reference.Component]
				if !ok {
					return fmt.Errorf("reference[%s] of step [%s] in post_process is not exist", postStep.Reference.Component, name)
				}
				if step, ok := refComp.(*schema.WorkflowSourceStep); ok {
					if step.Reference.Component != "" {
						continue
					} else {
						break
					}
				} else {
					return fmt.Errorf("component in post_process should be a step, not dag")
				}
			}
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
	disabledComponents := bwf.Source.GetDisabled()
	postComponents := map[string]schema.Component{}
	for k, v := range bwf.Source.PostProcess {
		postComponents[k] = v
	}

	disabledMap := map[string]int{}
	for _, compAbsName := range disabledComponents {
		disabledMap[compAbsName] = 1
	}

	for _, disFullName := range disabledComponents {
		_, ok := tempMap[disFullName]
		if ok {
			return nil, fmt.Errorf("disabled component[%s] is set repeatedly!", disFullName)
		}
		tempMap[disFullName] = 1
		components1, name1, ok1 := bwf.Source.GetCompsMapAndRelName(bwf.Source.EntryPoints.EntryPoints, disFullName)
		components2, name2, ok2 := bwf.Source.GetCompsMapAndRelName(postComponents, disFullName)
		var components map[string]schema.Component
		disName := ""
		if ok1 {
			components, disName = components1, name1
		} else if ok2 {
			components, disName = components2, name2
		} else {
			return nil, fmt.Errorf("disabled component[%s] not existed!", disFullName)
		}

		// 检查被disabled的节点有没有被引用
		// 先检查同级别的节点是否有引用
		for compName, comp := range components {
			_, ok1 := disabledMap[GetSiblingAbsoluteName(disFullName, compName)]

			depsMap := map[string]int{}
			for _, dep := range comp.GetDeps() {
				depsMap[dep] = 1
			}
			_, ok2 := depsMap[disName]
			// 如果该节点为自身、为disabled的节点、不在同级别节点的deps中，则不需要检查
			if compName == disName || ok1 || !ok2 {
				continue
			}
			// 检查输入Artifact引用
			for _, atfVal := range comp.GetArtifacts().Input {
				ok, err := checkRefed(disName, atfVal)
				if err != nil {
					return nil, err
				}
				if ok {
					return nil, fmt.Errorf("disabled component[%s] is refered by [%s]", disName, compName)
				}
			}

			// 检查parameters引用
			for _, paramVal := range comp.GetParameters() {
				paramVal, ok := paramVal.(string)
				if !ok {
					continue
				}
				ok, err := checkRefed(disName, paramVal)
				if err != nil {
					return nil, err
				}
				if ok {
					return nil, fmt.Errorf("disabled component[%s] is refered by [%s]", disName, compName)
				}
			}
		}

		// 再检查父节点是否有引用（输出Artifact引用）
		disNameList := strings.Split(disFullName, ".")
		disParentFullName := strings.Join(disNameList[:len(disNameList)-1], ".")
		_, ok = disabledMap[disParentFullName]
		if len(disNameList) > 1 && !ok {
			// 如果该节点有父节点，且该父节点没有被disabled，才需要检查
			disParentFullName := strings.Join(disNameList[:len(disNameList)-1], ".")
			components1, name1, ok1 := bwf.Source.GetCompsMapAndRelName(bwf.Source.EntryPoints.EntryPoints, disParentFullName)
			components2, name2, ok2 := bwf.Source.GetCompsMapAndRelName(postComponents, disParentFullName)
			var parentComponents map[string]schema.Component
			parentName := ""
			if ok1 {
				parentComponents, parentName = components1, name1
			} else if ok2 {
				parentComponents, parentName = components2, name2
			} else {
				return nil, fmt.Errorf("disabled component[%s] not existed!", disParentFullName)
			}
			for _, atfVal := range parentComponents[parentName].GetArtifacts().Output {
				ok, err := checkRefed(disName, atfVal)
				if err != nil {
					return nil, err
				}
				if ok {
					return nil, fmt.Errorf("disabled component[%s] is refered by [%s]", disName, parentName)
				}
			}
		}
	}

	return disabledComponents, nil
}

func checkRefed(target string, strWithRef string) (bool, error) {
	pattern := RegExpIncludingUpstreamTpl
	reg := regexp.MustCompile(pattern)
	matches := reg.FindAllStringSubmatch(strWithRef, -1)
	for _, row := range matches {
		if len(row) != 4 {
			return false, MismatchRegexError(strWithRef, pattern)
		}
		refList := ParseParamName(row[2])
		if len(refList) != 2 {
			return false, MismatchRegexError(strWithRef, pattern)
		}
		if target == refList[0] {
			return true, nil
		}
	}
	return false, nil
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
	GetJobCb        func(jobID string) (schema.JobView, error)
	UpdateRuntimeCb func(id string, event interface{}) (int64, bool)
	LogCacheCb      func(req schema.LogRunCacheRequest) (string, error)
	ListCacheCb     func(firstFp, fsID, source string) ([]models.RunCache, error)
	LogArtifactCb   func(req schema.LogRunArtifactRequest) error
}

// 实例化一个Workflow，并返回
func NewWorkflow(wfSource schema.WorkflowSource, runID string, params map[string]interface{}, extra map[string]string,
	callbacks WorkflowCallbacks) (*Workflow, error) {
	baseWorkflow := NewBaseWorkflow(wfSource, runID, params, extra)

	wf := &Workflow{
		BaseWorkflow: baseWorkflow,
		callbacks:    callbacks,
	}
	if err := wf.validate(); err != nil {
		return nil, err
	}
	if err := wf.NewWorkflowRuntime(); err != nil {
		return nil, err
	}
	return wf, nil
}

// 初始化 workflow runtime
func (wf *Workflow) NewWorkflowRuntime() error {
	if wf.Source.Parallelism <= 0 {
		wf.Source.Parallelism = WfParallelismDefault
	} else if wf.Source.Parallelism > WfParallelismMaximum {
		wf.Source.Parallelism = WfParallelismMaximum
	}

	logger.LoggerForRun(wf.RunID).Debugf("initializing [%d] parallelism jobs", wf.Source.Parallelism)
	runConf := NewRunConfig(&wf.Source, &wf.Source.FsOptions.MainFS, wf.Extra[WfExtraInfoKeyFSUserName], wf.RunID,
		logger.LoggerForRun(wf.RunID), wf.callbacks, wf.Extra[WfExtraInfoKeySource])
	wf.runtime = NewWorkflowRuntime(runConf)

	return nil
}

// Start to run a workflow
func (wf *Workflow) Start() {
	wf.runtime.Start()
}

func (wf *Workflow) Resume(entryPointView *schema.DagView, postProcessView schema.PostProcessView, runStatus string, stopForce bool) {
	wf.runtime.Resume(entryPointView, postProcessView, runStatus, stopForce)
}

// Restart 从 DB 中恢复重启 workflow
// Restart 调用逻辑：1. NewWorkflow 2. SetWorkflowRuntime 3. Restart
func (wf *Workflow) Restart(entryPointView *schema.DagView, postProcessView schema.PostProcessView) {
	wf.runtime.Restart(entryPointView, postProcessView)
}

// Stop a workflow
func (wf *Workflow) Stop(force bool) {
	wf.runtime.Stop(force)
}

func (wf *Workflow) Status() string {
	return wf.runtime.Status()
}
