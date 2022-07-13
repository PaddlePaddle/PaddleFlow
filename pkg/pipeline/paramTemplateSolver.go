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
	"reflect"
	"regexp"
	"strings"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	. "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

// 抓取参数引用模板
func fetchTemplate(tplString string) ([][]string, error) {
	pattern := RegExpIncludingTpl
	reg := regexp.MustCompile(pattern)

	matches := reg.FindAllStringSubmatch(tplString, -1)
	for index := range matches {
		row := matches[index]
		if len(row) != 4 {
			err := MismatchRegexError(tplString, pattern)
			return matches, err
		}
	}

	return matches, nil
}

// parseTemplate: 解析参数引用模板
func parseTemplate(paramName string) (string, string) {
	paramStrList := strings.Split(paramName, ".")
	if len(paramStrList) == 1 {
		return "", paramStrList[0]
	}

	return paramStrList[0], paramStrList[1]
}

// parseSysParamerterTemplate： 获取 SysParam 模板的值
func parseSysParamerterTemplate(tpl string, sysParams map[string]string) (string, error) {
	param, ok := sysParams[tpl]
	if !ok {
		err := fmt.Errorf("cannot parse param template[%s] as Sysparam", tpl)
		return "", err
	}
	return param, nil
}

// 用于解析 component 内部的引用模版，如env，condition，conditon，loop_argument 等字段中 parameter/artifact 模版
type innerSolver struct {
	schema.Component
	runtimeName string
	sysParams   map[string]string
	*runConfig
}

func NewInnerSolver(cp schema.Component, runtimeName string, config *runConfig) *innerSolver {
	return &innerSolver{
		Component:   cp,
		runtimeName: runtimeName,
		runConfig:   config,
	}
}

func (isv *innerSolver) setSysParams(params map[string]string) {
	isv.sysParams = params
}

// resloveParameterTemplate: 将字符串中 sysParam 和 parameter 模板替换成真实值
// PS: 在 替换 loop_arguments 中的引用模板时，需要保留原始的类型信息，所以增加了 transToString 这个参数，表明是否要将对应 parameter 强转成 string
func (isv *innerSolver) resloveParameterTemplate(tpl []string, fieldType string) (interface{}, error) {
	_, refParamName := parseTemplate(tpl[2])

	// 1、尝试按照 sysParameter 解析
	value, err := parseSysParamerterTemplate(refParamName, isv.sysParams)
	if err == nil {
		// 对于 sysParameter，其值本来就是 string，因此无需关注 transString 的值
		return value, nil
	} else {
		// 2. 尝试按照 Parameter 解析
		value2, err := isv.Component.GetParameterValue(refParamName)
		if err != nil {
			err := fmt.Errorf("cannot parse Template[%s] in %s[%s] as Parameter or SysParameter: %v",
				isv.Component.GetType(), tpl[0], isv.runtimeName, err.Error())
			return "", err
		}

		if fieldType == FieldLoopArguemt {
			return value2, err
		}

		value = fmt.Sprintf("%v", value2)
	}
	return value, nil
}

// resolveArtifactTemplate： 将ArtifactTemplate 替换成对应路径或者其中的内容
func (isv *innerSolver) resolveArtifactTemplate(tpl []string, fieldType string, forCache bool) (string, error) {
	_, refParamName := parseTemplate(tpl[2])

	var result string
	var err error

	// 如果是在处理 cache 阶段时进行模板替换，此时对于输出artifact的的模板则不应该进行替换
	if forCache {
		_, ok := isv.GetArtifacts().Output[refParamName]
		if ok {
			return tpl[0], nil
		}
	}
	path, err := isv.GetArtifactPath(refParamName)
	if err != nil {
		err = fmt.Errorf("cannot reslove template[%s] as Artifact for %s[%s]: %v",
			isv.Component.GetType(), tpl[0], isv.runtimeName, err.Error())
		return "", err

	}
	if fieldType != FieldCondition && fieldType != FieldLoopArguemt {
		ps := []string{}
		for _, p := range strings.Split(path, ",") {
			ps = append(ps, strings.Join([]string{ArtMountDir, p}, "/"))
		}
		result = strings.Join(ps, ",")
	} else {
		var maxSize int
		if fieldType == FieldCondition {
			maxSize = ConditionArtifactMaxSize
		} else {
			maxSize = LoopArgumentArtifactMaxSize
		}

		result, err = GetArtifactContent(path, maxSize, isv.GlobalFsID, isv.logger)
		if err != nil {
			err = fmt.Errorf("failed to resolve template[%s] for %s[%s], because cannot read the content from artifact[%s]",
				isv.Component.GetType(), tpl[0], isv.runtimeName, refParamName)
			return "", err
		}
	}

	return result, nil
}

// resolveEnv: 将字符串中给定模板替换成具体值
func (isv *innerSolver) resolveTemplate(tplString string, fieldType string, forCache bool) (interface{}, error) {
	isv.logger.Debugf("begin to resolve template for %s[%s] with field[%s]", isv.Component.GetType(), isv.runtimeName, fieldType)
	tpls, err := fetchTemplate(tplString)
	if err != nil {
		return "", err
	}

	if len(tpls) == 0 {
		return tplString, err
	}

	if fieldType == FieldLoopArguemt && tplString != tpls[0][0] {
		err := fmt.Errorf("paraTemplate[%s] for %s[%s]'s loop_argument or condition field cannot join with other string",
			isv.Component.GetType(), tplString, isv.runtimeName)
		return "", err
	}
	for index := range tpls {
		tpl := tpls[index]

		var value interface{}
		var err error
		// 1、 尝试按照 Parameter / sysParameter tpl 的方式进行解析，此时如果tpl为 artifact 模板，肯定会报错
		value, err = isv.resloveParameterTemplate(tpl, fieldType)
		if err != nil {
			// env 字段中只允许引用parameter/sysParameter 模板
			if fieldType == FieldEnv {
				err = fmt.Errorf("cannot not resolve Template[%s] for %s[%s] in env field. "+
					"only support parameter template in env field", isv.Component.GetType(), tpl[0], isv.runtimeName)
				return "", err
			}

			// 2、尝试按照 artifact tpl 的方式进行解析
			value, err = isv.resolveArtifactTemplate(tpl, fieldType, forCache)
			if err != nil {
				err = fmt.Errorf("cannot not resolve Template[%s] for %s[%s] in %s field.",
					isv.Component.GetType(), tpl[0], isv.runtimeName, fieldType)
				return "", err
			}
		}

		if fieldType == FieldLoopArguemt {
			return value, nil
		} else {
			valueString := fmt.Sprintf("%v", value)
			tplString = strings.Replace(tplString, tpl[0], valueString, -1)
		}
	}

	return tplString, nil
}

// resolveEnv: 替换 env 字段的中的template
func (isv *innerSolver) resolveEnv() error {
	// 调用方需要保证此时的 component 是一个Step
	for name, value := range isv.Component.(*schema.WorkflowSourceStep).Env {
		// env 支持因为 平台内置参数模板，当前step的parameter模板
		newValue, err := isv.resolveTemplate(value, FieldEnv, false)
		if err != nil {
			return err
		}
		isv.Component.(*schema.WorkflowSourceStep).Env[name] = newValue.(string)
	}

	isv.logger.Infof("after resolve template, the env of %s[%s] is: %v", isv.Component.GetType(),
		isv.Component.GetName(), isv.Component.(*schema.WorkflowSourceStep).Env)
	return nil
}

func (isv *innerSolver) resolveCommand(forCache bool) error {
	// 调用方需要保证此时的 component 是一个Step
	command := isv.Component.(*schema.WorkflowSourceStep).Command
	newCommand, err := isv.resolveTemplate(command, FieldCommand, forCache)
	if err != nil {
		return err
	}

	isv.Component.(*schema.WorkflowSourceStep).Command = newCommand.(string)

	isv.logger.Infof("after resolve template, the command of %s[%s] is: %v",
		isv.Component.GetType(), isv.Component.GetName(), newCommand)
	return nil
}

func (isv *innerSolver) resolveCondition() error {
	condition := isv.Component.GetCondition()
	newCondition, err := isv.resolveTemplate(condition, FieldCondition, false)
	if err != nil {
		return err
	}

	isv.Component.UpdateCondition(newCondition.(string))
	isv.logger.Infof("after resolve template, the condition of %s[%s] is: %v",
		isv.Component.GetType(), isv.Component.GetName(), newCondition)
	return nil
}

func (isv *innerSolver) resolveLoopArugment() error {
	loopArgument := isv.Component.GetLoopArgument()
	if loopArgument == nil {
		return nil
	}

	loopArgumentString, ok := loopArgument.(string)
	if !ok {
		// 此时的 loopArgument 应该是 list，其中不应该有任何的模板，所以无需进行替换
		return nil
	}

	newLoopArgument, err := isv.resolveTemplate(loopArgumentString, FieldLoopArguemt, false)
	if err != nil {
		return err
	}

	// 校验 loopArgument 的内容是否合法
	// loopArgument 的值可以是以下几种：
	// 1. json list
	// 2. list
	if valueString, ok := newLoopArgument.(string); ok {
		var loopValue interface{}

		err := json.Unmarshal([]byte(valueString), &loopValue)
		if err != nil {
			err := fmt.Errorf("the value of loop_argument for %s[%s] should be an list or json list, and now is: %v",
				isv.Component.GetType(), isv.runtimeName, newLoopArgument)
			return err
		}
		newLoopArgument = loopValue
	}

	t := reflect.TypeOf(newLoopArgument)
	if t.Kind() != reflect.Slice {
		err := fmt.Errorf("the value of loop_argument for %s[%s] should be an list or json list",
			isv.Component.GetType(), isv.runtimeName)
		return err
	}

	isv.logger.Infof("after resolve template, the loop_arguemnt of %s[%s] is: %v",
		isv.Component.GetType(), isv.Component.GetName(), newLoopArgument)
	isv.Component.UpdateLoopArguemt(newLoopArgument)
	return nil
}

// 用于解析可能存在依赖关系的模版
// 如 parameter，artifact 字段，因为其有可能会引用上游节点，以及父节点（子节点） 的相关信息
type DependencySolver struct {
	*DagRuntime
}

func NewDependencySolver(dr *DagRuntime) *DependencySolver {
	return &DependencySolver{
		DagRuntime: dr,
	}
}

func (ds *DependencySolver) resolveParameterTemplate(tplString string, subComponentName string) (interface{}, error) {
	subFullName := ds.generateSubComponentFullName(subComponentName)
	subComponent := ds.DagRuntime.getworkflowSouceDag().EntryPoints[subComponentName]

	tpls, err := fetchTemplate(tplString)
	if err != nil {
		return "", err
	}

	if len(tpls) == 0 {
		return tplString, err
	}

	for index := range tpls {
		tpl := tpls[index]
		refComponentName, refvalue := parseTemplate(tpl[2])
		var value interface{}
		// 1、系统变量
		if refComponentName == "" {
			value, err = parseSysParamerterTemplate(refvalue, ds.sysParams)
			if err != nil {
				err = fmt.Errorf("cannot resolve template[%v] in %s[%s] as sysParam", subComponent.GetType(),
					tpl, subComponentName)
				return "", err
			}
		} else if refComponentName == PF_PARENT {
			// 2、 引用了父节点的输入parameter
			value, err = ds.component.GetParameterValue(refvalue)
			if err != nil {
				// 3、引用了父节点的 loop_arugment
				if refvalue == SysParamNamePFLoopArgument {
					// 这里不能直接从 drt 的 sysParams 取的原因为，需要保留类型信息。 子节点可能用该值作为自己 循环参数。
					value, err = ds.getPFLoopArgument()

					if err != nil {
						return "", err
					}
				} else {
					err := fmt.Errorf("cannot resolve the template[%s] for %s[%s]", subComponent.GetType(), tpl[0], subFullName)
					return "", err
				}
			}
		} else {
			// 4、 引用了上有节点的输出parameter
			value, err = ds.GetSubComponentParameterValue(refComponentName, refvalue)
			if err != nil {
				return "", err
			}
		}

		// 如果 tplstring 与 tpls[0] 不相等，说明其是模板拼接而成，此时会将参数值转化为字符串
		if tplString != tpls[0][0] {
			valueString := fmt.Sprintf("%v", value)
			tplString = strings.Replace(tplString, tpl[0], valueString, -1)
		} else {
			return value, nil
		}
	}

	return tplString, nil
}

func (ds *DependencySolver) resolveArtifactTemplate(tplString, componentName string) (string, error) {
	var cpType string

	subComponent, ok := ds.DagRuntime.getworkflowSouceDag().EntryPoints[componentName]
	if ok {
		cpType = subComponent.GetType()
	} else {
		cpType = "dag"
	}

	tpls, err := fetchTemplate(tplString)
	if err != nil {
		return "", err
	}

	if len(tpls) == 0 {
		// 除了 Step 节点的输出artifact，其余类型的artifact必须要引用其余节点的输出artifct，因此，其至少需要引用某一个模板
		err := fmt.Errorf("cannot find any template in the value of %s[%s]' artifact: %s",
			cpType, componentName, tplString)
		return "", err
	}

	refComponentName, refvalue := parseTemplate(tpls[0][2])
	var value string

	// 1、 引用了父节点的输入artifact
	if refComponentName == PF_PARENT {
		value, err = ds.component.GetArtifactPath(refvalue)
		if err != nil {
			return "", err
		}
	} else {
		// 2、 引用了上游节点或者子节点的输出 artifact
		value, err = ds.GetSubComponentArtifactPaths(refComponentName, refvalue)
		if err != nil {
			return "", err
		}
	}

	return value, nil
}

func (ds *DependencySolver) ResolveBeforeRun(subComponent schema.Component) error {
	// 1. 解析 parameter 模版
	componentName := subComponent.GetName()
	for name, value := range subComponent.GetParameters() {
		valueString, ok := value.(string)
		if !ok {
			continue
		} else {
			newValue, err := ds.resolveParameterTemplate(valueString, componentName)
			if err != nil {
				return err
			}

			subComponent.GetParameters()[name] = newValue
			ds.logger.Infof("after dependency solver, the value of parameter[%s] for %s[%s] is %v",
				subComponent.GetType(), componentName, name, newValue)
		}
	}

	// 2. 解析 aritfact 模版
	// 2.1 解析输入 artifact
	for name, value := range subComponent.GetArtifacts().Input {
		// 输入 artifact 必然引用其余节点的 输出artifact
		newValue, err := ds.resolveArtifactTemplate(value, componentName)
		if err != nil {
			err = fmt.Errorf("resolver artifact template[%s] for artifact[%s] of %s[%s] failed",
				subComponent.GetType(), value, name, componentName)
			return err
		}

		subComponent.GetArtifacts().Input[name] = newValue
		ds.logger.Infof("after dependency solver, the value of artifact[%s] for %s[%s] is %v",
			subComponent.GetType(), componentName, name, newValue)
	}
	// 输出artifact 无需解析： step 的输出artifact的中不会有模版，dag 的输出artifact 虽然有模版，但是需要在dag 所有的子节点都运行完成后才能解析。

	return nil
}

// ResolveAfterDone: 当dag运行成功时调用，解析输出artifact 模版
func (ds *DependencySolver) ResolveAfterDone() error {
	// 对于step 类型的节点，所有模版均在运行钱完成了解析
	_, ok := ds.component.(*schema.WorkflowSourceStep)
	if ok {
		return nil
	}

	for name, value := range ds.component.GetArtifacts().Output {
		newValue, err := ds.resolveArtifactTemplate(value, ds.DagRuntime.name)
		if err != nil {
			return err
		} else {
			ds.component.GetArtifacts().Output[name] = newValue
		}
	}

	return nil
}
