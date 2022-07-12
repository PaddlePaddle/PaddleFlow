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

package common

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/mitchellh/mapstructure"
)

type DictParam struct {
	Type    string
	Default interface{}
}

func (p *DictParam) From(origin interface{}) error {
	if err := mapstructure.Decode(origin, p); err != nil {
		return err
	}
	return nil
}

func InvalidParamTypeError(param interface{}, expected string) error {
	return fmt.Errorf("invalid type[%T] for param[%+v]. expect type[%s]", param, param, expected)
}

func UnsupportedParamTypeError(param interface{}, paramName string) error {
	return fmt.Errorf("type[%T] is not supported for param[%+v]", param, paramName)
}

func UnsupportedDictParamTypeError(unsupportedType string, paramName string, param interface{}) error {
	return fmt.Errorf("type[%s] is not supported for dict param[name: %s, value: %+v]", unsupportedType, paramName, param)
}

func UnsupportedPathParamError(param interface{}, paramName string) error {
	return fmt.Errorf("invalid path value[%s] in parameter[%s]", param, paramName)
}

func MismatchRegexError(param, regex string) error {
	return fmt.Errorf("param[%s] mismatches regex pattern[%s]", param, regex)
}

func StringsContain(items []string, item string) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

// 1.4.3后，Checker承担了Parameters的处理任务，如dict形式的param替换为实际值、请求的参数替换节点参数
type ComponentParamChecker struct {
	Components    map[string]schema.Component // 需要传入相关的所有 step，以判断依赖是否合法
	SysParams     map[string]string           // sysParams 的值和 step 相关，需要传入
	DisabledSteps []string                    // 被disabled的节点列表
	UseFs         bool                        // 表示是否挂载Fs
	CompTempletes map[string]schema.Component // 对应WorkflowSource中的Components的内容
}

func (s *ComponentParamChecker) checkDuplication(currentComponent string) error {
	/*
		currentStep内，parameter, input/output artifact是否有重复的参数名
		这里的重名检查是大小写不敏感的，如"param"和"ParAm"会被认为重名
		Args:
			currentComponent: 需要校验的Component名
	*/
	component, ok := s.Components[currentComponent]
	if !ok {
		return fmt.Errorf("check param reference failed: component [%s] not exist", currentComponent)
	}

	m := make(map[string]string)
	for paramName, _ := range component.GetParameters() {
		paramNameUpper := strings.ToUpper(paramName)
		_, ok := m[paramNameUpper]
		if ok {
			return fmt.Errorf("parameter name[%s] has already existed in params/artifacts of component[%s] (these names are case-insensitive)",
				paramName, currentComponent)
		} else {
			m[paramNameUpper] = ""
		}
	}

	for inputAtfName, _ := range component.GetArtifacts().Input {
		inputAtfNameUpper := strings.ToUpper(inputAtfName)
		_, ok := m[inputAtfNameUpper]
		if ok {
			return fmt.Errorf("inputAtf name[%s] has already existed in params/artifacts of component[%s] (these names are case-insensitive)",
				inputAtfName, currentComponent)
		} else {
			m[inputAtfNameUpper] = ""
		}
	}

	for outputAtfName, _ := range component.GetArtifacts().Output {
		outputAtfNameUpper := strings.ToUpper(outputAtfName)
		_, ok := m[outputAtfNameUpper]
		if ok {
			return fmt.Errorf("outputAtf name[%s] has already existed in params/artifacts of component[%s] (these names are case-insensitive)",
				outputAtfName, currentComponent)
		} else {
			m[outputAtfNameUpper] = ""
		}
	}

	return nil
}

func (s *ComponentParamChecker) Check(currentComponent string, isOuterComp bool) error {
	/*
		1. 如果没有用到Fs，不能用Fs相关系统参数，以及inputAtf，outputAtf机制
		2. 先检查参数名是否有重复（parameter，artifact）
		3. 然后校验当前步骤参数引用是否合法。引用规则有：
		- parameters 字段中变量可引用系统参数、及上游节点parameter
		- input artifacts 只能引用上游 outputArtifact
		- output artifact不会引用任何参数，值由平台自动生成
		- env 支持平台内置参数替换，上游step的parameter依赖替换，当前step的parameter替换
		- command 支持平台内置参数替换，上游step的parameter依赖替换，当前step的parameter替换，当前step内input artifact、当前step内output artifact替换
		4. 引用上游参数时，必须保证上游节点不在disabled列表中。
	*/
	err := s.checkDuplication(currentComponent)
	if err != nil {
		return err
	}

	component, ok := s.Components[currentComponent]
	if !ok {
		return fmt.Errorf("check param reference failed: component [%s] not exist", currentComponent)
	}

	// Reference节点检查
	if err := s.checkReference(component); err != nil {
		return fmt.Errorf("check reference failed, only outer components can be refered, error: %s", err.Error())
	}

	// 1. parameter 校验
	// parameter不需要将dict类型的参数默认值提取出来，因为会在StepParamResolver中做了
	// 另外命令行参数替换默认值，会在workflow.go中完成
	for paramName, paramVal := range component.GetParameters() {
		if err = s.checkName(currentComponent, FieldParameters, paramName); err != nil {
			return err
		}
		realVal, err := s.solveParamValue(currentComponent, paramName, paramVal, FieldParameters)
		if err != nil {
			return err
		}
		component.GetParameters()[paramName] = realVal
	}

	// 2. input artifact 校验
	for inputAtfName, inputAtfVal := range component.GetArtifacts().Input {
		if err = s.checkName(currentComponent, FieldInputArtifacts, inputAtfName); err != nil {
			return err
		}

		if !isOuterComp {
			variableChecker := VariableChecker{}
			err = variableChecker.CheckRefUpstreamStep(inputAtfVal)
			if err != nil {
				return fmt.Errorf("check input artifact [%s] in component[%s] failed: %s", inputAtfName, currentComponent, err.Error())
			}

			_, err = s.solveParamValue(currentComponent, inputAtfName, inputAtfVal, FieldInputArtifacts)
			if err != nil {
				return err
			}
		}
	}

	// 3. output artifacts 由平台生成路径，所以在校验时，不会对其值进行校验（即使非空迟早也会被替换）
	// 如果不使用Fs，不能定义outputAtf。
	if !s.UseFs && len(component.GetArtifacts().Output) > 0 {
		return fmt.Errorf("cannot define artifact in component[%s] with no Fs mounted", currentComponent)
	}
	if dag, ok := component.(*schema.WorkflowSourceDag); ok {
		for outAtfName, outArtValue := range dag.Artifacts.Output {
			if err = s.checkName(currentComponent, FieldOutputArtifacts, outAtfName); err != nil {
				return err
			}
			variableChecker := VariableChecker{}
			// 虽然这里不是引用上游节点，而是子节点，但是规则相同，因此可以复用
			err = variableChecker.CheckRefUpstreamStep(outArtValue)
			if err != nil {
				return fmt.Errorf("check output artifact value [%s] in dag[%s] failed: %s", outArtValue, currentComponent, err.Error())
			}

			_, err = s.solveParamValue(currentComponent, outAtfName, outArtValue, FieldOutputArtifacts)
			if err != nil {
				return err
			}
		}
	} else {
		// step 的 OutputArtifact 没有 value
		for outAtfName, _ := range component.GetArtifacts().Output {
			if err = s.checkName(currentComponent, FieldOutputArtifacts, outAtfName); err != nil {
				return err
			}
		}
	}

	// 只有step才有env和command

	if step, ok := component.(*schema.WorkflowSourceStep); ok {
		// 4. env 校验/更新
		for envName, envVal := range step.Env {
			if err = s.checkName(currentComponent, FieldEnv, envName); err != nil {
				return err
			}
			_, err = s.solveParamValue(currentComponent, envName, envVal, FieldEnv)
			if err != nil {
				return err
			}
		}

		// 5. command 校验/更新
		_, err = s.solveParamValue(currentComponent, "command", step.Command, FieldCommand)
		if err != nil {
			return err
		}
	}
	return nil
}

// 对reference节点进行检查
func (s *ComponentParamChecker) checkReference(comp schema.Component) error {
	// Reference节点的parameters的keys要是被引用节点的子集
	// Reference节点的input artifact的keys要和被引用节点的一样
	if step, ok := comp.(*schema.WorkflowSourceStep); ok {
		refName := step.Reference.Component
		if refName != "" {
			if len(step.Artifacts.Output) > 0 || len(step.Command) > 0 || len(step.Condition) > 0 ||
				len(step.DockerEnv) > 0 || len(step.Env) > 0 || step.LoopArgument != nil ||
				len(step.Cache.FsScope) > 0 || len(step.Cache.MaxExpiredTime) > 0 || step.Cache.Enable ||
				len(step.FsMount) > 0 {
				return fmt.Errorf("reference step can only have deps, parameters, input artifacts, reference")
			}

			template, ok := s.CompTempletes[refName]
			if !ok {
				return fmt.Errorf("no component named %s", refName)
			}

			// reference节点的parameters的keys要是被引用节点的子集
			for paramName, param := range comp.GetParameters() {
				referedParam, ok := template.GetParameters()[paramName]
				if !ok {
					return fmt.Errorf("parameters in step with reference must be in refered component")
				}
				// 如果对应的被引用节点的参数为dict形式，则Reference节点的参数类型需要符合dict中Type的要求
				switch referedParam := referedParam.(type) {
				case map[string]interface{}:
					logger.Logger().Infof("debug: dict param check in")
					refDictParam := DictParam{}
					if err := refDictParam.From(referedParam); err != nil {
						return fmt.Errorf("invalid dict parameter[%v]", referedParam)
					}
					if _, err := CheckDictParam(refDictParam, paramName, param); err != nil {
						return fmt.Errorf("parameters in step with reference check dict param in refered param failed, error: %s", err.Error())
					}
				default:
					logger.Logger().Infof("debug: dict param check type is : %s, name is : %s", reflect.TypeOf(referedParam), paramName)
				}
			}

			// Reference节点的input artifact的keys要和被引用节点的一样
			for atfName := range comp.GetArtifacts().Input {
				if _, ok := template.GetArtifacts().Input[atfName]; !ok {
					return fmt.Errorf("input artifact in step with reference must be in refered component")
				}
			}

			if len(comp.GetArtifacts().Input) != len(template.GetArtifacts().Input) {
				return fmt.Errorf("input artifact in refered component should all used by reference component")
			}
		}
	}
	return nil
}

func (s *ComponentParamChecker) checkName(comp, fieldType, name string) error {
	variableChecker := VariableChecker{}
	err := variableChecker.CheckVarName(name)
	if err != nil {
		return fmt.Errorf("check %s[%s] in component[%s] failed: %s", fieldType, name, comp, err.Error())
	}
	return nil
}

// 该函数主要处理parameter, command, env，input artifact四类参数
// schema中已经限制了input artifact，command, env只能为string，此处再判断类型用于兜底
func (s *ComponentParamChecker) solveParamValue(compName string, paramName string, param interface{}, fieldType string) (interface{}, error) {
	if fieldType == FieldCommand || fieldType == FieldEnv || fieldType == FieldInputArtifacts {
		_, ok := param.(string)
		if !ok {
			return nil, fmt.Errorf("value of %s[%s] invalid: should be string", fieldType, paramName)
		}
	}

	// 参数值检查
	switch param := param.(type) {
	case float32, float64, int, int64:
		return param, nil
	case string:
		if err := s.resolveRefParam(compName, param, fieldType); err != nil {
			return nil, err
		} else {
			return param, nil
		}
	case map[string]interface{}:
		dictParam := DictParam{}
		if err := dictParam.From(param); err != nil {
			return nil, fmt.Errorf("invalid dict parameter[%s]", param)
		}
		return CheckDictParam(dictParam, paramName, nil)
	default:
		return nil, UnsupportedParamTypeError(param, paramName)
	}
}

// resolveRefParam 解析引用参数
// parameters 字段中变量可引用系统参数、及上游节点parameter
// input artifacts 只能引用上游 outputArtifact
// env 支持平台内置参数替换，上游step的parameter依赖替换，当前step的parameter替换
// command 支持平台内置参数替换，上游step的parameter依赖替换，当前step的parameter替换，当前step内input artifact、当前step内output artifact替换
func (s *ComponentParamChecker) resolveRefParam(componentName, param, fieldType string) error {
	// regular expression must match case {{ xxx }} like {{ <step-name>.<param_name> }} or {{ PS_RUN_ID }}
	pattern := RegExpIncludingTpl
	reg := regexp.MustCompile(pattern)
	matches := reg.FindAllStringSubmatch(param, -1)
	for _, row := range matches {
		if len(row) != 4 {
			return MismatchRegexError(param, pattern)
		}
		refList := ParseParamName(row[2])
		refComponent, refParamName := "", ""
		if len(refList) > 1 {
			refComponent, refParamName = refList[0], refList[1]
		} else {
			refParamName = refList[0]
		}

		if len(refComponent) == 0 {
			// 分别替换系统参数，如{{PF_RUN_ID}}；当前step parameter；当前step的input artifact；当前step的output artifact
			// 只有param，env，command三类变量需要处理

			var ok bool
			_, ok = s.SysParams[refParamName]
			if !ok {
				if fieldType == FieldParameters {
					return fmt.Errorf("unsupported SysParamName[%s] for param[%s] of filedType[%s]", refParamName, param, fieldType)
				}
				_, ok := s.Components[componentName].GetParameters()[refParamName]
				if !ok {
					if fieldType == FieldEnv {
						return fmt.Errorf("unsupported RefParamName[%s] for param[%s] of filedType[%s]", refParamName, param, fieldType)
					}

					// 处理command逻辑，command 可以引用 system parameter + step 内的 parameter + artifact
					_, ok = s.Components[componentName].GetArtifacts().Input[refParamName]
					if !ok {
						_, ok = s.Components[componentName].GetArtifacts().Output[refParamName]
						if !ok {
							return fmt.Errorf("unsupported RefParamName[%s] for param[%s] of filedType[%s]", refParamName, param, fieldType)
						}
					}
				}
			}
		} else {
			err := s.refParamExist(componentName, refComponent, refParamName, fieldType)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ParseParamName(paramName string) []string {
	paramStrList := strings.Split(paramName, ".")
	return paramStrList
}

func (s *ComponentParamChecker) refParamExist(currentCompName, refCompName, refParamName, fieldType string) error {
	/*
		env, command只能引用当前节点的param, input/output artifacts
		param 只做上游step的 param 依赖替换
		input artifact 只做上游step的output artifact 依赖替换
		PF_PARENT 为引用父节点的
	*/
	if refCompName == PF_PARENT {
		if len(strings.Split(currentCompName, ".")) < 2 {
			return fmt.Errorf("PF_PARENT should used by a child component")
		}
		return nil
	}
	curComponent := s.Components[currentCompName]
	// s.DisabledSteps和s.Components保存的是完整节点名称，即从跟节点到当前节点的完整路径，如A.BB.CCC
	// 因此需要结合currentCompName（完整节点名称），对refCompName（相对节点名称）进行拓展
	var absoluteRefCompName string
	switch fieldType {
	case FieldOutputArtifacts:
		if curComponent.GetType() != "dag" {
			return fmt.Errorf("only dag can use output aritfacts of subComponents")
		}
		absoluteRefCompName = currentCompName + "." + refCompName
	default:
		if !StringsContain(curComponent.GetDeps(), refCompName) {
			return fmt.Errorf("invalid reference param {{ %s.%s }} in component[%s]: component[%s] not in deps", refCompName, refParamName, currentCompName, refCompName)
		}

		refCompNameList := strings.Split(currentCompName, ".")
		if len(refCompNameList) > 1 {
			absoluteRefCompName = strings.Join(refCompNameList[:len(refCompNameList)-1], ".") + "." + refCompName
		} else {
			absoluteRefCompName = refCompName
		}
	}

	refComponent, ok := s.Components[absoluteRefCompName]
	if !ok {
		return fmt.Errorf("invalid reference param {{ %s.%s }} in component[%s]: component[%s] not exist", refCompName, refParamName, currentCompName, absoluteRefCompName)
	}

	// 如果refComponent是reference节点（设置了reference.component）的话，则需要找到对应的Component再进行下面的检查
	// 在此之前已经做过检查，确保：1. reference节点的parameter为被引用Component的子集，2. reference节点的input artifacts与被引用Component相等
	if step, ok := refComponent.(*schema.WorkflowSourceStep); ok && step.Reference.Component != "" {
		for {
			refTemp, ok := s.CompTempletes[step.Reference.Component]
			if !ok {
				return fmt.Errorf("reference.component [%s] in component[%s] is not exist", step.Reference.Component, step.Name)
			}
			if tempStep, ok := refTemp.(*schema.WorkflowSourceStep); ok && tempStep.Reference.Component != "" {
				step = tempStep
			} else {
				refComponent = refTemp
				break
			}
		}
	}

	switch fieldType {
	case FieldInputArtifacts:
		fallthrough
	case FieldOutputArtifacts:
		// 这里的Output Artfacts仅适用于Dag
		if refComponent.GetCondition() != "" {
			return fmt.Errorf("invalid reference param {{ %s.%s }} in component[%s]: component[%s] has condition so its param and output artifacts can't be refered",
				refCompName, refParamName, currentCompName, refCompName)
		}
		if _, ok := refComponent.GetArtifacts().Output[refParamName]; !ok {
			return fmt.Errorf("invalid reference param {{ %s.%s }} in component[%s]: output artifact[%s] not exist", refCompName, refParamName, currentCompName, refParamName)
		}
	case FieldParameters:
		if _, ok := refComponent.GetParameters()[refParamName]; !ok {
			return fmt.Errorf("invalid reference param {{ %s.%s }} in component[%s]: parameter[%s] not exist", refCompName, refParamName, currentCompName, refParamName)
		}
	default:
		return fmt.Errorf("component [%s] refer [%s.%s] invalid, only parameters can use upstream parameters and only input artifacts can use upstream output artifacts", currentCompName, refCompName, refParamName)
	}

	return nil
}

func CheckDictParam(dict DictParam, paramName string, realVal interface{}) (interface{}, error) {
	if dict.Type == "" {
		return nil, UnsupportedDictParamTypeError(dict.Type, paramName, dict)
	}
	if realVal == nil || realVal == "" {
		if dict.Default == nil || dict.Default == "" {
			return nil, fmt.Errorf("invalid value[%v] in dict param[name: %s, value: %+v]", dict.Default, paramName, dict)
		}
		realVal = dict.Default
	}

	switch dict.Type {
	case ParamTypeString:
		_, ok := realVal.(string)
		if ok {
			return realVal, nil
		} else {
			return nil, InvalidParamTypeError(realVal, ParamTypeString)
		}
	case ParamTypeFloat:
		_, ok1 := realVal.(float32)
		_, ok2 := realVal.(float64)
		_, ok3 := realVal.(int32)
		_, ok4 := realVal.(int64)
		_, ok5 := realVal.(int)
		if ok1 || ok2 || ok3 || ok4 || ok5 {
			return realVal, nil
		}
		return nil, InvalidParamTypeError(realVal, ParamTypeFloat)
	case ParamTypeInt:
		_, ok1 := realVal.(int32)
		_, ok2 := realVal.(int64)
		_, ok3 := realVal.(int)
		if ok1 || ok2 || ok3 {
			return realVal, nil
		}
		return nil, InvalidParamTypeError(realVal, ParamTypeInt)
	case ParamTypeList:
		_, ok1 := realVal.([]float32)
		_, ok2 := realVal.([]float64)
		_, ok3 := realVal.([]int32)
		_, ok4 := realVal.([]int64)
		_, ok5 := realVal.([]int)
		_, ok6 := realVal.([]string)
		if ok1 || ok2 || ok3 || ok4 || ok5 || ok6 {
			return realVal, nil
		}
	case ParamTypePath:
		realValStr, ok := realVal.(string)
		if !ok {
			return nil, InvalidParamTypeError(realVal, ParamTypePath)
		}
		pattern := `^[.a-zA-Z0-9/_-]+$`
		reg := regexp.MustCompile(pattern)
		matched := reg.Match([]byte(realValStr))
		if !matched {
			return nil, UnsupportedPathParamError(realVal, paramName)
		}
		return realVal, nil
	default:
		return nil, UnsupportedDictParamTypeError(dict.Type, paramName, dict)
	}
	return nil, fmt.Errorf("check dict param failed in end")
}
