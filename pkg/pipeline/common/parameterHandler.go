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
	"regexp"
	"strings"

	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	. "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
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

/*
* StepParamSolver
* 用于校验以及替换所有WorkflowSourceStep中的参数，分四种情况：
* 1. 只校验不替换
* 2. 替换参数，用于计算Cache fingerprint
* 3. cache命中后，利用cache替换参数
* 4. 替换参数，用于job运行
 */
type StepParamSolver struct {
	stepParamChecker    StepParamChecker
	jobs                map[string]Job // 需要传入相关的所有step，以进行替换（不能从WorkflowSourceStep获取，因为output artifact是动态生成的，如果服务重启后，step的WorkflowSourceStep中output artifact路径会重置为空字符串，需要从Job里面才能拿到）
	forCacheFingerprint bool           // 替换参数是为了计算cache fingerorint，还是用于节点运行（还有一种情况是cache命中后，利用cache的output artifact路径替换参数，这种情况通过独立函数完成，不走Solve函数，因此不由这个变量控制）
	pplName             string
	runID               string
	fsID                string
	logger              *log.Entry
}

func NewStepParamSolver(
	steps map[string]*schema.WorkflowSourceStep,
	sysParams map[string]string,
	jobs map[string]Job,
	forCacheFingerprint bool,
	pplName string,
	runID string,
	fsID string,
	logger *log.Entry) StepParamSolver {

	useFs := true
	if sysParams[SysParamNamePFFsID] == "" {
		useFs = false
	}

	stepParamChecker := StepParamChecker{
		Components: steps,
		SysParams:  sysParams,
		UseFs:      useFs,
	}

	stepParamSolver := StepParamSolver{
		stepParamChecker:    stepParamChecker,
		jobs:                jobs,
		forCacheFingerprint: forCacheFingerprint,
		pplName:             pplName,
		runID:               runID,
		fsID:                fsID,
		logger:              logger,
	}

	return stepParamSolver
}

func (s *StepParamSolver) getSysParams() map[string]string {
	return s.stepParamChecker.getSysParams()
}

func (s *StepParamSolver) Solve(currentStep string, cacheOutputArtifacts map[string]string) error {
	s.stepParamChecker.Check(currentStep)

	step, ok := s.stepParamChecker.getWorkflowSourceStep(currentStep)
	if !ok {
		return fmt.Errorf("check param reference failed: %s no exists", currentStep)
	}

	// 1. parameter 校验/更新
	// parameter 必须最先被更新，artifact env command 可能会引用 step 内的param
	for paramName, paramVal := range step.Parameters {
		realVal, err := s.resolveParamValue(currentStep, paramName, paramVal, FieldParameters)
		if err != nil {
			return err
		}
		step.Parameters[paramName] = realVal
	}

	// 2. artifact 更新
	// artifact 必须先更新，command 可能会引用 step 内的 artifact
	// input artifacts 只能引用上游 outputArtifact; 因此如果没有上游节点，就不能定义input artifact
	for inputAtfName, inputAtfVal := range step.Artifacts.Input {
		realVal, err := s.resolveParamValue(currentStep, inputAtfName, inputAtfVal, FieldInputArtifacts)
		if err != nil {
			return err
		}
		step.Artifacts.Input[inputAtfName] = fmt.Sprintf("%v", realVal)
	}

	// 3. output artifacts 由平台生成路径，所以在校验时，不会对其值进行校验（即使非空迟早也会被替换）
	for outAtfName, _ := range step.Artifacts.Output {
		realVal, err := s.solveOutputArtifactValue(currentStep, outAtfName, cacheOutputArtifacts)
		if err != nil {
			return err
		}
		step.Artifacts.Output[outAtfName] = fmt.Sprintf("%v", realVal)
	}

	// 4. env 校验/更新
	for envName, envVal := range step.Env {
		realVal, err := s.resolveParamValue(currentStep, envName, envVal, FieldEnv)
		if err != nil {
			return err
		}

		step.Env[envName] = fmt.Sprintf("%v", realVal)
	}

	// 5. command 校验/更新
	realVal, err := s.resolveParamValue(currentStep, "command", step.Command, FieldCommand)
	if err != nil {
		return err
	}
	step.Command = fmt.Sprintf("%v", realVal)

	return nil
}

func (s *StepParamSolver) solveOutputArtifactValue(stepName string, paramName string, cacheOutputArtifacts map[string]string) (string, error) {
	/*
		存在三种场景:
		1. 用于计算Cache fingerprint: 此时output atf应该替换为为空字符串
		2. cache命中后更新参数值: 此时cacheOutputArtifacts非空，使用该map内的output atf值进行替换
		3. cache命中失败后, 用于job运行: output atf值由平台生成，路径格式为 ./pipeline/{{PF_RUNID}}/{{pplName}}/{{stepName}}/{{outputAtfName}}
	*/
	if s.forCacheFingerprint {
		return "", nil
	}

	var result string
	var ok bool
	if cacheOutputArtifacts != nil {
		// output artifacts 由参数传入，直接覆盖本step内对应output artifact路径即可
		result, ok = cacheOutputArtifacts[paramName]
		if !ok {
			return "", fmt.Errorf("replace cache output artifact[%s] for step[%s] failed, output artifact not exist in cached data", paramName, stepName)
		}
	} else {
		resourceHandler, err := NewResourceHandler(s.runID, s.fsID, s.logger)
		if err != nil {
			newErr := fmt.Errorf("generate path for output artifact[%s] in step[%s] with pplname[%s] pplrunid[%s] failed: %s",
				paramName, stepName, s.pplName, resourceHandler.pplRunID, err.Error())
			return "", newErr
		}

		result, err = resourceHandler.generateOutAtfPath(s.pplName, stepName, paramName, true)
		if err != nil {
			return "", err
		}
	}

	return result, nil
}

// 该函数主要处理parameter， command， env三类参数
// command都只能是字符串, schema中已经限制了command， env只能为string，此处再判断类型用于兜底
func (s *StepParamSolver) resolveParamValue(step string, paramName string, param interface{}, fieldType string) (interface{}, error) {
	switch param.(type) {
	case float32, float64, int:
		return param, nil
	case string:
		return s.resolveRefParam(step, param.(string), fieldType)
	case map[interface{}]interface{}:
		dictParam := DictParam{}
		if err := dictParam.From(param); err != nil {
			return "", fmt.Errorf("invalid dict parameter[%s]", param)
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
func (s *StepParamSolver) resolveRefParam(stepName, param, fieldType string) (interface{}, error) {
	// regular expression must match case {{ xxx }} like {{ <step-name>.<param_name> }} or {{ PS_RUN_ID }}
	pattern := RegExpIncludingTpl
	reg := regexp.MustCompile(pattern)

	matches := reg.FindAllStringSubmatch(param, -1)
	result := param
	for index := range matches {
		row := matches[index]
		if len(row) != 4 {
			err := MismatchRegexError(param, pattern)
			return "", err
		}
		var tmpVal string
		refStepName, refParamName := ParseParamName(row[2])
		if len(refStepName) == 0 {
			// 分别替换系统参数，如{{PF_RUN_ID}}；当前step parameter；当前step的input artifact；当前step的output artifact
			// 只有param，env，command三类变量需要处理

			currentStep, ok := s.stepParamChecker.getWorkflowSourceStep(stepName)
			if !ok {
				return nil, fmt.Errorf("check param reference[%s] failed: %s no exists", param, stepName)
			}

			tmpVal, ok = s.stepParamChecker.getSysParam(refParamName)
			if !ok {
				if fieldType == FieldParameters {
					return "", fmt.Errorf("unsupported SysParamName[%s] for param[%s] of filedType[%s]", refParamName, param, fieldType)
				}
				tmpVal2, ok := currentStep.Parameters[refParamName]
				if !ok {
					if fieldType == FieldEnv {
						return "", fmt.Errorf("unsupported RefParamName[%s] for param[%s] of filedType[%s]", refParamName, param, fieldType)
					}

					// 处理command逻辑，command 可以引用 system parameter + step 内的 parameter + artifact
					tmpVal, ok = currentStep.Artifacts.Input[refParamName]
					if !ok {
						tmpVal, ok = currentStep.Artifacts.Output[refParamName]
						if !ok {
							return "", fmt.Errorf("unsupported RefParamName[%s] for param[%s] of filedType[%s]", refParamName, param, fieldType)
						}

						// 如果替换参数是用于计算fingerprint的话，不会替换当前节点output artifact的值
						// 目前只有command中会替换当前节点output artifact的值
						if s.forCacheFingerprint {
							tmpVal = row[0]
						}
					}
				} else {
					tmpVal = fmt.Sprintf("%v", tmpVal2)
				}
			}
		} else {
			tmpVal2, err := s.refParamExist(stepName, refStepName, refParamName, fieldType)
			if err != nil {
				return nil, err
			}
			tmpVal = fmt.Sprintf("%v", tmpVal2)
		}

		result = strings.Replace(result, row[0], tmpVal, -1)
	}
	return result, nil
}

func (s *StepParamSolver) refParamExist(currentStep, refStep, refParamName, fieldType string) (interface{}, error) {
	// 只有param，env，command，input artifact 四类变量需要处理
	// param，env，command 只做上游step的parameter 依赖替换
	// input artifact 只做上游step的output artifact 依赖替换
	ref := s.jobs[refStep].(*PaddleFlowJob)
	switch fieldType {
	case FieldInputArtifacts:
		if refParamVal, ok := ref.Artifacts.Output[refParamName]; ok {
			return refParamVal, nil
		}
	default:
		if refParamVal, ok := ref.Parameters[refParamName]; ok {
			return refParamVal, nil
		}
	}
	return nil, fmt.Errorf("invalid %s reference {{ %s.%s }} in step %s", fieldType, refStep, refParamName, currentStep)
}

type StepParamChecker struct {
	Components    map[string]schema.Component // 需要传入相关的所有 step，以判断依赖是否合法
	SysParams     map[string]string           // sysParams 的值和 step 相关，需要传入
	DisabledSteps []string                    // 被disabled的节点列表
	UseFs         bool                        // 表示是否挂载Fs
	CompTempletes map[string]schema.Component // 对应WorkflowSource中的Components的内容
}

func (s *StepParamChecker) getWorkflowSourceStep(currentStep string) (*schema.WorkflowSourceStep, bool) {
	step, ok := s.Components[currentStep]
	return step, ok
}

func (s *StepParamChecker) getSysParam(paramName string) (string, bool) {
	param, ok := s.SysParams[paramName]
	return param, ok
}

func (s *StepParamChecker) getSysParams() map[string]string {
	return s.SysParams
}

func (s *StepParamChecker) checkDuplication(currentComponent string) error {
	/*
		currentStep内，parameter, input/output artifact是否有重复的参数名
		这里的重名检查是大小写不敏感的，如"param"和"ParAm"会被认为重名
		Args:
			currentComponent: 需要校验的Component名
	*/
	component, ok := s.Components[currentComponent]
	if !ok {
		return fmt.Errorf("check param reference failed: component %s not exist", currentComponent)
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
			return fmt.Errorf("outputAtf name[%s] has already existed in params/artifacts of step[%s] (these names are case-insensitive)",
				outputAtfName, currentComponent)
		} else {
			m[outputAtfNameUpper] = ""
		}
	}

	return nil
}

func (s *StepParamChecker) Check(currentComponent string) error {
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
		return fmt.Errorf("check param reference failed: component %s not exist", currentComponent)
	}

	// 1. parameter 校验
	// parameter不需要将dict类型的参数默认值提取出来，因为会在StepParamResolver中做了
	// 另外命令行参数替换默认值，会在workflow.go中完成
	for paramName, paramVal := range component.GetParameters() {
		if err = s.checkName(currentComponent, FieldParameters, paramName); err != nil {
			return err
		}
		err = s.checkParamValue(currentComponent, paramName, paramVal, FieldParameters)
		if err != nil {
			return err
		}
	}

	// 2. input artifact 校验
	for inputAtfName, inputAtfVal := range component.GetArtifacts().Input {
		if err = s.checkName(currentComponent, FieldInputArtifacts, inputAtfName); err != nil {
			return err
		}

		variableChecker := VariableChecker{}
		err = variableChecker.CheckRefUpstreamStep(inputAtfVal)
		if err != nil {
			return fmt.Errorf("check input artifact [%s] in step[%s] failed: %s", inputAtfName, currentComponent, err.Error())
		}

		err = s.checkParamValue(currentComponent, inputAtfName, inputAtfVal, FieldInputArtifacts)
		if err != nil {
			return err
		}
	}

	// 3. output artifacts 由平台生成路径，所以在校验时，不会对其值进行校验（即使非空迟早也会被替换）
	// 如果不使用Fs，不能定义outputAtf。
	if !s.UseFs && len(component.GetArtifacts().Output) > 0 {
		return fmt.Errorf("cannot define artifact in step[%s] with no Fs mounted", currentComponent)
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
				return fmt.Errorf("check input artifact [%s] in step[%s] failed: %s", outArtValue, currentComponent, err.Error())
			}

			err = s.checkParamValue(currentComponent, outAtfName, outArtValue, FieldOutputArtifacts)
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
			err = s.checkParamValue(currentComponent, envName, envVal, FieldEnv)
			if err != nil {
				return err
			}
		}

		// 5. command 校验/更新
		err = s.checkParamValue(currentComponent, "command", step.Command, FieldCommand)
		if err != nil {
			return err
		}

		// 6. reference 校验
		if step.Reference != "" {
			if err := s.CheckRefComponent(currentComponent); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *StepParamChecker) checkName(step, fieldType, name string) error {
	variableChecker := VariableChecker{}
	err := variableChecker.CheckVarName(name)
	if err != nil {
		return fmt.Errorf("check %s[%s] in step[%s] failed: %s", fieldType, name, step, err.Error())
	}
	return nil
}

// 该函数主要处理parameter, command, env，input artifact四类参数
// schema中已经限制了input artifact，command, env只能为string，此处再判断类型用于兜底
func (s *StepParamChecker) checkParamValue(compName string, paramName string, param interface{}, fieldType string) error {
	if fieldType == FieldCommand || fieldType == FieldEnv || fieldType == FieldInputArtifacts {
		_, ok := param.(string)
		if !ok {
			return fmt.Errorf("value of %s[%s] invalid: should be string", fieldType, paramName)
		}
	}

	// Reference节点相关检查
	comp, ok := s.Components[compName]
	// 兜底检查
	if !ok {
		return fmt.Errorf("no component in EntryPoints named %s", compName)
	}
	if step, ok := comp.(*schema.WorkflowSourceStep); ok {
		if step.Reference != "" {
			// 对reference节点进行检查
			template, ok := s.CompTempletes[step.Reference]
			if !ok {
				return fmt.Errorf("no component named %s", compName)
			}

			switch fieldType {
			case FieldParameters:
				// 在reference 节点中定义的parameters 需要是其引用的component的 parameters 的子集
				referedParam, ok := template.GetParameters()[paramName]
				if !ok {
					return fmt.Errorf("parameters in step with reference must be in refered component")
				}
				// 如果 component 中的parameter 为dict 形式，在reference 节点中对应的parameter需要满足类型要求
				switch referedParam.(type) {
				case map[interface{}]interface{}:
					dictParam := DictParam{}
					if err := dictParam.From(referedParam); err != nil {
						return fmt.Errorf("invalid dict parameter[%s]", param)
					}
					if _, err := CheckDictParam(dictParam, step.Reference, param); err != nil {
						return fmt.Errorf("parameters in step with reference check dict param in refered param failed, error: %s", err.Error())
					}
				}
			case FieldInputArtifacts:
				// 在reference 节点中定义的 input artifacts 需要是其引用的component的 input artifacts 的子集
				if _, ok := template.GetArtifacts().Input[paramName]; !ok {
					return fmt.Errorf("input artifact in step with reference must be in refered component")
				}
			}
		}
	}

	// 参数值检查
	switch param.(type) {
	case float32, float64, int:
		return nil
	case string:
		return s.resolveRefParam(compName, param.(string), fieldType)
	case map[interface{}]interface{}:
		dictParam := DictParam{}
		if err := dictParam.From(param); err != nil {
			return fmt.Errorf("invalid dict parameter[%s]", param)
		}
		_, err := CheckDictParam(dictParam, paramName, nil)
		return err
	default:
		return UnsupportedParamTypeError(param, paramName)
	}
}

// resolveRefParam 解析引用参数
// parameters 字段中变量可引用系统参数、及上游节点parameter
// input artifacts 只能引用上游 outputArtifact
// env 支持平台内置参数替换，上游step的parameter依赖替换，当前step的parameter替换
// command 支持平台内置参数替换，上游step的parameter依赖替换，当前step的parameter替换，当前step内input artifact、当前step内output artifact替换
func (s *StepParamChecker) resolveRefParam(componentName, param, fieldType string) error {
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
			if !s.UseFs && (refParamName == SysParamNamePFFsID || refParamName == SysParamNamePFFsName) {
				return fmt.Errorf("cannot use sysParam[%s] template in step[%s] for pipeline run with no Fs mounted", refParamName, componentName)
			}

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

func (s *StepParamChecker) refParamExist(currentCompName, refCompName, refParamName, fieldType string) error {
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
		absoluteRefCompName = currentCompName + "." + refParamName
	default:
		if !StringsContain(curComponent.GetDeps(), refCompName) {
			return fmt.Errorf("invalid reference param {{ %s.%s }} in step[%s]: step[%s] not in deps", refCompName, refParamName, currentCompName, refCompName)
		}

		refCompNameList := strings.Split(currentCompName, ".")
		if len(refCompNameList) > 1 {
			absoluteRefCompName = strings.Join(refCompNameList[:len(refCompNameList)-1], ".") + "." + refCompName
		} else {
			absoluteRefCompName = refParamName
		}
	}
	// 检查引用的节点是否被disabled
	for _, disableStepName := range s.DisabledSteps {
		if absoluteRefCompName == disableStepName {
			return fmt.Errorf("invalid reference param {{ %s.%s }} in step[%s]: step[%s] is disabled", refCompName, refParamName, currentCompName, refCompName)
		}
	}

	refComponent, ok := s.Components[absoluteRefCompName]
	if !ok {
		return fmt.Errorf("invalid reference param {{ %s.%s }} in step[%s]: step[%s] not exist", refCompName, refParamName, currentCompName, refCompName)
	}

	switch fieldType {
	case FieldInputArtifacts:
		fallthrough
	case FieldOutputArtifacts:
		if _, ok := refComponent.GetArtifacts().Output[refParamName]; !ok {
			return fmt.Errorf("invalid reference param {{ %s.%s }} in step[%s]: output artifact[%s] not exist", refCompName, refParamName, currentCompName, refParamName)
		}
	case FieldParameters:
		if _, ok := refComponent.GetParameters()[refParamName]; !ok {
			return fmt.Errorf("invalid reference param {{ %s.%s }} in step[%s]: parameter[%s] not exist", refCompName, refParamName, currentCompName, refParamName)
		}
	default:
		return fmt.Errorf("only parameters can use upstream parameters and only input artifacts can use upstream output artifacts")
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
		pattern := `^[a-zA-Z0-9/_-]+$`
		reg := regexp.MustCompile(pattern)
		matched := reg.Match([]byte(realValStr))
		if !matched {
			return nil, UnsupportedPathParamError(realVal, paramName)
		}
		return realVal, nil
	default:
		return nil, UnsupportedDictParamTypeError(dict.Type, paramName, dict)
	}
}

func (s *StepParamChecker) CheckRefComponent(compName string) error {
	component, ok := s.Components[compName]
	if !ok {
		fmt.Errorf("no component named %s", compName)
	}

	if step, ok := component.(*schema.WorkflowSourceStep); ok {
		if len(step.Artifacts.Output) > 0 || len(step.Command) > 0 || len(step.Condition) > 0 ||
			len(step.DockerEnv) > 0 || len(step.Env) > 0 || step.LoopArgument != nil ||
			len(step.Cache.FsScope) > 0 || len(step.Cache.MaxExpiredTime) > 0 || step.Cache.Enable {
			fmt.Errorf("reference step can only have deps, parameters, input artifacts, reference")
		}
	}
	return nil
}
