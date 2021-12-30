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
	"strings"

	"github.com/mitchellh/mapstructure"
	"paddleflow/pkg/common/schema"
)

const (
	SysParamNamePFRunID    = "PF_RUN_ID"
	SysParamNamePFFsID     = "PF_FS_ID"
	SysParamNamePFJobID    = "PF_JOB_ID"
	SysParamNamePFStepName = "PF_STEP_NAME"
	SysParamNamePFFsName   = "PF_FS_NAME"
	SysParamNamePFUserID   = "PF_USER_ID"
	SysParamNamePFUserName = "PF_USER_NAME"

	WfExtraInfoKeySource   = "Source" // pipelineID or yamlPath
	WfExtraInfoKeyUserName = "UserName"
	WfExtraInfoKeyFsName   = "FsName"
	WfExtraInfoKeyFsID     = "FsID"

	ParamTypeString = "string"
	ParamTypeFloat  = "float"
	ParamTypePath   = "path"

	WfParallelismDefault = 10
	WfParallelismMaximum = 20

	fieldParameters      string = "parameters"
	fieldCommand         string = "command"
	fieldEnv             string = "env"
	fieldInputArtifacts  string = "inputArtifacts"
	fieldOutputArtifacts string = "outputArtifacts"

	CacheStrategyConservative = "conservative"
	CacheStrategyAggressive   = "aggressive"
	CacheExpiredTimeNever     = "-1"
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

type StepParamSolver struct {
	steps       map[string]*schema.WorkflowSourceStep // 需要传入相关的所有 step，以处理依赖解析问题
	sysParams   map[string]string                     // sysParams 的值和 step 相关，需要传入
	needReplace bool                                  // 是否更新参数
}

func (s *StepParamSolver) Solve(currentStep string) error {
	step := s.steps[currentStep]
	// 1. parameter 校验/更新
	// parameter 必须最先被更新，artifact env command 可能会引用 step 内的param
	for paramName, paramVal := range step.Parameters {
		if err := s.checkName(currentStep, fieldParameters, paramName); err != nil {
			return err
		}
		realVal, err := s.checkParamValue(currentStep, paramName, paramVal, fieldParameters)
		if err != nil {
			return err
		}
		step.Parameters[paramName] = realVal
	}
	// 2. artifact 更新
	// artifact 必须先更新，command 可能会引用 step 内的 artifact
	for inputAtfName, inputAtfVal := range step.Artifacts.Input {
		if err := s.checkName(currentStep, fieldInputArtifacts, inputAtfName); err != nil {
			return err
		}
		realVal, err := s.checkParamValue(currentStep, inputAtfName, inputAtfVal, fieldInputArtifacts)
		if err != nil {
			return err
		}
		step.Artifacts.Input[inputAtfName] = fmt.Sprintf("%v", realVal)
	}
	for outAtfName, outAtfVal := range step.Artifacts.Output {
		if err := s.checkName(currentStep, fieldOutputArtifacts, outAtfName); err != nil {
			return err
		}
		realVal, err := s.checkParamValue(currentStep, outAtfName, outAtfVal, fieldOutputArtifacts)
		if err != nil {
			return err
		}
		step.Artifacts.Output[outAtfName] = fmt.Sprintf("%v", realVal)
	}
	// 3. env 校验/更新
	// 支持上游step参数依赖替换，当前step的parameter替换，以及平台内置参数替换
	for envName, envVal := range step.Env {
		if err := s.checkName(currentStep, fieldEnv, envName); err != nil {
			return err
		}
		realVal, err := s.checkParamValue(currentStep, envName, envVal, fieldEnv)
		if err != nil {
			return err
		}
		step.Env[envName] = fmt.Sprintf("%v", realVal)
	}
	// 4. env 校验/更新
	realVal, err := s.checkParamValue(currentStep, "command", step.Command, fieldCommand)
	if err != nil {
		return err
	}
	step.Command = fmt.Sprintf("%v", realVal)

	return nil
}

func (s *StepParamSolver) checkName(step, fieldType, name string) error {
	pattern := `^[a-zA-Z0-9_]+$`
	reg := regexp.MustCompile(pattern)
	regResult := reg.FindAllString(name, -1)
	if regResult == nil {
		err := fmt.Errorf("format of %s[%s] in step[%s] incorrect, should be in [a-zA-z0-9_]", fieldType, name, step)
		return err
	}
	return nil
}

func (s *StepParamSolver) checkParamValue(step string, paramName string, param interface{}, fieldType string) (interface{}, error) {
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
		return checkDictParam(dictParam, paramName, nil)
	default:
		return nil, UnsupportedParamTypeError(param, paramName)
	}
}

// resolveRefParam 解析引用参数
// parameters 字段中变量可引用系统参数、及上游参数
// input artifacts 字段中变量可引用系统参数、上游 outputArtifact、及本阶段 parameter
// output artifacts 字段中变量可引用系统参数、本阶段 parameter
func (s *StepParamSolver) resolveRefParam(step, param, fieldType string) (interface{}, error) {
	// regular expression must match case {{ xxx }} like {{ <step_name>.<param_name> }} or {{ PS_RUN_ID }}
	pattern := `\{\{(\s)*([a-zA-Z0-9_]*\.?[a-zA-Z0-9_]+)?(\s)*\}\}`
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
		refStep, refParamName := parseParamName(row[2])
		if len(refStep) == 0 {
			// {{ PS_RUN_ID }}
			var ok bool
			tmpVal, ok = s.sysParams[refParamName]
			if !ok {
				if fieldType == fieldParameters {
					return "", fmt.Errorf("unsupported SysParamName[%s] for param[%s]", refParamName, param)
				}
				tmpVal2, ok := s.steps[step].Parameters[refParamName]
				if !ok {
					if fieldType == fieldInputArtifacts || fieldType == fieldOutputArtifacts || fieldType == fieldEnv {
						return "", fmt.Errorf("unsupported RefParamName[%s] for param[%s]", refParamName, param)
					}
					// command 可以引用 system parameter + step 内的 parameter + artifact
					tmpVal, ok = s.steps[step].Artifacts.Input[refParamName]
					if !ok {
						tmpVal, ok = s.steps[step].Artifacts.Output[refParamName]
						if !ok {
							return "", fmt.Errorf("unsupported RefParamName[%s] for param[%s]", refParamName, param)
						}
					}
				} else {
					tmpVal = fmt.Sprintf("%v", tmpVal2)
				}
			}
		} else {
			tmpVal2, err := s.refParamExist(step, refStep, refParamName, fieldType)
			if err != nil {
				return nil, err
			}
			tmpVal = fmt.Sprintf("%v", tmpVal2)
		}
		if s.needReplace {
			result = strings.Replace(result, row[0], tmpVal, -1)
		}
	}
	return result, nil
}

func parseParamName(paramName string) (string, string) {
	paramStrList := strings.Split(paramName, ".")
	if len(paramStrList) == 1 {
		return "", paramStrList[0]
	}

	return paramStrList[0], paramStrList[1]
}

func (s *StepParamSolver) refParamExist(currentStep, refStep, refParamName, fieldType string) (interface{}, error) {
	if fieldType == fieldOutputArtifacts {
		return "", fmt.Errorf("output artifact[{{ %s.%s }}] cannot refer upstream artifact", refStep, refParamName)
	}
	step, ok := s.steps[currentStep]
	if !ok {
		return nil, fmt.Errorf("invalid reference param {{ %s.%s }} in step %s", refStep, refParamName, currentStep)
	}
	if StringsContain(step.GetDeps(), refStep) {
		ref := s.steps[refStep]
		switch fieldType {
		case fieldInputArtifacts:
			if refParamVal, ok := ref.Artifacts.Output[refParamName]; ok {
				// recursively get param value
				if realRefParamVal, err := s.checkParamValue(refStep, refParamName, refParamVal, fieldType); err != nil {
					return nil, err
				} else {
					return realRefParamVal, nil
				}
			}
		default:
			if refParamVal, ok := ref.Parameters[refParamName]; ok {
				// recursively get param value
				if realRefParamVal, err := s.checkParamValue(refStep, refParamName, refParamVal, fieldType); err != nil {
					return nil, err
				} else {
					return realRefParamVal, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("invalid %s reference {{ %s.%s }} in step %s", fieldType, refStep, refParamName, currentStep)
}

func checkDictParam(dict DictParam, paramName string, realVal interface{}) (interface{}, error) {
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
