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
	"os"
	"regexp"
	"strings"

	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
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
*	resourceHandler
 */
type ResourceHandler struct {
	pplRunID  string
	fsHandler *handler.FsHandler
	logger    *log.Entry
}

func NewResourceHandler(runID string, fsID string, logger *log.Entry) (ResourceHandler, error) {
	fsHandler, err := handler.NewFsHandlerWithServer(fsID, logger)

	if err != nil {
		newErr := fmt.Errorf("init fsHandler failed: %s", err.Error())
		logger.Errorln(newErr)
		return ResourceHandler{}, newErr
	}

	resourceHandler := ResourceHandler{
		pplRunID:  runID,
		fsHandler: fsHandler,
		logger:    logger,
	}
	return resourceHandler, nil
}

func (resourceHandler *ResourceHandler) generateOutAtfPath(pplName string, stepName string, outatfName string, toInit bool) (string, error) {
	pipelineDir := "./.pipeline"
	outatfDir := fmt.Sprintf("%s/%s/%s/%s", pipelineDir, resourceHandler.pplRunID, pplName, stepName)
	outatfPath := fmt.Sprintf("%s/%s", outatfDir, outatfName)

	if toInit {
		isExist, err := resourceHandler.fsHandler.Exist(outatfPath)
		if err != nil {
			return "", err
		} else if isExist {
			resourceHandler.logger.Infof("path[%s] of outAtf[%s] already existed, clear first", outatfPath, outatfName)
			err = resourceHandler.fsHandler.RemoveAll(outatfPath)
			if err != nil {
				newErr := fmt.Errorf("clear generatePath[%s] for outAtf[%s] in step[%s] with pplname[%s] pplrunid[%s] failed: %s",
					outatfPath, outatfName, stepName, pplName, resourceHandler.pplRunID, err.Error())
				return "", newErr
			}
		}

		isExist, err = resourceHandler.fsHandler.Exist(outatfPath)
		if err != nil {
			return "", err
		} else if !isExist {
			resourceHandler.logger.Infof("prepare dir[%s] for path[%s] of outAtf[%s]", outatfDir, outatfPath, outatfName)
			err = resourceHandler.fsHandler.MkdirAll(outatfDir, os.ModePerm)
			if err != nil {
				newErr := fmt.Errorf("prepare dir[%s] for outAtf[%s] in step[%s] with pplname[%s] pplrunid[%s] failed: %s",
					outatfDir, outatfName, stepName, pplName, resourceHandler.pplRunID, err.Error())
				return "", newErr
			}
		}
	}

	return outatfPath, nil
}

func (resourceHandler *ResourceHandler) ClearResource() error {
	// 用于清理pplRunID对应的output artifact资源
	pipelineDir := "./.pipeline"
	runResourceDir := fmt.Sprintf("%s/%s", pipelineDir, resourceHandler.pplRunID)

	resourceHandler.logger.Infof("clear resource path[%s] of pplrunID[%s]", runResourceDir, resourceHandler.pplRunID)
	err := resourceHandler.fsHandler.RemoveAll(runResourceDir)
	if err != nil {
		newErr := fmt.Errorf("clear resource path[%s] of pplrunID[%s] failed: %s",
			runResourceDir, resourceHandler.pplRunID, err.Error())
		return newErr
	}

	return nil
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
		steps:     steps,
		sysParams: sysParams,
		useFs:     useFs,
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
		3. cache命中失败后, 用于job运行: output atf值由平台生成，路径格式为 ./.pipeline/{{PF_RUNID}}/{{pplName}}/{{stepName}}/{{outputAtfName}}
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
		return checkDictParam(dictParam, paramName, nil)
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
		refStepName, refParamName := parseParamName(row[2])
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
	steps         map[string]*schema.WorkflowSourceStep // 需要传入相关的所有 step，以判断依赖是否合法
	sysParams     map[string]string                     // sysParams 的值和 step 相关，需要传入
	disabledSteps []string                              // 被disabled的节点列表
	useFs         bool                                  // 表示是否挂载Fs
}

func (s *StepParamChecker) getWorkflowSourceStep(currentStep string) (*schema.WorkflowSourceStep, bool) {
	step, ok := s.steps[currentStep]
	return step, ok
}

func (s *StepParamChecker) getSysParam(paramName string) (string, bool) {
	param, ok := s.sysParams[paramName]
	return param, ok
}

func (s *StepParamChecker) getSysParams() map[string]string {
	return s.sysParams
}

func (s *StepParamChecker) checkDuplication(currentStep string) error {
	/*
		currentStep内，parameter, input/output artifact是否有重复的参数名
		这里的重名检查是大小写不敏感的，如"param"和"ParAm"会被认为重名
		Args:
			currentStep: 需要校验的step名
	*/
	step, ok := s.steps[currentStep]
	if !ok {
		return fmt.Errorf("check param reference failed: step %s not exist", currentStep)
	}

	m := make(map[string]string)
	for paramName, _ := range step.Parameters {
		paramNameUpper := strings.ToUpper(paramName)
		_, ok := m[paramNameUpper]
		if ok {
			return fmt.Errorf("parameter name[%s] has already existed in params/artifacts of step[%s] (these names are case-insensitive)",
				paramName, currentStep)
		} else {
			m[paramNameUpper] = ""
		}
	}

	for inputAtfName, _ := range step.Artifacts.Input {
		inputAtfNameUpper := strings.ToUpper(inputAtfName)
		_, ok := m[inputAtfNameUpper]
		if ok {
			return fmt.Errorf("inputAtf name[%s] has already existed in params/artifacts of step[%s] (these names are case-insensitive)",
				inputAtfName, currentStep)
		} else {
			m[inputAtfNameUpper] = ""
		}
	}

	for outputAtfName, _ := range step.Artifacts.Output {
		outputAtfNameUpper := strings.ToUpper(outputAtfName)
		_, ok := m[outputAtfNameUpper]
		if ok {
			return fmt.Errorf("outputAtf name[%s] has already existed in params/artifacts of step[%s] (these names are case-insensitive)",
				outputAtfName, currentStep)
		} else {
			m[outputAtfNameUpper] = ""
		}
	}

	return nil
}

func (s *StepParamChecker) Check(currentStep string) error {
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
	err := s.checkDuplication(currentStep)
	if err != nil {
		return err
	}

	step, ok := s.steps[currentStep]
	if !ok {
		return fmt.Errorf("check param reference failed: step %s not exist", currentStep)
	}

	// 1. parameter 校验
	// parameter不需要将dict类型的参数默认值提取出来，因为会在StepParamResolver中做了
	// 另外命令行参数替换默认值，会在workflow.go中完成
	for paramName, paramVal := range step.Parameters {
		if err = s.checkName(currentStep, FieldParameters, paramName); err != nil {
			return err
		}
		err = s.checkParamValue(currentStep, paramName, paramVal, FieldParameters)
		if err != nil {
			return err
		}
	}

	// 2. input artifact 校验
	for inputAtfName, inputAtfVal := range step.Artifacts.Input {
		if err = s.checkName(currentStep, FieldInputArtifacts, inputAtfName); err != nil {
			return err
		}

		variableChecker := VariableChecker{}
		err = variableChecker.CheckRefUpstreamStep(inputAtfVal)
		if err != nil {
			return fmt.Errorf("check input artifact [%s] in step[%s] failed: %s", inputAtfName, currentStep, err.Error())
		}

		err = s.checkParamValue(currentStep, inputAtfName, inputAtfVal, FieldInputArtifacts)
		if err != nil {
			return err
		}
	}

	// 3. output artifacts 由平台生成路径，所以在校验时，不会对其值进行校验（即使非空迟早也会被替换）
	// 如果不使用Fs，不能定义outputAtf。因为inputAtf只能引用上游output Atf，所以只需要校验outputAtf即可。
	if !s.useFs && len(step.Artifacts.Output) > 0 {
		return fmt.Errorf("cannot define artifact in step[%s] with no Fs mounted", currentStep)
	}
	for outAtfName, _ := range step.Artifacts.Output {
		if err = s.checkName(currentStep, FieldOutputArtifacts, outAtfName); err != nil {
			return err
		}
	}

	// 4. env 校验/更新
	for envName, envVal := range step.Env {
		if err = s.checkName(currentStep, FieldEnv, envName); err != nil {
			return err
		}
		err = s.checkParamValue(currentStep, envName, envVal, FieldEnv)
		if err != nil {
			return err
		}
	}

	// 5. command 校验/更新
	err = s.checkParamValue(currentStep, "command", step.Command, FieldCommand)
	if err != nil {
		return err
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
func (s *StepParamChecker) checkParamValue(step string, paramName string, param interface{}, fieldType string) error {
	if fieldType == FieldCommand || fieldType == FieldEnv || fieldType == FieldInputArtifacts {
		_, ok := param.(string)
		if !ok {
			return fmt.Errorf("value of %s[%s] invalid: should be string", fieldType, paramName)
		}
	}

	switch param.(type) {
	case float32, float64, int:
		return nil
	case string:
		return s.resolveRefParam(step, param.(string), fieldType)
	case map[interface{}]interface{}:
		dictParam := DictParam{}
		if err := dictParam.From(param); err != nil {
			return fmt.Errorf("invalid dict parameter[%s]", param)
		}
		_, err := checkDictParam(dictParam, paramName, nil)
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
func (s *StepParamChecker) resolveRefParam(step, param, fieldType string) error {
	// regular expression must match case {{ xxx }} like {{ <step-name>.<param_name> }} or {{ PS_RUN_ID }}
	pattern := RegExpIncludingTpl
	reg := regexp.MustCompile(pattern)
	matches := reg.FindAllStringSubmatch(param, -1)
	for _, row := range matches {
		if len(row) != 4 {
			return MismatchRegexError(param, pattern)
		}

		refStep, refParamName := parseParamName(row[2])
		if len(refStep) == 0 {
			// 分别替换系统参数，如{{PF_RUN_ID}}；当前step parameter；当前step的input artifact；当前step的output artifact
			// 只有param，env，command三类变量需要处理
			if !s.useFs && (refParamName == SysParamNamePFFsID || refParamName == SysParamNamePFFsName) {
				return fmt.Errorf("cannot use sysParam[%s] template in step[%s] for pipeline run with no Fs mounted", refParamName, step)
			}

			var ok bool
			_, ok = s.sysParams[refParamName]
			if !ok {
				if fieldType == FieldParameters {
					return fmt.Errorf("unsupported SysParamName[%s] for param[%s] of filedType[%s]", refParamName, param, fieldType)
				}
				_, ok := s.steps[step].Parameters[refParamName]
				if !ok {
					if fieldType == FieldEnv {
						return fmt.Errorf("unsupported RefParamName[%s] for param[%s] of filedType[%s]", refParamName, param, fieldType)
					}

					// 处理command逻辑，command 可以引用 system parameter + step 内的 parameter + artifact
					_, ok = s.steps[step].Artifacts.Input[refParamName]
					if !ok {
						_, ok = s.steps[step].Artifacts.Output[refParamName]
						if !ok {
							return fmt.Errorf("unsupported RefParamName[%s] for param[%s] of filedType[%s]", refParamName, param, fieldType)
						}
					}
				}
			}
		} else {
			err := s.refParamExist(step, refStep, refParamName, fieldType)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func parseParamName(paramName string) (string, string) {
	paramStrList := strings.Split(paramName, ".")
	if len(paramStrList) == 1 {
		return "", paramStrList[0]
	}

	return paramStrList[0], paramStrList[1]
}

func (s *StepParamChecker) refParamExist(currentStepName, refStepName, refParamName, fieldType string) error {
	/*
		只有param，env，command，input artifact 四类变量需要处理
		param，env，command 只做上游step的parameter 依赖替换
		input artifact 只做上游step的output artifact 依赖替换
	*/
	step := s.steps[currentStepName]
	if !StringsContain(step.GetDeps(), refStepName) {
		return fmt.Errorf("invalid reference param {{ %s.%s }} in step[%s]: step[%s] not in deps", refStepName, refParamName, currentStepName, refStepName)
	}

	for _, disableStepName := range s.disabledSteps {
		if refStepName == disableStepName {
			return fmt.Errorf("invalid reference param {{ %s.%s }} in step[%s]: step[%s] is disabled", refStepName, refParamName, currentStepName, refStepName)
		}
	}

	refStep, ok := s.steps[refStepName]
	if !ok {
		return fmt.Errorf("invalid reference param {{ %s.%s }} in step[%s]: step[%s] not exist", refStepName, refParamName, currentStepName, refStepName)
	}

	switch fieldType {
	case FieldInputArtifacts:
		if _, ok := refStep.Artifacts.Output[refParamName]; !ok {
			return fmt.Errorf("invalid reference param {{ %s.%s }} in step[%s]: output artifact[%s] not exist", refStepName, refParamName, currentStepName, refParamName)
		}
	default:
		if _, ok := refStep.Parameters[refParamName]; !ok {
			return fmt.Errorf("invalid reference param {{ %s.%s }} in step[%s]: parameter[%s] not exist", refStepName, refParamName, currentStepName, refParamName)
		}
	}

	return nil
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
	case ParamTypeInt:
		_, ok1 := realVal.(int32)
		_, ok2 := realVal.(int64)
		_, ok3 := realVal.(int)
		if ok1 || ok2 || ok3 {
			return realVal, nil
		}
		return nil, InvalidParamTypeError(realVal, ParamTypeInt)
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
