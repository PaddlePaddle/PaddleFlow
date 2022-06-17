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
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"

	. "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"

	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
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

		refStep, refParamName := parseTemplate(row[2])
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

// 获取制定Artifact的内容
// TODO: 作为 ResourceHandler 的成员函数
func GetArtifactContent(artPath string, maxSize int, fsID string, logger *logrus.Entry) (string, error) {
	fsHandler, err := handler.NewFsHandlerWithServer(fsID, logger)
	if err != nil {
		err = fmt.Errorf("failed to get the content of artifact by path[%s] : %v",
			artPath, err.Error())
		return "", err
	}

	stat, err := fsHandler.Stat(artPath)
	if err != nil {
		err = fmt.Errorf("failed to get the content of artifact by path[%s] : %v",
			artPath, err.Error())
		return "", err
	}

	if stat.IsDir() || stat.Size() >= int64(maxSize) {
		err = fmt.Errorf("failed to get the content of artifact by path[%s]: "+
			"maybe it's an directory or it is too large[>= %d]",
			artPath, maxSize)
	}

	content, err := fsHandler.ReadFsFile(artPath)
	if err != nil {
		err = fmt.Errorf("failed to get the content of artifact by path[%s] : %v",
			artPath, err.Error())
		return "", err
	}

	contentString := string(content)
	return contentString, nil
}

func GetRandID(randNum int) string {
	b := make([]byte, randNum/2)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func GetInputArtifactEnvName(atfName string) string {
	return "PF_INPUT_ARTIFACT_" + strings.ToUpper(atfName)
}

func GetOutputArtifactEnvName(atfName string) string {
	return "PF_OUTPUT_ARTIFACT_" + strings.ToUpper(atfName)
}
