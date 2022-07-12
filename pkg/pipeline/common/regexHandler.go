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
)

type VariableChecker struct {
}

func (variableChecker *VariableChecker) CheckVarName(varName string) error {
	// 校验字符串是一个合格变量名，只能由字母数字下划线组成，且以字母下划线开头
	pattern := `^[A-Za-z_][A-Za-z0-9_]{1,49}$`
	reg := regexp.MustCompile(pattern)
	if !reg.MatchString(varName) {
		err := fmt.Errorf("format of variable name[%s] invalid, should be in ^[A-Za-z_][A-Za-z0-9_]{1,49}$", varName)
		return err
	}
	return nil
}

func (VariableChecker *VariableChecker) CheckCompName(compName string) error {
	// 和CheckVarName的区别在于，component的名称不可以包含下划线(_)，而可以包含中划线(-)
	pattern := `^[a-zA-Z][a-zA-Z0-9-]*$`
	reg := regexp.MustCompile(pattern)
	if !reg.MatchString(compName) || strings.HasPrefix(compName, "PF_") {
		err := fmt.Errorf("format of component name[%s] invalid, should be in ^[a-zA-Z][a-zA-Z0-9-]*$ and not start with \"PF_\"", compName)
		return err
	}
	return nil
}

func (variableChecker *VariableChecker) CheckRefUpstreamStep(varValue string) error {
	// 匹配引用上游节点参数的字符串
	pattern := RegExpUpstreamTpl
	reg := regexp.MustCompile(pattern)
	if !reg.MatchString(varValue) {
		err := fmt.Errorf("format of value[%s] invalid, should be like {{XX-XX.XX_XX}}", varValue)
		return err
	}
	return nil
}

// 检查是否使用了模板，如{{xxx}}
func (variableChecker *VariableChecker) CheckRefCurArgument(varValue string) error {
	pattern := RegExpIncludingCurTpl
	reg := regexp.MustCompile(pattern)
	if !reg.MatchString(varValue) {
		err := fmt.Errorf("format of value[%s] invalid, should be like {{XX_XX}}", varValue)
		return err
	}
	return nil
}
