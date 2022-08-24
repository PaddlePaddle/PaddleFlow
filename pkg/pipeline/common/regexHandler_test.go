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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckVarName(t *testing.T) {
	vc := VariableChecker{}
	fmt.Println("123")

	varName := "validName1_"
	err := vc.CheckVarName(varName)
	assert.Nil(t, err)

	varName = "_validName1_"
	err = vc.CheckVarName(varName)
	assert.Nil(t, err)

	varName = "ValidName1_"
	err = vc.CheckVarName(varName)
	assert.Nil(t, err)

	varName = "1validName1_"
	err = vc.CheckVarName(varName)
	assert.NotNil(t, err)
	assert.Equal(t, "format of variable name[1validName1_] invalid, should be in ^[A-Za-z_][A-Za-z0-9_]{0,49}$", err.Error())

	varName = "validName.1_"
	err = vc.CheckVarName(varName)
	assert.NotNil(t, err)
	assert.Equal(t, "format of variable name[validName.1_] invalid, should be in ^[A-Za-z_][A-Za-z0-9_]{0,49}$", err.Error())
}

func TestCheckRefUpstreamStep(t *testing.T) {
	vc := VariableChecker{}

	refPattern := "{{step1.varName2}}"
	err := vc.CheckRefUpstreamStep(refPattern)
	assert.Nil(t, err)

	refPattern = "{{-step1._varName2}}"
	err = vc.CheckRefUpstreamStep(refPattern)
	assert.Nil(t, err)

	refPattern = "{{  step1.varName2}}"
	err = vc.CheckRefUpstreamStep(refPattern)
	assert.Nil(t, err)

	refPattern = "{{step1.varName2  }}"
	err = vc.CheckRefUpstreamStep(refPattern)
	assert.Nil(t, err)

	refPattern = "{{  step1.varName2  }}"
	err = vc.CheckRefUpstreamStep(refPattern)
	assert.Nil(t, err)

	refPattern = "{{step1.varName2.wrongPattern}}"
	err = vc.CheckRefUpstreamStep(refPattern)
	assert.NotNil(t, err)
	assert.Equal(t, "format of value[{{step1.varName2.wrongPattern}}] invalid, should be like {{XX-XX.XX_XX}}", err.Error())

	refPattern = "{{wrongPattern}}"
	err = vc.CheckRefUpstreamStep(refPattern)
	assert.NotNil(t, err)
	assert.Equal(t, "format of value[{{wrongPattern}}] invalid, should be like {{XX-XX.XX_XX}}", err.Error())

	refPattern = "prefix{{step1.varName2}}"
	err = vc.CheckRefUpstreamStep(refPattern)
	assert.NotNil(t, err)
	assert.Equal(t, "format of value[prefix{{step1.varName2}}] invalid, should be like {{XX-XX.XX_XX}}", err.Error())

	refPattern = "{{step1.varName2}}postfix"
	err = vc.CheckRefUpstreamStep(refPattern)
	assert.NotNil(t, err)
	assert.Equal(t, "format of value[{{step1.varName2}}postfix] invalid, should be like {{XX-XX.XX_XX}}", err.Error())

	refPattern = "step1.varName2"
	err = vc.CheckRefUpstreamStep(refPattern)
	assert.NotNil(t, err)
	assert.Equal(t, "format of value[step1.varName2] invalid, should be like {{XX-XX.XX_XX}}", err.Error())

	refPattern = "{{step1.varName2"
	err = vc.CheckRefUpstreamStep(refPattern)
	assert.NotNil(t, err)
	assert.Equal(t, "format of value[{{step1.varName2] invalid, should be like {{XX-XX.XX_XX}}", err.Error())

	refPattern = "step1.varName2}}"
	err = vc.CheckRefUpstreamStep(refPattern)
	assert.NotNil(t, err)
	assert.Equal(t, "format of value[step1.varName2}}] invalid, should be like {{XX-XX.XX_XX}}", err.Error())
}
