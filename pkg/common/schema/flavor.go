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

package schema

import (
	"regexp"
	"strings"

	"paddleflow/pkg/common/errors"
)

const (
	RegPatternResource = "^[1-9][0-9]*([numkMGTPE]|Ki|Mi|Gi|Ti|Pi|Ei)?$"
)

type ScalarResourcesType map[ResourceName]string

type ResourceInfo struct {
	Cpu             string              `json:"cpu" yaml:"cpu"`
	Mem             string              `json:"mem" yaml:"mem"`
	ScalarResources ScalarResourcesType `json:"scalarResources,omitempty" yaml:"scalarResources,omitempty"`
}

type Flavour struct {
	ResourceInfo `yaml:",inline"`
	Name         string `json:"name" yaml:"name"`
}

func isValidScalarResource(r string, scalarResourcesType []string) bool {
	for _, sr := range scalarResourcesType {
		if strings.EqualFold(r, sr) {
			return true
		}
	}
	return false
}

func CheckReg(str, pattern string) bool {
	reg, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}
	ret := reg.MatchString(str)
	return ret
}

func ValidateResourceInfo(resourceInfo ResourceInfo, scalarResourcesType []string) error {
	if resourceInfo.Cpu == "" {
		return errors.CpuNotFoundError()
	}

	if resourceInfo.Mem == "" {
		return errors.MemoryNotFoundError()
	}

	if !CheckReg(resourceInfo.Cpu, RegPatternResource) || !CheckReg(resourceInfo.Mem, RegPatternResource) {
		return errors.QueueResourceNotMatchError(resourceInfo.Cpu, resourceInfo.Mem)
	}
	return ValidateScalarResourceInfo(resourceInfo.ScalarResources, scalarResourcesType)
}

func ValidateScalarResourceInfo(scalarResources ScalarResourcesType, scalarResourcesType []string) error {
	for k := range scalarResources {
		resourceName := string(k)
		if !isValidScalarResource(resourceName, scalarResourcesType) {
			return errors.InvalidScaleResourceError(resourceName)
		}
	}
	return nil
}
