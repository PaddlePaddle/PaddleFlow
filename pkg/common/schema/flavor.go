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
	"fmt"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
)

// ResourceName is the name identifying various resources in a ResourceList.
type ResourceName string

// ScalarResourcesType is the type of scalar resources
type ScalarResourcesType map[ResourceName]string

// ResourceInfo is a struct that contains the information of a resource.
type ResourceInfo struct {
	CPU             string              `json:"cpu" yaml:"cpu"`
	Mem             string              `json:"mem" yaml:"mem"`
	ScalarResources ScalarResourcesType `json:"scalarResources,omitempty" yaml:"scalarResources,omitempty"`
}

// Flavour is a set of resources that can be used to run a job.
type Flavour struct {
	ResourceInfo `yaml:",inline"`
	Name         string `json:"name" yaml:"name"`
}

func (r ResourceInfo) ToMap() map[string]string {
	res := make(map[string]string)
	res[resources.ResCPU] = r.CPU
	res[resources.ResMemory] = r.Mem
	if r.ScalarResources != nil {
		for key, value := range r.ScalarResources {
			res[string(key)] = value
		}
	}
	return res
}

func isValidScalarResource(r string, scalarResourcesType []string) bool {
	// TODO: get scalarResourcesType from cluster
	if len(scalarResourcesType) == 0 {
		return true
	}
	for _, sr := range scalarResourcesType {
		if strings.EqualFold(r, sr) {
			return true
		}
	}
	return false
}

// CheckReg
// TODO: remove this function to util
func CheckReg(str, pattern string) bool {
	reg, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}
	ret := reg.MatchString(str)
	return ret
}

func CheckScalarResource(res string) error {
	q, err := resource.ParseQuantity(res)
	if err != nil {
		return err
	}
	if q.Sign() < 0 {
		return fmt.Errorf("resource cannot be negative")
	}
	return nil
}

// ValidateResourceItem check resource for cpu or memory
func ValidateResourceItem(res string) error {
	q, err := resource.ParseQuantity(res)
	if err != nil {
		return err
	}
	if q.IsZero() || q.Sign() < 0 {
		return fmt.Errorf("cpu cannot be negative")
	}
	return nil
}

// IsEmptyResource return true when cpu or mem is nil
func IsEmptyResource(resourceInfo ResourceInfo) bool {
	return resourceInfo.CPU == "" || resourceInfo.Mem == ""
}

// ValidateScalarResourceInfo validate scalar resource info
func ValidateScalarResourceInfo(scalarResources ScalarResourcesType, scalarResourcesType []string) error {
	for k, v := range scalarResources {
		resourceName := string(k)
		if !isValidScalarResource(resourceName, scalarResourcesType) {
			return errors.InvalidScaleResourceError(resourceName)
		}
		if err := CheckScalarResource(v); err != nil {
			return fmt.Errorf("%s %v", resourceName, err)
		}
	}
	return nil
}

// ValidateResource validate resource info
func ValidateResource(resourceInfo ResourceInfo, scalarResourcesType []string) error {
	if err := ValidateResourceItem(resourceInfo.CPU); err != nil {
		return fmt.Errorf("validate cpu failed,err: %v", err)
	}
	if err := ValidateResourceItem(resourceInfo.Mem); err != nil {
		return fmt.Errorf("validate mem failed,err: %v", err)
	}

	return ValidateScalarResourceInfo(resourceInfo.ScalarResources, scalarResourcesType)
}
