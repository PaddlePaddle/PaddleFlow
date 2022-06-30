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
)

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

func EmptyResourceInfo() *ResourceInfo {
	return &ResourceInfo{
		CPU:             "0",
		Mem:             "0",
		ScalarResources: make(ScalarResourcesType),
	}
}

// SetScalar sets a resource by a scalar value of this resource.
func (r *ResourceInfo) SetScalar(name ResourceName, value string) {
	// Lazily allocate scalar resource map.
	if r.ScalarResources == nil {
		r.ScalarResources = make(ScalarResourcesType)
	}
	r.ScalarResources[name] = value
}

// LessEqual returns true if the current flavour is less than or equal to the other one.
func (r *ResourceInfo) LessEqual(r2 ResourceInfo) bool {
	quantities1, _ := newQuantities(*r)
	quantities2, _ := newQuantities(r2)
	return quantities1.lessEqual(quantities2)
}

// Add adds the resource info of the other flavour to the current one.
func (r *ResourceInfo) Add(r2 ResourceInfo) ResourceInfo {
	quantities1, _ := newQuantities(*r)
	quantities2, _ := newQuantities(r2)
	(*quantities1).add(quantities2)
	return newResourceInfo(*quantities1)
}

// Sub subtracts the other resource from the current one.
func (r *ResourceInfo) Sub(r2 ResourceInfo) (ResourceInfo, error) {
	quantities1, _ := newQuantities(*r)
	quantities2, _ := newQuantities(r2)
	_, err := (*quantities1).sub(quantities2)
	if err != nil {
		return ResourceInfo{}, err
	}
	return newResourceInfo(*quantities1), nil
}

func newResourceInfo(q quantities) ResourceInfo {
	res := ResourceInfo{
		CPU:             q.MilliCPU.String(),
		Mem:             q.Memory.String(),
		ScalarResources: ScalarResourcesType{},
	}
	if q.ScalarResources != nil && len(q.ScalarResources) > 0 {
		for k, v := range q.ScalarResources {
			res.ScalarResources[k] = v.String()
		}
	}
	return res
}

type quantities struct {
	MilliCPU resource.Quantity `json:"cpu"`
	Memory   resource.Quantity `json:"memory"`

	// ScalarResources
	ScalarResources map[ResourceName]resource.Quantity `json:"scalarResources"`
}

// lessEqual returns true if this quantity is less than or equal to the other.
func (q quantities) lessEqual(q2 *quantities) bool {
	if q.MilliCPU.Cmp(q2.MilliCPU) > 0 {
		return false
	}
	if q.Memory.Cmp(q2.Memory) > 0 {
		return false
	}
	if q.ScalarResources == nil || len(q.ScalarResources) == 0 {
		return true
	}
	if q2.ScalarResources == nil || len(q2.ScalarResources) == 0 {
		return false
	}
	// it permits q2 has more resource types than q, we just check all keys in q
	for k, v := range q.ScalarResources {
		if v.Cmp(q2.ScalarResources[k]) > 0 {
			return false
		}
	}
	return true
}

func (q *quantities) add(q2 *quantities) *quantities {
	q.MilliCPU.Add(q2.MilliCPU)
	q.Memory.Add(q2.Memory)
	if q.ScalarResources == nil || len(q.ScalarResources) == 0 {
		q.ScalarResources = q2.ScalarResources
		return q
	}
	if q2.ScalarResources == nil || len(q2.ScalarResources) == 0 {
		return q
	}
	for k, v := range q2.ScalarResources {
		if _, exist := q.ScalarResources[k]; !exist {
			q.ScalarResources[k] = v
		} else {
			cur := q.ScalarResources[k]
			cur.Add(v)
			q.ScalarResources[k] = cur
		}
	}
	return q
}

func (q *quantities) sub(q2 *quantities) (*quantities, error) {
	// ensuring all resource in r2 is less equal than r
	if !q2.lessEqual(q) {
		return &quantities{}, fmt.Errorf("resource %v is not less equal than %v", q2, q)
	}
	q.MilliCPU.Sub(q2.MilliCPU)
	q.Memory.Sub(q2.Memory)
	if q.ScalarResources == nil || len(q.ScalarResources) == 0 {
		q.ScalarResources = q2.ScalarResources
		return q, nil
	}
	if q2.ScalarResources == nil || len(q2.ScalarResources) == 0 {
		return q, nil
	}
	for k, v := range q2.ScalarResources {
		if _, exist := q.ScalarResources[k]; !exist {
			return &quantities{}, fmt.Errorf("resource %s is not exist, sub operate is not support", k)
		}
		cur := q.ScalarResources[k]
		cur.Sub(v)
		q.ScalarResources[k] = cur
	}
	return q, nil
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

// CheckReg todo remove this function to util
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

// ValidateResourceItemNonNegative check resource non-negative, which permit zero value
func ValidateResourceItemNonNegative(res string) error {
	q, err := resource.ParseQuantity(res)
	if err != nil {
		return err
	}
	if q.Sign() < 0 {
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

// ValidateResourceNonNegative validate resource info with Non-negative
func ValidateResourceNonNegative(resourceInfo ResourceInfo, scalarResourcesType []string) error {
	if err := ValidateResourceItemNonNegative(resourceInfo.CPU); err != nil {
		return fmt.Errorf("validate cpu failed,err: %v", err)
	}
	if err := ValidateResourceItemNonNegative(resourceInfo.Mem); err != nil {
		return fmt.Errorf("validate mem failed,err: %v", err)
	}

	return ValidateScalarResourceInfo(resourceInfo.ScalarResources, scalarResourcesType)
}

func newQuantities(r ResourceInfo) (*quantities, error) {
	var err error
	q := &quantities{}
	if q.MilliCPU, err = resource.ParseQuantity(r.CPU); err != nil {
		return nil, err
	}
	if q.Memory, err = resource.ParseQuantity(r.Mem); err != nil {
		return nil, err
	}
	if q.ScalarResources == nil {
		q.ScalarResources = make(map[ResourceName]resource.Quantity)
	}
	for k, v := range r.ScalarResources {
		if q.ScalarResources[k], err = resource.ParseQuantity(v); err != nil {
			return nil, err
		}
	}
	return q, nil
}
