/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

// ResourceName is the name identifying various resources in a ResourceList.
type ResourceName string

// Resource struct defines all the resource type
type Resource struct {
	MilliCPU float64
	Memory   float64
	Storage  float64

	// ScalarResources
	ScalarResources map[ResourceName]float64
}

type NodeQuotaInfo struct {
	NodeName    string   `json:"nodeName"`
	Schedulable bool     `json:"schedulable"`
	Total       Resource `json:"total"`
	Idle        Resource `json:"idle"`
}

type QuotaSummary struct {
	TotalQuota Resource `json:"total"`
	IdleQuota  Resource `json:"idle"`
}

// Resource类的json schema
type resourceQuota struct {
	CPU             float64             `json:"cpu"`
	Memory          string              `json:"memory"`
	Storage         string              `json:"storage"`
	ScalarResources ScalarResourcesType `json:"scalarResources,omitempty"`
}

// Resource类json序列化的时候会自动调用该方法
func (r Resource) MarshalJSON() ([]byte, error) {
	result := resourceQuota{}
	result.ScalarResources = ScalarResourcesType{}

	result.CPU, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", r.MilliCPU/1000), 64)
	memory := float64(r.Memory) / 1024.0 / 1024.0
	result.Memory = fmt.Sprintf("%.2fMi", memory)
	storage := float64(r.Storage) / 1024.0 / 1024.0
	result.Storage = fmt.Sprintf("%.2fMi", storage)

	for resourceName, value := range r.ScalarResources {
		result.ScalarResources[resourceName] = fmt.Sprintf("%.f", value)
	}
	return json.Marshal(result)
}

// EmptyResource creates a empty resource object and returns
func EmptyResource() *Resource {
	r := Resource{}
	r.ScalarResources = map[ResourceName]float64{}
	return &r
}

func NewResource(cpu, memory string, scalarResources ScalarResourcesType) (*Resource, error) {
	r := Resource{}
	cpuInt, _ := strconv.Atoi(cpu)
	r.MilliCPU = float64(cpuInt * 1000)

	// todo current logical unsupport memory with unit
	memInt, _ := strconv.Atoi(memory)
	r.Memory = float64(memInt)

	for k, v := range scalarResources {
		vFloat, err := strconv.ParseFloat(v, 64)
		if err != nil {
			fmt.Errorf("string[%s] ParseFloat failed, %+v", v, err)
			return nil, err
		}
		r.AddScalar(k, vFloat)
	}
	return &r, nil
}

func (r *Resource) updateScalarResources(other *Resource, fn func(float64, float64) float64) {
	for resourceName, otherValue := range other.ScalarResources {
		value, found := r.ScalarResources[resourceName]
		if found {
			r.ScalarResources[resourceName] = fn(value, otherValue)
		} else {
			r.ScalarResources[resourceName] = otherValue
		}
	}
}

func (r *Resource) add(rr *Resource) *Resource {
	r.MilliCPU += rr.MilliCPU
	r.Memory += rr.Memory
	r.Storage += rr.Storage
	return r
}

// 不更新ScalarResources
func (r *Resource) AddWithoutScalarResources(rr *Resource) *Resource {
	return r.add(rr)
}

// add two resources
func (r *Resource) Add(rr *Resource) *Resource {
	r.add(rr)
	r.updateScalarResources(rr, func(x, y float64) float64 {
		return x + y
	})
	return r
}

func (r *Resource) sub(rr *Resource) *Resource {
	r.MilliCPU -= rr.MilliCPU
	r.Memory -= rr.Memory
	r.Storage -= rr.Storage
	return r
}

// 不更新ScalarResources
func (r *Resource) SubWithoutScalarResources(rr *Resource) *Resource {
	return r.sub(rr)
}

// Sub subtracts two Resource objects.
func (r *Resource) Sub(rr *Resource) *Resource {
	r.sub(rr)
	r.updateScalarResources(rr, func(x, y float64) float64 {
		return x - y
	})
	return r
}

// Multi multiples the resource with ratio provided
func (r *Resource) Multi(ratio float64) *Resource {
	r.MilliCPU *= ratio
	r.Memory *= ratio
	r.Storage *= ratio

	for rName, rQuant := range r.ScalarResources {
		r.ScalarResources[rName] = rQuant * ratio
	}
	return r
}

// String returns resource details in string format
func (r *Resource) String() string {
	str := fmt.Sprintf("cpu: %0.2f, memory: %0.2f, storage: %0.2f", r.MilliCPU, r.Memory, r.Storage)
	for rName, rQuant := range r.ScalarResources {
		str = fmt.Sprintf("%s, %s %0.2f", str, rName, rQuant)
	}
	return str
}

// AddScalar adds a resource by a scalar value of this resource.
func (r *Resource) AddScalar(name ResourceName, quantity float64) {
	r.SetScalar(name, r.ScalarResources[name]+quantity)
}

// SetScalar sets a resource by a scalar value of this resource.
func (r *Resource) SetScalar(name ResourceName, quantity float64) {
	// Lazily allocate scalar resource map.
	if r.ScalarResources == nil {
		r.ScalarResources = map[ResourceName]float64{}
	}
	r.ScalarResources[name] = quantity
}

var minValue float64 = 0.01

// LessEqual once any field in r less than rr, it would be return false
// Notice that r is properly less than rr and r.ScalarResources can be nil
// e.g. `if !requestResource.LessEqual(staticResource) {}`
func (r Resource) LessEqual(rr *Resource) bool {
	lessEqualFunc := func(l, r, diff float64) bool {
		if l < r || math.Abs(l-r) < diff {
			return true
		}
		return false
	}

	if !lessEqualFunc(r.MilliCPU, rr.MilliCPU, minValue) {
		return false
	}
	if !lessEqualFunc(r.Memory, rr.Memory, minValue) {
		return false
	}

	if r.ScalarResources == nil {
		return true
	}

	for rName, rQuant := range r.ScalarResources {
		if rQuant <= minValue {
			continue
		}
		if rr.ScalarResources == nil {
			return false
		}

		rrQuant := rr.ScalarResources[rName]
		if !lessEqualFunc(rQuant, rrQuant, minValue) {
			return false
		}
	}

	return true
}
