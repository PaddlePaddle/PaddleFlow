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

package resources

import (
	"encoding/json"
	"fmt"
)

const (
	ResourceCPU     = "cpu"
	ResourceMemory  = "mem"
	ResourceStorage = "storage"
)

// Resource is a struct that contains the information of a resource.
// For cpu resource, the unit of cpu value is milli
type Resource struct {
	Resources map[string]Quantity `json:"-"`
}

func EmptyResource() *Resource {
	return &Resource{
		Resources: make(map[string]Quantity),
	}
}

func NewResourceFromInfo(cpu, mem string, scalarResources map[string]string) (*Resource, error) {
	if scalarResources == nil {
		scalarResources = make(map[string]string)
	}
	scalarResources[ResourceCPU] = cpu
	scalarResources[ResourceMemory] = mem
	return NewResourceFromMap(scalarResources)
}

func NewResourceFromMap(resourceInfo map[string]string) (*Resource, error) {
	r := EmptyResource()
	for key, strVal := range resourceInfo {
		var intValue Quantity
		var err error
		switch key {
		case ResourceCPU:
			intValue, err = ParseMilliQuantity(strVal)
		default:
			intValue, err = ParseQuantity(strVal)
		}
		if err != nil {
			return nil, err
		}
		if intValue < 0 {
			return nil, fmt.Errorf("negative resources not permitted: %v", resourceInfo)
		}
		r.Resources[key] = intValue
	}
	return r, nil
}

func (r Resource) MarshalJSON() ([]byte, error) {
	res := &struct {
		CPU             string            `json:"cpu"`
		Mem             string            `json:"mem"`
		Storage         string            `json:"storage,omitempty"`
		ScalarResources map[string]string `json:"scalarResources,omitempty"`
	}{
		CPU:             "0",
		Mem:             "0",
		ScalarResources: make(map[string]string),
	}
	for key, val := range r.Resources {
		switch key {
		case ResourceCPU:
			res.CPU = val.MilliString()
		case ResourceMemory:
			res.Mem = val.MemString()
		case ResourceStorage:
			res.Storage = val.MemString()
		default:
			res.ScalarResources[key] = val.String()
		}
	}
	return json.Marshal(res)
}

func (r *Resource) UnmarshalJSON(value []byte) error {
	res := &struct {
		CPU             string            `json:"cpu,omitempty"`
		Mem             string            `json:"mem,omitempty"`
		Storage         string            `json:"storage,omitempty"`
		ScalarResources map[string]string `json:"scalarResources,omitempty"`
	}{
		ScalarResources: make(map[string]string),
	}

	err := json.Unmarshal(value, res)
	if err != nil {
		return err
	}
	res.ScalarResources[ResourceCPU] = res.CPU
	res.ScalarResources[ResourceMemory] = res.Mem
	if res.Storage != "" {
		res.ScalarResources[ResourceStorage] = res.Storage
	}
	rr, err := NewResourceFromMap(res.ScalarResources)
	if err != nil {
		return err
	}
	*r = *rr
	return nil
}

func (r Resource) String() string {
	msg := ""
	for rName, rValue := range r.Resources {
		switch rName {
		case ResourceCPU:
			msg += fmt.Sprintf("%s: %s, ", rName, rValue.MilliString())
		case ResourceMemory:
			msg += fmt.Sprintf("%s: %s, ", rName, rValue.MemString())
		default:
			msg += fmt.Sprintf("%s: %s, ", rName, rValue.String())
		}
	}
	return msg
}

func (r *Resource) SetResources(name string, value int64) {
	if r != nil {
		r.Resources[name] = r.Resources[name].add(Quantity(value))
	}
}

// Clone Return a deep copy of the resource it is called on.
func (r *Resource) Clone() *Resource {
	ret := EmptyResource()
	if r != nil {
		for k, v := range r.Resources {
			ret.Resources[k] = v
		}
		return ret
	}
	return nil
}

// Add additional resource to the base and updating the base resource
// Should be used by temporary computation only
func (r *Resource) Add(rr *Resource) {
	if r != nil {
		if rr == nil {
			return
		}
		for k, v := range rr.Resources {
			r.Resources[k] = r.Resources[k].add(v)
		}
	}
}

// Sub subtract from the resource the passed in resource by updating the resource it is called on.
// Should be used by temporary computation only
func (r *Resource) Sub(rr *Resource) {
	if r != nil {
		if rr == nil {
			return
		}
		for k, v := range rr.Resources {
			r.Resources[k] = r.Resources[k].sub(v)
		}
	}
}

// Multi multiples the resource with ratio provided
func (r *Resource) Multi(ratio int) {
	if r != nil {
		for k, v := range r.Resources {
			r.Resources[k] = v.multi(ratio)
		}
	}
}

// LessEqual returns true if this quantity is less than or equal to the other.
func (r *Resource) LessEqual(r2 *Resource) bool {
	if r == nil || r.Resources == nil || len(r.Resources) == 0 {
		return true
	}
	if r2 == nil || r2.Resources == nil || len(r2.Resources) == 0 {
		return false
	}
	// it permits q2 has more resource types than r, we just check all keys in r
	for k, v := range r.Resources {
		if v.cmp(r2.Resources[k]) > 0 {
			return false
		}
	}
	return true
}
