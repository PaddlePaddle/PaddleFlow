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
	"strings"
)

const (
	ResCPU     = "cpu"
	ResMemory  = "memory"
	ResStorage = "storage"
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

func NewResourceFromMap(resourceInfo map[string]string) (*Resource, error) {
	r := EmptyResource()
	for key, strVal := range resourceInfo {
		var intValue Quantity
		var err error
		switch key {
		case ResCPU:
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
		Mem             string            `json:"memory"`
		Storage         string            `json:"storage,omitempty"`
		ScalarResources map[string]string `json:"scalarResources,omitempty"`
	}{
		CPU:             "0",
		Mem:             "0",
		ScalarResources: make(map[string]string),
	}
	for key, val := range r.Resources {
		switch key {
		case ResCPU:
			res.CPU = val.MilliString()
		case ResMemory:
			res.Mem = val.MemString()
		case ResStorage:
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
		Mem             string            `json:"mem,omitempty"` // deprecated
		Memory          string            `json:"memory,omitempty"`
		Storage         string            `json:"storage,omitempty"`
		ScalarResources map[string]string `json:"scalarResources,omitempty"`
	}{
		ScalarResources: make(map[string]string),
	}

	err := json.Unmarshal(value, res)
	if err != nil {
		return err
	}
	res.ScalarResources[ResCPU] = res.CPU
	if res.Mem != "" {
		res.ScalarResources[ResMemory] = res.Mem
	} else {
		res.ScalarResources[ResMemory] = res.Memory
	}

	if res.Storage != "" {
		res.ScalarResources[ResStorage] = res.Storage
	}
	rr, err := NewResourceFromMap(res.ScalarResources)
	if err != nil {
		return err
	}
	*r = *rr
	return nil
}

func (r *Resource) ToMap() map[string]interface{} {
	res := make(map[string]interface{})
	for key, val := range r.Resources {
		switch key {
		case ResCPU:
			res[ResCPU] = val.MilliString()
		case ResMemory:
			res[ResMemory] = val.MemString()
		case ResStorage:
			res[ResStorage] = val.MemString()
		default:
			res[key] = val.String()
		}
	}
	return res
}

func (r Resource) String() string {
	msg := ""
	for rName, rValue := range r.Resources {
		switch rName {
		case ResCPU:
			msg += fmt.Sprintf("%s: %s, ", rName, rValue.MilliString())
		case ResMemory:
			msg += fmt.Sprintf("%s: %s, ", rName, rValue.MemString())
		default:
			msg += fmt.Sprintf("%s: %s, ", rName, rValue.String())
		}
	}
	return msg
}

func (r *Resource) SetResources(name string, value int64) {
	if r != nil {
		r.Resources[name] = Quantity(value)
	}
}

func (r *Resource) DelResources(name string) {
	if r != nil {
		delete(r.Resources, name)
	}
}

func (r *Resource) CPU() Quantity {
	if r != nil && r.Resources != nil {
		return r.Resources[ResCPU]
	}
	return 0
}

func (r *Resource) Memory() Quantity {
	if r != nil && r.Resources != nil {
		return r.Resources[ResMemory]
	}
	return 0
}

func (r *Resource) Storage() Quantity {
	if r != nil && r.Resources != nil {
		return r.Resources[ResStorage]
	}
	return 0
}

// ScalarResources return scalar resources by prefix, if prefix is empty, return all
func (r *Resource) ScalarResources(prefix string) map[string]Quantity {
	quans := map[string]Quantity{}
	if r != nil && r.Resources != nil {
		for res, quan := range r.Resources {
			switch res {
			case ResMemory, ResCPU, ResStorage:
			// pass
			default:
				if strings.HasPrefix(res, prefix) {
					quans[res] = quan
				}
			}
		}
	}
	return quans
}

// Resource return all resources
func (r *Resource) Resource() map[string]Quantity {
	if r.Resources == nil {
		return make(map[string]Quantity)
	}
	return r.Resources
}

func (r *Resource) IsNegative() bool {
	isNegative := false
	if r != nil {
		for _, rQuantity := range r.Resources {
			if rQuantity < 0 {
				isNegative = true
				break
			}
		}
	}
	return isNegative
}

// IsZero check if exist any zero resource
func (r *Resource) IsZero() bool {
	isZero := true
	if r != nil {
		for _, rQuantity := range r.Resources {
			if rQuantity > 0 {
				isZero = false
				break
			}
		}
	}
	return isZero
}

// Clone Return a deep copy of the resource it is called on.
func (r *Resource) Clone() *Resource {
	res := EmptyResource()
	if r != nil {
		for rName, rQuantity := range r.Resources {
			res.Resources[rName] = rQuantity
		}
	}
	return res
}

// Add additional resource to the base and updating the base resource
// Should be used by temporary computation only
func (r *Resource) Add(rr *Resource) {
	if r != nil {
		if rr == nil {
			return
		}
		for rName, rQuantity := range rr.Resources {
			r.Resources[rName] = r.Resources[rName].add(rQuantity)
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
		for rName, rQuantity := range rr.Resources {
			r.Resources[rName] = r.Resources[rName].sub(rQuantity)
		}
	}
}

// Multi multiples the resource with ratio provided
func (r *Resource) Multi(ratio int) {
	if r != nil {
		for rName, rQuantity := range r.Resources {
			r.Resources[rName] = rQuantity.multi(ratio)
		}
	}
}

// LessEqual returns true if this quantity is less than or equal to the other.
func (r *Resource) LessEqual(rr *Resource) bool {
	if r == nil || r.Resources == nil || len(r.Resources) == 0 {
		return true
	}
	if rr == nil || rr.Resources == nil || len(rr.Resources) == 0 {
		return false
	}
	// it permits rr has more resource types than r, we just check all keys in r
	for rName, rQuantity := range r.Resources {
		if rQuantity.cmp(rr.Resources[rName]) > 0 {
			return false
		}
	}
	return true
}
