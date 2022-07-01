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
	"k8s.io/apimachinery/pkg/api/resource"
)

// Quantity defines value type
type Quantity int64

// TODO: use our internal parse quantity

// ParseQuantity is used to parse user-provided values into int64.
func ParseQuantity(value string) (Quantity, error) {
	if len(value) == 0 {
		value = "0"
	}
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, err
	}
	return Quantity(q.Value()), nil
}

// ParseMilliQuantity is used to parse user-provided values into milli int64.
func ParseMilliQuantity(value string) (Quantity, error) {
	if len(value) == 0 {
		value = "0"
	}
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, err
	}
	return Quantity(q.MilliValue()), nil
}

func (q Quantity) String() string {
	rq := resource.NewQuantity(int64(q), resource.DecimalSI)
	return rq.String()
}

func (q Quantity) MilliString() string {
	rq := resource.NewMilliQuantity(int64(q), resource.DecimalSI)
	return rq.String()
}

func (q Quantity) MemString() string {
	format := resource.BinarySI
	if int64(q)%1000 == 0 {
		format = resource.DecimalSI
	}
	rq := resource.NewQuantity(int64(q), format)
	return rq.String()
}

func (q Quantity) add(rq Quantity) Quantity {
	// TODO: add more check
	return q + rq
}

func (q Quantity) sub(rq Quantity) Quantity {
	return q.add(-rq)
}

func (q Quantity) multi(ratio int) Quantity {
	return q * Quantity(ratio)
}

func (q Quantity) cmp(rq Quantity) int {
	switch {
	case q == rq:
		return 0
	case q < rq:
		return -1
	default:
		return 1
	}
}
