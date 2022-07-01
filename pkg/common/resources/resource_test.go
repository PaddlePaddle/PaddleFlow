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
	"testing"

	"github.com/stretchr/testify/assert"
)

var marshalTestCases = []struct {
	name string
	res  Resource
	err  error
}{
	{
		name: "resource with 10 milli cpu",
		res: Resource{
			Resources: map[string]Quantity{
				"cpu":            10,
				"mem":            1024 * 1000,
				"nvidia.com/gpu": 2,
			},
		},
		err: nil,
	},
	{
		name: "resource with 1 cpu",
		res: Resource{
			Resources: map[string]Quantity{
				"cpu":            1000,
				"mem":            1024 * 1024 * 1024,
				"nvidia.com/gpu": 2,
			},
		},
		err: nil,
	},
	{
		name: "resource with 1k cpu",
		res: Resource{
			Resources: map[string]Quantity{
				"cpu":            1000 * 1000,
				"mem":            1024 * 1000 * 1000,
				"nvidia.com/gpu": 2,
			},
		},
		err: nil,
	},
}

var unmarshalTestCases = []struct {
	name         string
	resourceJSON string
	err          error
}{
	{
		name:         "resource with null string",
		resourceJSON: `{"cpu":"","mem":""}`,
		err:          nil,
	},
	{
		name:         "resource with zero string",
		resourceJSON: `{"cpu":"0","mem":"1Ki"}`,
		err:          nil,
	},
	{
		name:         "resource with 10 milli cpu",
		resourceJSON: `{"cpu":"10m","mem":"1Mi"}`,
		err:          nil,
	},
	{
		name:         "resource with 1 cpu",
		resourceJSON: `{"cpu":"1","mem":"1M"}`,
		err:          nil,
	},
	{
		name:         "resource with 1k cpu",
		resourceJSON: `{"cpu":"1k","mem":"1Gi"}`,
		err:          nil,
	},
	{
		name:         "resource with 1G mem",
		resourceJSON: `{"cpu":"1","mem":"1G"}`,
		err:          nil,
	},
}

func TestNewResourceFromInfo(t *testing.T) {
	testCases := []struct {
		name         string
		resourceInfo map[string]string
		err          error
	}{
		{
			name:         "resource with nil",
			resourceInfo: nil,
			err:          nil,
		},
		{
			name:         "resource without any resource mappings",
			resourceInfo: make(map[string]string),
			err:          nil,
		},
		{
			name: "resource with empty cpu",
			resourceInfo: map[string]string{
				"cpu": "",
				"mem": "",
			},
			err: nil,
		},
		{
			name: "resource with zero cpu",
			resourceInfo: map[string]string{
				"cpu": "0",
				"mem": "1G",
			},
			err: nil,
		},
		{
			name: "resource with zero mem",
			resourceInfo: map[string]string{
				"cpu": "1",
				"mem": "0",
			},
			err: nil,
		},
		{
			name: "resource with 10 milli cpu",
			resourceInfo: map[string]string{
				"cpu": "10m",
				"mem": "1Mi",
			},
			err: nil,
		},
		{
			name: "resource with 1 cpu",
			resourceInfo: map[string]string{
				"cpu": "1",
				"mem": "1Ki",
			},
			err: nil,
		},
		{
			name: "resource with 1k cpu",
			resourceInfo: map[string]string{
				"cpu": "1k",
				"mem": "1024",
			},
			err: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			r, err := NewResourceFromMap(test.resourceInfo)
			assert.Equal(t, test.err, err)
			if r != nil {
				rJSON, err := json.Marshal(r)
				assert.Equal(t, nil, err)
				t.Logf("result origin: %v, str: %s\n", r.Resources, string(rJSON))
			}
		})
	}
}

func TestResource_MarshalJSON(t *testing.T) {
	for _, test := range marshalTestCases {
		t.Run(test.name, func(t *testing.T) {
			r, err := json.Marshal(test.res)
			assert.Equal(t, test.err, err)
			if err == nil {
				t.Logf("resource info: %s", string(r))
			}
		})
	}
}

func TestResource_UnmarshalJSON(t *testing.T) {
	for _, test := range unmarshalTestCases {
		t.Run(test.name, func(t *testing.T) {
			r := EmptyResource()
			err := json.Unmarshal([]byte(test.resourceJSON), r)
			assert.Equal(t, test.err, err)
			if err == nil {
				t.Logf("resource info: %v", r)
			}
		})
	}
}

func BenchmarkResource_MarshalJSON(b *testing.B) {
	for _, test := range marshalTestCases {
		b.Run(test.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err := json.Marshal(test.res)
				if err != nil {
					b.Logf("marshal resource failed, err: %v", err)
				}
			}
		})
	}
}

func BenchmarkResource_UnmarshalJSON(b *testing.B) {
	for _, test := range unmarshalTestCases {
		b.Run(test.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				res := EmptyResource()
				err := json.Unmarshal([]byte(test.resourceJSON), res)
				if err != nil {
					b.Logf("unmarshal resource failed, err: %v", err)
				}
			}
		})
	}
}
