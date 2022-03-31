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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResource_LessEqual(t *testing.T) {
	queue1, _ := NewResource("1", "100M", nil)
	queue2, _ := NewResource("1", "100M", ScalarResourcesType{"nvidia.com/gpu": "500"})

	flavours := []struct {
		CPU             string
		Mem             string
		ScalarResources ScalarResourcesType
	}{
		{
			CPU:             "1",
			Mem:             "100M",
			ScalarResources: ScalarResourcesType{},
		},
		{
			CPU: "1",
			Mem: "100M",
			ScalarResources: ScalarResourcesType{
				"nvidia.com/gpu": "500",
			},
		},
	}

	cpuRes := flavours[0]
	cpuFlavour, _ := NewResource(cpuRes.CPU, cpuRes.Mem, cpuRes.ScalarResources)

	gpuRes := flavours[1]
	gpuFlavour, _ := NewResource(gpuRes.CPU, gpuRes.Mem, gpuRes.ScalarResources)

	// case1
	assert.True(t, cpuFlavour.LessEqual(queue1))
	assert.False(t, gpuFlavour.LessEqual(queue1))

	// case2
	assert.True(t, cpuFlavour.LessEqual(queue2))
	assert.True(t, gpuFlavour.LessEqual(queue2))
}
