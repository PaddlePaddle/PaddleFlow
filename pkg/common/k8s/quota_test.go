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

package k8s

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestGPUDeviceIDX(t *testing.T) {
	t.Run("test gpu device idx", func(t *testing.T) {
		for idx := 0; idx < MaxGPUIndex; idx++ {
			devIDX := GPUDeviceIDX(idx)
			t.Logf("device idx for gpu %d is %d", idx, devIDX)
		}
	})
}

func TestGPUSharedDevices(t *testing.T) {
	testCases := []struct {
		name      string
		sharedGPU []int
	}{
		{
			name:      "2 gpu shared task",
			sharedGPU: []int{0, 1},
		},
		{
			name:      "4 gpu shared task",
			sharedGPU: []int{0, 1, 1, 3},
		},
		{
			name:      "5 gpu shared task",
			sharedGPU: []int{0, 0, 2, 3, 7},
		},
		{
			name:      "5 gpu shared task",
			sharedGPU: []int{0, 1, 2, 3, 7},
		},
		{
			name:      "high gpu index",
			sharedGPU: []int{12, 14, 14, 15, 15},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var deviceSum int64 = 0
			for _, idx := range tc.sharedGPU {
				deviceSum += GPUDeviceIDX(idx)
			}
			t.Logf("gpu device idx: %d", deviceSum)
			devices := GPUSharedDevices(deviceSum)
			t.Logf("shared gpu: %v", devices)
		})
	}
}

func TestSharedGPUIDX(t *testing.T) {
	testCases := []struct {
		name      string
		pod       *v1.Pod
		expectAns int64
	}{
		{
			name:      "annotation is nil",
			pod:       &v1.Pod{},
			expectAns: -1,
		},
		{
			name: "GPU_CORE_POD or GPU_IDX is nil",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"GPU_CORE_POD": "100",
					},
				},
			},
			expectAns: -1,
		},
		{
			name: "GPU_CORE_POD is error",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"GPU_CORE_POD": "10a",
						"GPU_IDX":      "0,1",
					},
				},
			},
			expectAns: -1,
		},
		{
			name: "gpu is shared",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"GPU_CORE_POD": "100",
						"GPU_IDX":      "0,1",
					},
				},
			},
			expectAns: 5,
		},
		{
			name: "gpu is not shared",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"GPU_CORE_POD": "100",
						"GPU_IDX":      "0",
					},
				},
			},
			expectAns: -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ans := SharedGPUIDX(tc.pod)
			assert.Equal(t, tc.expectAns, ans)
		})
	}
}

func TestIsGPUX(t *testing.T) {
	os.Setenv("PF_GPU_CORE_POD_KEY", "xxx_COM_GPU_CORE_POD")
	os.Setenv("PF_GPU_IDX_KEY", "XXX_COM_GPU_IDX")
	assert.Equal(t, false, IsGPUX("nvidia.com/gpu"))
}
