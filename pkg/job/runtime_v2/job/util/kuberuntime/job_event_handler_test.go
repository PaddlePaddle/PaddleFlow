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

package kuberuntime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
)

func TestGetParentJobID(t *testing.T) {
	testCases := []struct {
		name   string
		obj    *unstructured.Unstructured
		pJobID string
	}{
		{
			name: "no parent job",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"namespace": "default",
						"name":      "test-job",
					},
					"status": make(map[string]interface{}),
				}},
			pJobID: "",
		},
		{
			name: "parent job",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"namespace": "default",
						"name":      "test-job",
						"ownerReferences": []interface{}{
							map[string]interface{}{
								"apiVersion": k8s.ArgoWorkflowGVK.GroupVersion().String(),
								"kind":       k8s.ArgoWorkflowGVK.Kind,
								"name":       "argo-workflow1",
							},
						},
					},
					"status": make(map[string]interface{}),
				}},
			pJobID: "argo-workflow1",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			jobID := GetParentJobID(testCase.obj)
			assert.Equal(t, testCase.pJobID, jobID)
		})
	}
}
