/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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

func TestToKindGroupVersion(t *testing.T) {
	testCases := []struct {
		name        string
		clusterType string
		framework   Framework
		annotations map[string]string
		wantRes     KindGroupVersion
	}{
		{
			name:        "default paddle job kind group version",
			clusterType: KubernetesType,
			framework:   FrameworkPaddle,
			wantRes:     PaddleKindGroupVersion,
		},
		{
			name:        "custom paddle job kind group version",
			clusterType: KubernetesType,
			framework:   FrameworkPaddle,
			annotations: map[string]string{
				JobKindAnnotation:         "PaddleJob",
				JobGroupVersionAnnotation: "kubeflow.org/v1",
			},
			wantRes: KindGroupVersion{Kind: "PaddleJob", Group: "kubeflow.org", APIVersion: "v1"},
		},
		{
			name:        "custom paddle job kind group version failed",
			clusterType: KubernetesType,
			framework:   FrameworkPaddle,
			annotations: map[string]string{
				JobKindAnnotation:         "PaddleJob",
				JobGroupVersionAnnotation: "xx/kubeflow.org/v1",
			},
			wantRes: PaddleKindGroupVersion,
		},
		{
			name:        "custom tf job kind group version",
			clusterType: KubernetesType,
			framework:   FrameworkTF,
			annotations: map[string]string{
				JobKindAnnotation:         "TFJob",
				JobGroupVersionAnnotation: "v1",
			},
			wantRes: KindGroupVersion{Kind: "TFJob", Group: "", APIVersion: "v1"},
		},
		{
			name:        "unexcepted framework",
			clusterType: KubernetesType,
			framework:   Framework("xxx"),
			wantRes:     KindGroupVersion{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kgv := ToKindGroupVersion(tc.clusterType, tc.framework, tc.annotations)
			assert.Equal(t, tc.wantRes, kgv)
		})
	}
}
