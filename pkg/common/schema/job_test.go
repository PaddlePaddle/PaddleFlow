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

func TestJobConf_KindGroupVersion(t *testing.T) {
	testCases := []struct {
		name      string
		conf      *Conf
		framework Framework
		wanted    KindGroupVersion
	}{
		{
			name: "new kind group version",
			conf: &Conf{
				Name:             "paddle-job",
				KindGroupVersion: PaddleKindGroupVersion,
			},
			framework: FrameworkPaddle,
			wanted:    PaddleKindGroupVersion,
		},
		{
			name: "kind group version for old distributed paddle job",
			conf: &Conf{
				Name: "paddle-job",
			},
			framework: FrameworkPaddle,
			wanted:    PaddleKindGroupVersion,
		},
		{
			name: "kind group version for old workflow job",
			conf: &Conf{
				Name: "workflow-job",
			},
			framework: Framework(""),
			wanted:    WorkflowKindGroupVersion,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kgv := tc.conf.GetKindGroupVersion(tc.framework)
			assert.Equal(t, tc.wanted, kgv)
		})
	}
}
