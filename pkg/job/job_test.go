/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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

package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/schema"
)

func initConfigsForTest(confEnv map[string]string) {
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.DefaultJobYamlDir = "../../config/server/default/job"
	config.GlobalServerConfig.Job.SchedulerName = "testSchedulerName"

	confEnv[schema.EnvJobType] = string(schema.TypeVcJob)
	confEnv[schema.EnvJobNamespace] = "N1"
	config.GlobalServerConfig.FlavourMap = map[string]schema.Flavour{
		"ss": {
			Name: "ss",
			ResourceInfo: schema.ResourceInfo{
				Cpu: "1",
				Mem: "100M",
			},
		},
	}
	confEnv[schema.EnvJobFlavour] = "ss"
	confEnv[schema.EnvJobPServerReplicas] = "3"
	confEnv[schema.EnvJobFsID] = "fs-root-test"
	confEnv[schema.EnvJobPVCName] = "test-pvc-name"
}

func TestAppendVolumeIfAbsent(t *testing.T) {
	var vls []corev1.Volume
	vls = nil
	t.Logf("%v", vls)
	vls = appendVolumeIfAbsent(vls, corev1.Volume{Name: "vm1"})
	assert.Equal(t, 1, len(vls))
}
