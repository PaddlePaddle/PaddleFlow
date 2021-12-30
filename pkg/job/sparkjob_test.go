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

	sparkapp "paddleflow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/schema"
)

func TestPatchSparkAppVariable(t *testing.T) {
	confEnv := make(map[string]string)
	initConfigsForTest(confEnv)

	confEnv[schema.EnvJobType] = "spark"
	confEnv[schema.EnvJobNamespace] = "N1"

	tests := []struct {
		caseName      string
		additionalEnv map[string]string
		actualValue   *sparkapp.SparkApplication
		expectValue   string
		errMsg        string
	}{
		{
			caseName:    "spark_test",
			actualValue: &sparkapp.SparkApplication{},
			expectValue: "N1",
		},
		{
			caseName: "fromUserPath",
			additionalEnv: map[string]string{
				schema.EnvJobNamespace: "N2",
			},
			actualValue: &sparkapp.SparkApplication{},
			expectValue: "N2",
		},
	}

	for _, test := range tests {
		if len(test.additionalEnv) != 0 {
			for k, v := range test.additionalEnv {
				confEnv[k] = v
			}
		}
		conf := &models.Conf{
			Env:     confEnv,
			Command: "sleep 3600",
			Image:   "test",
		}

		jobApp := test.actualValue

		if err := createJobFromYaml(conf, jobApp); err != nil {
			assert.Equal(t, test.errMsg, err.Error())
		}

		jobID := generateJobID(conf.Name)
		patchSparkAppVariable(jobApp, jobID, conf)
		t.Logf("case[%s] jobApp=%+v", test.caseName, *jobApp)

		assert.NotEmpty(t, jobApp.Spec.Volumes)
		assert.NotEmpty(t, jobApp.Spec.MainClass)
		assert.NotEmpty(t, jobApp.Spec.BatchSchedulerOptions.Queue)

		assert.NotEmpty(t, jobApp.Spec.Driver)
		assert.NotEmpty(t, jobApp.Spec.Driver.VolumeMounts)

		assert.NotEmpty(t, jobApp.Spec.Executor)
		assert.NotEmpty(t, jobApp.Spec.Executor.VolumeMounts)
		assert.NotEmpty(t, jobApp.Spec.Executor.Instances)

		assert.Equal(t, test.expectValue, jobApp.Namespace)
	}
}
