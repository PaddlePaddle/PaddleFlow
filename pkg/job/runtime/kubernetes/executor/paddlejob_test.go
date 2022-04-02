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

package executor

import (
	"net/http/httptest"
	"testing"

	pdv1 "github.com/paddleflow/paddle-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"

	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/job/api"
)

func TestPatchPaddleJobVariable(t *testing.T) {
	confEnv := make(map[string]string)
	initConfigsForTest(confEnv)
	confEnv[schema.EnvJobType] = string(schema.TypePaddleJob)
	confEnv[schema.EnvJobFlavour] = "gpu"
	// init for paddle's ps mode
	confEnv[schema.EnvJobPServerCommand] = "sleep 30"
	confEnv[schema.EnvJobWorkerCommand] = "sleep 30"
	confEnv[schema.EnvJobPServerReplicas] = "2"
	confEnv[schema.EnvJobWorkerReplicas] = "2"
	confEnv[schema.EnvJobPServerFlavour] = "cpu"
	confEnv[schema.EnvJobWorkerFlavour] = "gpu"
	confEnv[schema.EnvJobFsID] = "fs1"

	pfjob := &api.PFJob{
		Conf: schema.Conf{
			Name:    "confName",
			Env:     confEnv,
			Command: "sleep 3600",
			Image:   "nginx",
		},
		JobType: schema.TypePaddleJob,
	}

	tests := []struct {
		caseName      string
		ID            string
		Name          string
		jobMode       string
		additionalEnv map[string]string
		actualValue   *pdv1.PaddleJob
		expectValue   string
		errMsg        string
	}{
		{
			caseName:    "psMode",
			ID:          "job-1",
			Name:        "job-1",
			jobMode:     schema.EnvJobModePS,
			actualValue: &pdv1.PaddleJob{},
			expectValue: "paddle",
		},
		{
			caseName:    "collectiveMode",
			ID:          "job-2",
			Name:        "job-2",
			jobMode:     schema.EnvJobModeCollective,
			actualValue: &pdv1.PaddleJob{},
			expectValue: "paddle",
		},
		{
			caseName: "fromUserPath",
			ID:       "job-3",
			Name:     "job-3",
			jobMode:  schema.EnvJobModePS,
			additionalEnv: map[string]string{
				schema.EnvJobNamespace: "N2",
			},
			actualValue: &pdv1.PaddleJob{},
			expectValue: "paddle",
		},
	}

	var server = httptest.NewServer(DiscoveryHandlerFunc)
	defer server.Close()
	dynamicClient := newFakeDynamicClient(server)

	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			if len(test.additionalEnv) != 0 {
				for k, v := range test.additionalEnv {
					pfjob.Conf.SetEnv(k, v)
				}
			}
			pfjob.Conf.SetEnv(schema.EnvJobMode, test.caseName)
			pfjob.JobMode = test.jobMode
			pfjob.ID = test.ID
			pfjob.Name = test.Name
			pfjob.Namespace = "default"
			// yaml content
			extRuntimeConf, err := pfjob.GetExtRuntimeConf(pfjob.Conf.GetFS(), pfjob.Conf.GetYamlPath())
			if err != nil {
				t.Errorf(err.Error())
			}
			pfjob.ExtRuntimeConf = extRuntimeConf
			paddleJob, err := NewKubeJob(pfjob, dynamicClient)
			assert.Equal(t, nil, err)

			pdj := test.actualValue
			_, err = paddleJob.CreateJob()
			assert.Equal(t, nil, err)

			jobObj, err := Get(pfjob.Namespace, pfjob.Name, k8s.PaddleJobGVK, dynamicClient)
			assert.Equal(t, nil, err)

			err = runtime.DefaultUnstructuredConverter.FromUnstructured(jobObj.Object, pdj)
			assert.Equal(t, nil, err)

			t.Logf("case[%s]\n pdj=%+v \n pdj.Spec.SchedulingPolicy=%+v \n pdj.Spec.Worker=%+v", test.caseName, pdj, pdj.Spec.SchedulingPolicy, pdj.Spec.Worker)

			assert.NotEmpty(t, pdj.Spec.Worker)
			assert.NotEmpty(t, pdj.Spec.Worker.Template.Spec.Containers)
			assert.NotEmpty(t, pdj.Spec.Worker.Template.Spec.Containers[0].Name)
			if test.jobMode == schema.EnvJobModePS {
				assert.NotEmpty(t, pdj.Spec.PS)
				assert.NotEmpty(t, pdj.Spec.PS.Template.Spec.Containers)
				assert.NotEmpty(t, pdj.Spec.PS.Template.Spec.Containers[0].Name)
			}
			assert.NotEmpty(t, pdj.Spec.SchedulingPolicy)
			t.Logf("len(pdj.Spec.SchedulingPolicy.MinResources)=%d", len(pdj.Spec.SchedulingPolicy.MinResources))
			assert.NotZero(t, len(pdj.Spec.SchedulingPolicy.MinResources))
			t.Logf("case[%s] SchedulingPolicy=%+v MinAvailable=%+v MinResources=%+v ", test.caseName, pdj.Spec.SchedulingPolicy,
				*pdj.Spec.SchedulingPolicy.MinAvailable, pdj.Spec.SchedulingPolicy.MinResources)
			assert.NotEmpty(t, pdj.Spec.Worker.Template.Spec.Containers[0].VolumeMounts)
		})
	}
}
