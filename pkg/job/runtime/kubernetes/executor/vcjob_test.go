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

package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	vcjob "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
)

var (
	psTasks = []schema.Member{
		{
			ID:       "task-normal-0001",
			Replicas: 3,
			Role:     schema.RolePServer,
			Conf: schema.Conf{
				Name:    "normal",
				Command: "sleep 200",
				Image:   "mockImage",
				Env:     nil,
				Flavour: schema.Flavour{Name: "cpu", ResourceInfo: schema.ResourceInfo{CPU: "2", Mem: "2"}},
			},
		},
		{
			ID:       "task-normal-0001",
			Replicas: 3,
			Role:     schema.RolePWorker,
			Conf: schema.Conf{
				Name:    "normal",
				Command: "sleep 200",
				Image:   "mockImage",
				Env:     nil,
				Flavour: schema.Flavour{Name: "cpu", ResourceInfo: schema.ResourceInfo{CPU: "2", Mem: "2"}},
			},
		},
	}
	collectiveTask = []schema.Member{
		{
			ID:       "task-normal-0001",
			Replicas: 3,
			Role:     schema.RoleWorker,
			Conf: schema.Conf{
				Name:    "normal",
				Command: "sleep 200",
				Image:   "mockImage",
				Env:     nil,
				Flavour: schema.Flavour{Name: "cpu", ResourceInfo: schema.ResourceInfo{CPU: "2", Mem: "2"}},
			},
		},
	}
)

// deprecated
func initConfigsForTest(confEnv map[string]string) {
	initGlobalServerConfig()
	confEnv[schema.EnvJobType] = string(schema.TypeVcJob)
	confEnv[schema.EnvJobNamespace] = "N1"
	confEnv[schema.EnvJobFlavour] = "cpu"
	confEnv[schema.EnvJobPServerReplicas] = "3"
	confEnv[schema.EnvJobFsID] = "fs-root-test"
	confEnv[schema.EnvJobPVCName] = "test-pvc-name"
}

func TestPatchVCJobVariable(t *testing.T) {
	confEnv := make(map[string]string)
	initConfigsForTest(confEnv)
	// init for vcJob's ps mode
	confEnv[schema.EnvJobPServerCommand] = "sleep 30"
	confEnv[schema.EnvJobWorkerCommand] = "sleep 30"
	confEnv[schema.EnvJobPServerReplicas] = "2"
	confEnv[schema.EnvJobWorkerReplicas] = "2"
	confEnv[schema.EnvJobPServerFlavour] = "ss"
	confEnv[schema.EnvJobWorkerFlavour] = "ss"

	pfjob := &api.PFJob{
		Conf: schema.Conf{
			Name:    "confName",
			Env:     confEnv,
			Command: "sleep 3600",
			Image:   "nginx",
		},
		JobType: schema.TypeVcJob,
	}

	tests := []struct {
		caseName      string
		vcJobMode     string
		additionalEnv map[string]string
		actualValue   *vcjob.Job
		expectValue   string
		errMsg        string
	}{
		{
			caseName:    "psMode",
			vcJobMode:   schema.EnvJobModePS,
			actualValue: &vcjob.Job{},
			expectValue: "normal",
		},
		{
			caseName:    "podMode",
			vcJobMode:   schema.EnvJobModePod,
			actualValue: &vcjob.Job{},
			expectValue: "container",
		},
		{
			caseName:    "collectiveMode",
			vcJobMode:   schema.EnvJobModeCollective,
			actualValue: &vcjob.Job{},
			expectValue: "normal",
		},
		{
			caseName:  "fromUserPath",
			vcJobMode: schema.EnvJobModePod,
			additionalEnv: map[string]string{
				schema.EnvJobNamespace: "N2",
			},
			actualValue: &vcjob.Job{},
			expectValue: "container",
		},
	}

	for _, test := range tests {
		if len(test.additionalEnv) != 0 {
			for k, v := range test.additionalEnv {
				pfjob.Conf.SetEnv(k, v)
			}
		}
		pfjob.JobMode = test.vcJobMode
		// create Job
		kubeJob := KubeJob{
			ID:               test.caseName,
			Name:             "randomName",
			Namespace:        "namespace",
			JobType:          schema.TypeVcJob,
			JobMode:          test.vcJobMode,
			GroupVersionKind: k8s.VCJobGVK,
			Image:            pfjob.Conf.GetImage(),
			Command:          pfjob.Conf.GetCommand(),
			Env:              pfjob.Conf.GetEnv(),
			Priority:         pfjob.Conf.GetPriority(),
			QueueName:        pfjob.Conf.GetQueueName(),
			Tasks: []schema.Member{{Conf: schema.Conf{Flavour: schema.Flavour{
				ResourceInfo: schema.ResourceInfo{
					CPU: "1",
					Mem: "1Gi",
				},
			}}}},
		}
		// yaml content
		yamlTemplateContent, err := kubeJob.getDefaultTemplate(pfjob.Framework)
		if err != nil {
			t.Errorf(err.Error())
		}
		kubeJob.YamlTemplateContent = yamlTemplateContent

		if test.vcJobMode == schema.EnvJobModePS {
			kubeJob.Tasks = psTasks
		} else if test.vcJobMode == schema.EnvJobModeCollective {
			kubeJob.Tasks = collectiveTask
		}
		jobModeParams := JobModeParams{
			JobFlavour:      pfjob.Conf.GetFlavour(),
			PServerReplicas: pfjob.Conf.GetPSReplicas(),
			PServerFlavour:  pfjob.Conf.GetFlavour(),
			PServerCommand:  pfjob.Conf.GetPSCommand(),
			WorkerReplicas:  pfjob.Conf.GetWorkerReplicas(),
			WorkerFlavour:   pfjob.Conf.GetWorkerFlavour(),
			WorkerCommand:   pfjob.Conf.GetWorkerCommand(),
		}
		vcJob := VCJob{
			KubeJob:       kubeJob,
			JobModeParams: jobModeParams,
		}
		jobApp := test.actualValue
		if err := vcJob.createJobFromYaml(jobApp); err != nil {
			t.Errorf("create job failed, err %v", err)
		}

		// patch
		err = vcJob.patchVCJobVariable(jobApp, test.caseName)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Logf("case[%s] jobApp=%+v", test.caseName, *jobApp)

		assert.NotEmpty(t, jobApp.Spec.Tasks)
		assert.NotEmpty(t, jobApp.Spec.Tasks[0].Template.Spec.Containers)
		assert.Equal(t, test.expectValue, jobApp.Spec.Tasks[0].Template.Spec.Containers[0].Name)
	}
}
