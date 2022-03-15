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

	sparkapp "paddleflow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/job/api"
)

func initConfigsForTest(confEnv map[string]string) {
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.DefaultJobYamlDir = "../../../../../config/server/default/job"
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
		"cpu": {
			Name: "cpu",
			ResourceInfo: schema.ResourceInfo{
				Cpu: "1",
				Mem: "100M",
			},
		},
		"gpu": {
			Name: "gpu",
			ResourceInfo: schema.ResourceInfo{
				Cpu: "1",
				Mem: "100M",
				ScalarResources: schema.ScalarResourcesType{
					"nvidia.com/gpu": "500M",
				},
			},
		},
	}
	confEnv[schema.EnvJobFlavour] = "ss"
	confEnv[schema.EnvJobPServerReplicas] = "3"
	confEnv[schema.EnvJobFsID] = "fs-root-test"
	confEnv[schema.EnvJobPVCName] = "test-pvc-name"
}

func TestPatchSparkAppVariable(t *testing.T) {
	confEnv := make(map[string]string)
	initConfigsForTest(confEnv)

	confEnv[schema.EnvJobType] = "spark"
	confEnv[schema.EnvJobNamespace] = "N1"

	pfjob := &api.PFJob{
		Conf: schema.Conf{
			Name:    "confName",
			Env:     confEnv,
			Command: "sleep 3600",
			Image:   "nginx",
		},
		JobType: schema.TypeSparkJob,
	}

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
				pfjob.Conf.SetEnv(k, v)
			}
		}
		if pfjob.Conf.GetNamespace() != "" {
			pfjob.Namespace = pfjob.Conf.GetNamespace()
		}
		// yaml content
		extRuntimeConf, err := pfjob.GetExtRuntimeConf(pfjob.Conf.GetFS(), pfjob.Conf.GetYamlPath())
		if err != nil {
			t.Errorf(err.Error())
		}
		kubeJob := KubeJob{
			ID:                  test.caseName,
			Name:                "randomName",
			Namespace:           pfjob.Namespace,
			JobType:             schema.TypeSparkJob,
			Image:               pfjob.Conf.GetImage(),
			Command:             pfjob.Conf.GetCommand(),
			Env:                 pfjob.Conf.GetEnv(),
			VolumeName:          pfjob.Conf.GetFS(),
			PVCName:             "PVCName",
			Priority:            pfjob.Conf.GetPriority(),
			QueueName:           pfjob.Conf.GetQueueName(),
			YamlTemplateContent: extRuntimeConf,
		}

		sparkJob := SparkJob{
			KubeJob:          kubeJob,
			SparkMainFile:    "",
			SparkMainClass:   "",
			SparkArguments:   "",
			DriverFlavour:    "",
			ExecutorFlavour:  "",
			ExecutorReplicas: "",
		}
		jobApp := test.actualValue
		if err := sparkJob.createJobFromYaml(jobApp); err != nil {
			t.Errorf("create job failed, err %v", err)
		}
		err = sparkJob.patchSparkAppVariable(jobApp, test.caseName)
		if err != nil {
			t.Errorf(err.Error())
		}

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
