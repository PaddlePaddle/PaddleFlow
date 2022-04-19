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

	"github.com/ghodss/yaml"
	pdv1 "github.com/paddleflow/paddle-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/database/dbinit"
	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/job/api"
)

var (
	extensionPaddleYaml = `
apiVersion: batch.paddlepaddle.org/v1
kind: PaddleJob
metadata:
  name: default-name
spec:
  withGloo: 1
  intranet: PodIP
  cleanPodPolicy: OnCompletion
  worker:
    replicas: 2
    template:
      spec:
        containers:
          - name: paddle
            image: nginx
  ps:
    replicas: 2
    template:
      spec:
        containers:
          - name: paddle
            image: nginx
`
	mockPaddleJob = api.PFJob{
		ID:        "job-normal-0c272d0a",
		Name:      "",
		Namespace: "default",
		JobType:   schema.TypeDistributed,
		Framework: schema.FrameworkPaddle,
		UserName:  "root",
		QueueID:   "mockQueueID",
		Conf: schema.Conf{
			Name:    "normal",
			Command: "sleep 200",
			Image:   "mockImage",
			Env: map[string]string{
				"PF_JOB_MODE":           "Collective",
				"PF_FS_ID":              "fs-name_1",
				"PF_JOB_CLUSTER_ID":     "testClusterID",
				"PF_JOB_ID":             "",
				"PF_JOB_NAMESPACE":      "paddleflow",
				"PF_JOB_PRIORITY":       "NORMAL",
				"PF_JOB_QUEUE_ID":       "mockQueueID",
				"PF_JOB_FLAVOUR":        "cpu",
				"PF_JOB_WORKER_FLAVOUR": "cpu",
				"PF_JOB_WORKER_COMMAND": "sleep 3600",
				"PF_JOB_QUEUE_NAME":     "mockQueueName",
				//schema.EnvJobType:   string(schema.TypePodJob),
				"PF_USER_NAME": "root",
			},
			Flavour: schema.Flavour{Name: "mockFlavourName", ResourceInfo: schema.ResourceInfo{CPU: "3", Mem: "3"}},
		},
		Tasks: []models.Member{
			{
				ID:       "task-normal-0001",
				Replicas: 3,
				Role:     schema.RoleWorker,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
					Env: map[string]string{
						"PF_FS_ID":          "fs-name_1",
						"PF_JOB_CLUSTER_ID": "testClusterID",
						"PF_JOB_FLAVOUR":    "cpu",
						"PF_JOB_ID":         "",
						"PF_JOB_NAMESPACE":  "paddleflow",
						"PF_JOB_PRIORITY":   "NORMAL",
						"PF_JOB_QUEUE_ID":   "mockQueueID",
						"PF_JOB_QUEUE_NAME": "mockQueueName",
						schema.EnvJobType:   string(schema.TypePaddleJob),
						"PF_USER_NAME":      "root",
					},
					Flavour: schema.Flavour{Name: "cpu", ResourceInfo: schema.ResourceInfo{CPU: "2", Mem: "2"}},
				},
			},
		},
		ExtRuntimeConf: []byte(extensionPaddleYaml),
	}
	mockPaddlePSJob = api.PFJob{
		ID:        "job-normal-0001",
		Name:      "",
		Namespace: "default",
		JobType:   schema.TypeDistributed,
		Framework: schema.FrameworkPaddle,
		UserName:  "root",
		QueueID:   "mockQueueID",
		Conf:      schema.Conf{},
		Tasks: []models.Member{
			{
				ID:       "task-normal-0001",
				Replicas: 3,
				Role:     schema.RolePSWorker,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
					Env:     map[string]string{},
					Flavour: schema.Flavour{Name: "cpu", ResourceInfo: schema.ResourceInfo{CPU: "2", Mem: "2"}},
				},
			},
			{
				ID:       "task-normal-0002",
				Replicas: 3,
				Role:     schema.RolePSWorker,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
					Env:     map[string]string{},
					Flavour: schema.Flavour{Name: "cpu", ResourceInfo: schema.ResourceInfo{CPU: "4", Mem: "4"}},
				},
			},
		},
		ExtRuntimeConf: []byte(extensionPaddleYaml),
	}
)

func TestPaddleJob_CreateJob(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = "testSchedulerName"
	config.GlobalServerConfig.FlavourMap = map[string]schema.Flavour{
		"cpu": {
			Name: "cpu",
			ResourceInfo: schema.ResourceInfo{
				CPU: "1",
				Mem: "100M",
			},
		},
		"gpu": {
			Name: "gpu",
			ResourceInfo: schema.ResourceInfo{
				CPU: "1",
				Mem: "100M",
				ScalarResources: schema.ScalarResourcesType{
					"nvidia.com/gpu": "500M",
				},
			},
		},
	}

	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	dynamicClient := newFakeDynamicClient(server)
	// mock db
	dbinit.InitMockDB()
	// create kubernetes resource with dynamic client
	tests := []struct {
		caseName string
		jobObj   *api.PFJob
		wantErr  bool
		wantMsg  string
	}{
		{
			caseName: "pod_test1",
			jobObj: &api.PFJob{
				JobType: schema.TypeDistributed,
			},
			wantErr: true,
			wantMsg: "namespace is empty",
		},
		//{
		//	caseName: "pod_test2",
		//	jobObj:   &mockPaddleJob,
		//	wantErr:  false,
		//	wantMsg:  "",
		//},
		{
			caseName: "pod_test3",
			jobObj:   &mockPaddlePSJob,
			wantErr:  false,
			wantMsg:  "",
		},
	}

	for _, test := range tests {
		p, err := NewKubeJob(test.jobObj, dynamicClient)

		if test.wantErr {
			assert.NotNil(t, err)
			t.Logf("%s: create kube job failed, err: %v", test.caseName, err)
			continue
		}
		assert.NotNil(t, p)
		t.Logf("case[%s] to NewKubeJob=%+v", test.caseName, p)

		jobID, err := p.CreateJob()
		if test.wantErr && assert.Error(t, err) {
			assert.Equal(t, test.wantMsg, err.Error())
		} else if !test.wantErr && assert.NotEmpty(t, jobID) {
			t.Logf("case[%s] to CreateJob, paddleFlowJob=%+v", test.caseName, p)
			jobObj, err := Get(test.jobObj.Namespace, test.jobObj.ID, k8s.PaddleJobGVK, dynamicClient)
			if !assert.NoError(t, err) {
				t.Errorf(err.Error())
				continue
			}
			t.Logf("obj=%#v", jobObj)
			pdj := pdv1.PaddleJob{}
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(jobObj.Object, &pdj)
			assert.NoError(t, err)
			t.Logf("pdj=%#v", pdj)
			podYaml, err := yaml.Marshal(pdj)
			assert.NoError(t, err)
			t.Logf("%s", string(podYaml))
			t.Skipped()
		} else {
			t.Errorf("error case, %v", err)
		}
	}
}

func TestPatchPaddleJobVariable(t *testing.T) {
	confEnv := make(map[string]string)
	initConfigsForTest(confEnv)
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
		JobType:   schema.TypeDistributed,
		Framework: schema.FrameworkPaddle,
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

	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
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
			t.Logf("extRuntimeConf=%#v", extRuntimeConf)
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
