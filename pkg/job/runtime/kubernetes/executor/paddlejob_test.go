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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
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
				// schema.EnvJobType:   string(schema.TypePodJob),
				"PF_USER_NAME": "root",
			},
			Flavour: schema.Flavour{Name: "mockFlavourName", ResourceInfo: schema.ResourceInfo{CPU: "3", Mem: "3"}},
		},
		Tasks: []schema.Member{
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
		ExtensionTemplate: []byte(extensionPaddleYaml),
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
		Tasks: []schema.Member{
			{
				ID:       "task-normal-0001",
				Replicas: 3,
				Role:     schema.RolePWorker,
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
				Role:     schema.RolePWorker,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
					Env:     map[string]string{},
					Flavour: schema.Flavour{Name: "cpu", ResourceInfo: schema.ResourceInfo{CPU: "4", Mem: "4"}},
				},
			},
		},
		ExtensionTemplate: []byte(extensionPaddleYaml),
	}
)

func TestPaddleJob_CreateJob(t *testing.T) {
	initGlobalServerConfig()
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	dynamicClient := newFakeDynamicClient(server)
	// mock db
	driver.InitMockDB()
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
		{
			caseName: "pod_test2",
			jobObj:   &mockPaddleJob,
			wantErr:  false,
			wantMsg:  "",
		},
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
