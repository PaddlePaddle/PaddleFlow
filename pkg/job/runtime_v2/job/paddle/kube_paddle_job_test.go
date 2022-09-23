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

package paddle

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
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
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = "testSchedulerName"
	config.GlobalServerConfig.Job.DefaultJobYamlDir = "../../../../../config/server/default/job"

	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeRuntimeClient := client.NewFakeKubeRuntimeClient(server)
	// mock db
	driver.InitMockDB()
	// create kubernetes resource with dynamic client
	tests := []struct {
		caseName string
		jobObj   *api.PFJob
		wantErr  error
		wantMsg  string
	}{
		{
			caseName: "create paddle job failed",
			jobObj: &api.PFJob{
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkPaddle,
				JobMode:   "xx",
			},
			wantErr: errors.New("get default template failed, err: get job file from path[../../../../../config/server/default/job/paddle_xx.yaml] failed"),
			wantMsg: "namespace is empty",
		},
		{
			caseName: "create paddle job with Collective mode",
			jobObj:   &mockPaddleJob,
			wantErr:  nil,
			wantMsg:  "",
		},
		{
			caseName: "job test3",
			jobObj:   &mockPaddlePSJob,
			wantErr:  nil,
			wantMsg:  "",
		},
	}

	paddleJob := New(kubeRuntimeClient)
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			err := paddleJob.Submit(context.TODO(), test.jobObj)
			if test.wantErr == nil {
				assert.Equal(t, test.wantErr, err)
				t.Logf("case[%s] to CreateJob, paddleFlowJob=%+v", test.caseName, test.jobObj)
				_, err := kubeRuntimeClient.Get(test.jobObj.Namespace, test.jobObj.ID, KubePaddleFwVersion)
				if !assert.NoError(t, err) {
					t.Errorf(err.Error())
				}
			} else {
				assert.NotNil(t, err)
				assert.Equal(t, test.wantErr.Error(), err.Error())
			}
		})
	}
}
