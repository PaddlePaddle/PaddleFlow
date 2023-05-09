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
	"fmt"
	"net/http/httptest"
	"testing"

	paddlejobv1 "github.com/paddleflow/paddle-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

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
            resources:
              limits:
                cpu: "1"
                memory: 1Gi
  ps:
    replicas: 2
    template:
      spec:
        containers:
          - name: paddle
            image: nginx
            resources:
              limits:
                cpu: "2"
                memory: 2Gi
              requests:
                cpu: "2"
                memory: 2Gi
`
	nilWorkerContainerYaml = `
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

  ps:
    replicas: 2
    template:
      spec:
        containers:
          - name: paddle
            image: nginx
            resources:
              limits:
                cpu: "1"
                memory: 1Gi
              requests:
                cpu: "1"
                memory: 1Gi
`
	nilPSContainerYaml = `
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
            resources:
              limits:
                cpu: "1"
                memory: 1Gi
              requests:
                cpu: "1"
                memory: 1Gi
  ps:
    replicas: 2
`
	extensionPaddleYamlNilPS = `
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
            resources:
              limits:
                cpu: "1"
                memory: 1Gi
              requests:
                cpu: "1"
                memory: 1Gi
`
	extensionPaddleYamlNilWorker = `
apiVersion: batch.paddlepaddle.org/v1
kind: PaddleJob
metadata:
  name: default-name
spec:
  withGloo: 1
  intranet: PodIP
  cleanPodPolicy: OnCompletion
  ps:
    replicas: 2
    template:
      spec:
        containers:
          - name: paddle
            image: nginx
            resources:
              limits:
                cpu: "1"
                memory: 1Gi
              requests:
                cpu: "1"
                memory: 1Gi
`
	mockPaddleJob = api.PFJob{
		ID:        "job-normal-0c272d0a",
		Name:      "",
		Namespace: "default",
		JobType:   schema.TypeDistributed,
		Framework: schema.FrameworkPaddle,
		JobMode:   schema.EnvJobModeCollective,
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
				"PF_USER_NAME":          "root",
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
		JobMode:   schema.EnvJobModePS,
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
	defaultJobYamlPath := "../../../../../config/server/default/job/job_template.yaml"
	config.InitJobTemplate(defaultJobYamlPath)

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
			wantErr: errors.New("get default template failed, err: job template paddle-xx-job is not found"),
			wantMsg: "namespace is empty",
		},
		{
			caseName: "extensionTemplate NilWorker",
			jobObj: &api.PFJob{
				ID:        "job-normal-0c272d0b",
				Name:      "",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkPaddle,
				JobMode:   schema.EnvJobModeCollective,
				UserName:  "root",
				QueueID:   "mockQueueID",
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
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
				ExtensionTemplate: []byte(extensionPaddleYamlNilWorker),
			},
			wantErr: errors.New("worker is required in paddleJob"),
		},
		{
			caseName: "extensionTemplate NilWorker",
			jobObj: &api.PFJob{
				ID:        "job-normal-0c272d0b",
				Name:      "",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkPaddle,
				JobMode:   schema.EnvJobModeCollective,
				UserName:  "root",
				QueueID:   "mockQueueID",
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
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
							Flavour: schema.Flavour{Name: "cpu", ResourceInfo: schema.ResourceInfo{CPU: "-2", Mem: "2"}},
						},
					},
				},
				ExtensionTemplate: []byte(extensionPaddleYaml),
			},
			wantErr: errors.New("negative resources not permitted: map[cpu:-2 memory:2]"),
		},
		{
			caseName: "extensionTemplate NilPS",
			jobObj: &api.PFJob{
				ID:        "job-normal-0c272d0c",
				Name:      "",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkPaddle,
				JobMode:   schema.EnvJobModePS,
				UserName:  "root",
				QueueID:   "mockQueueID",
				Tasks: []schema.Member{
					{
						ID:       "task-normal-0001",
						Replicas: 1,
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
					{
						ID:       "task-normal-0001",
						Replicas: 1,
						Role:     schema.RolePServer,
					},
				},
				ExtensionTemplate: []byte(extensionPaddleYamlNilPS),
			},
			wantErr: errors.New("PS mode required spec.PS"),
		},
		{
			caseName: "extensionTemplate NilWorkerContainer",
			jobObj: &api.PFJob{
				ID:        "job-normal-0c272d0c",
				Name:      "",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkPaddle,
				JobMode:   schema.EnvJobModePS,
				UserName:  "root",
				QueueID:   "mockQueueID",
				Tasks: []schema.Member{
					{
						ID:       "task-normal-0001",
						Replicas: 1,
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
					{
						ID:       "task-normal-0001",
						Replicas: 1,
						Role:     schema.RolePServer,
					},
				},
				ExtensionTemplate: []byte(nilWorkerContainerYaml),
			},
			wantErr: errors.New("container is required in paddleJob"),
		},
		{
			caseName: "extensionTemplate NilWorkerContainer",
			jobObj: &api.PFJob{
				ID:        "job-normal-0c272d0c",
				Name:      "",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkPaddle,
				JobMode:   schema.EnvJobModePS,
				UserName:  "root",
				QueueID:   "mockQueueID",
				Tasks: []schema.Member{
					{
						ID:       "task-normal-0001",
						Replicas: 1,
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
					{
						ID:       "task-normal-0001",
						Replicas: 1,
						Role:     schema.RolePServer,
					},
				},
				ExtensionTemplate: []byte(nilPSContainerYaml),
			},
			wantErr: errors.New("container is required in paddleJob"),
		},
		{
			caseName: "extensionTemplate member has no flavour",
			jobObj: &api.PFJob{
				ID:        "job-normal-0c272d0c",
				Name:      "",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkPaddle,
				JobMode:   schema.EnvJobModePS,
				UserName:  "root",
				QueueID:   "mockQueueID",
				Tasks: []schema.Member{
					{
						ID:       "task-normal-0001",
						Replicas: 1,
						Role:     schema.RoleWorker,
						Conf: schema.Conf{
							Name:    "normal",
							Command: "sleep 200",
							Image:   "mockImage",
						},
					},
					{
						ID:       "task-normal-0001",
						Replicas: 1,
						Role:     schema.RolePServer,
					},
				},
				ExtensionTemplate: []byte(extensionPaddleYaml),
			},
			wantErr: nil,
		},
		{
			caseName: "create paddle job with Collective mode",
			jobObj:   &mockPaddleJob,
			wantErr:  nil,
			wantMsg:  "",
		},
		{
			caseName: "job test ps mode",
			jobObj:   &mockPaddlePSJob,
			wantErr:  nil,
			wantMsg:  "",
		},
	}

	paddleJob := New(kubeRuntimeClient)
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			t.Logf("run case[%s]", test.caseName)
			err := paddleJob.Submit(context.TODO(), test.jobObj)
			if test.wantErr == nil {
				assert.Equal(t, test.wantErr, err)
				t.Logf("case[%s] to CreateJob, paddleFlowJob=%+v", test.caseName, test.jobObj)
				obj, err := kubeRuntimeClient.Get(test.jobObj.Namespace, test.jobObj.ID, schema.PaddleKindGroupVersion)
				if !assert.NoError(t, err) {
					t.Errorf(err.Error())
				}
				t.Logf("result: %v", obj)
			} else {
				if assert.Error(t, err) {
					assert.Equal(t, test.wantErr.Error(), err.Error())
				}
			}
		})
	}
}

func TestKubePaddleJob_JobStatus(t *testing.T) {
	testCases := []struct {
		name       string
		obj        interface{}
		wantStatus schema.JobStatus
		wantErr    error
	}{
		{
			name: "paddle job is pending",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.PaddleKindGroupVersion.Kind,
					"apiVersion": schema.PaddleKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": paddlejobv1.Pending,
					},
				},
			},
			wantStatus: schema.StatusJobPending,
			wantErr:    nil,
		},
		{
			name: "paddle job is running",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.PaddleKindGroupVersion.Kind,
					"apiVersion": schema.PaddleKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": paddlejobv1.Running,
					},
				},
			},
			wantStatus: schema.StatusJobRunning,
			wantErr:    nil,
		},
		{
			name: "paddle job is terminating",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.PaddleKindGroupVersion.Kind,
					"apiVersion": schema.PaddleKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": paddlejobv1.Terminating,
					},
				},
			},
			wantStatus: schema.StatusJobTerminating,
			wantErr:    nil,
		},
		{
			name: "paddle job is terminated",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.PaddleKindGroupVersion.Kind,
					"apiVersion": schema.PaddleKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": paddlejobv1.Aborted,
					},
				},
			},
			wantStatus: schema.StatusJobTerminated,
			wantErr:    nil,
		},
		{
			name: "paddle job is success",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.PaddleKindGroupVersion.Kind,
					"apiVersion": schema.PaddleKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": paddlejobv1.Succeed,
					},
				},
			},
			wantStatus: schema.StatusJobSucceeded,
			wantErr:    nil,
		},
		{
			name: "paddle job is failed",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.PaddleKindGroupVersion.Kind,
					"apiVersion": schema.PaddleKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": paddlejobv1.Failed,
					},
				},
			},
			wantStatus: schema.StatusJobFailed,
			wantErr:    nil,
		},
		{
			name: "paddle job status is unknown",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.PaddleKindGroupVersion.Kind,
					"apiVersion": schema.PaddleKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": "xxx",
					},
				},
			},
			wantStatus: "",
			wantErr:    fmt.Errorf("unexpected paddlejob status: xxx"),
		},
	}

	paddleJob := KubePaddleJob{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status, err := paddleJob.JobStatus(tc.obj)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantStatus, status.Status)
			t.Logf("paddle job status: %v", status)
		})
	}
}
