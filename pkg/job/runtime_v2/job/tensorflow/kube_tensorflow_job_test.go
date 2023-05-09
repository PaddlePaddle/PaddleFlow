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

package tensorflow

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

var tfJobYaml = `apiVersion: "kubeflow.org/v1"
kind: "TFJob"
metadata:
  name: "dist-mnist-for-e2e-test"
spec:
  tfReplicaSpecs:
    PS:
      replicas: 2
      restartPolicy: Never
      template:
        spec:
          containers:
            - name: tensorflow
              image: kubeflow/tf-dist-mnist-test:latest
    Worker:
      replicas: 4
      restartPolicy: Never
      template:
        spec:
          containers:
            - name: tensorflow
              image: kubeflow/tf-dist-mnist-test:latest
`

func TestTFJob_CreateJob(t *testing.T) {
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
		caseName  string
		jobObj    *api.PFJob
		expectErr error
		wantErr   bool
		wantMsg   string
	}{
		{
			caseName: "create builtin job successfully",
			jobObj: &api.PFJob{
				Name:      "test-tf-job",
				ID:        "job-test-tf",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				JobMode:   schema.EnvJobModePS,
				Framework: schema.FrameworkTF,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
					Env:     map[string]string{},
				},
				Tasks: []schema.Member{
					{
						Replicas: 1,
						Role:     schema.RoleMaster,
						Conf: schema.Conf{
							Flavour: schema.Flavour{Name: "", ResourceInfo: schema.ResourceInfo{CPU: "4", Mem: "4Gi"}},
						},
					},
					{
						Replicas: 2,
						Role:     schema.RoleWorker,
						Conf: schema.Conf{
							Flavour: schema.Flavour{Name: "", ResourceInfo: schema.ResourceInfo{CPU: "4", Mem: "4Gi"}},
						},
					},
				},
			},
			expectErr: nil,
			wantErr:   false,
		},
		{
			caseName: "create custom tf job successfully",
			jobObj: &api.PFJob{
				Name:      "test-custom-tf-job",
				ID:        "job-test-custom-tf",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				JobMode:   schema.EnvJobModePS,
				Framework: schema.FrameworkTF,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
					Env:     map[string]string{},
				},
				Tasks:             []schema.Member{},
				ExtensionTemplate: []byte(tfJobYaml),
			},
			expectErr: nil,
			wantErr:   false,
		},
	}

	tfJob := New(kubeRuntimeClient)
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			err := tfJob.Submit(context.TODO(), test.jobObj)
			assert.Equal(t, test.expectErr, err)
			if err != nil {
				t.Logf("create job failed, err: %v", err)
			} else {
				jobObj, err := kubeRuntimeClient.Get(test.jobObj.Namespace, test.jobObj.ID, schema.TFKindGroupVersion)
				if err != nil {
					t.Errorf(err.Error())
				} else {
					t.Logf("obj=%#v", jobObj)
				}
			}
		})
	}
}

func TestKubeTFJob_JobStatus(t *testing.T) {
	testCases := []struct {
		name       string
		obj        interface{}
		wantStatus schema.JobStatus
		wantErr    error
	}{
		{
			name: "tensorflow job is pending",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.TFKindGroupVersion.Kind,
					"apiVersion": schema.TFKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"conditions": []map[string]interface{}{
							{
								"type":    kubeflowv1.JobCreated,
								"message": "kubeflow tensorflow job is created",
							},
						},
					},
				},
			},
			wantStatus: schema.StatusJobPending,
			wantErr:    nil,
		},
		{
			name: "tensorflow job is running",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.TFKindGroupVersion.Kind,
					"apiVersion": schema.TFKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"conditions": []map[string]interface{}{
							{
								"type":    kubeflowv1.JobRunning,
								"message": "kubeflow tensorflow job is running",
							},
						},
					},
				},
			},
			wantStatus: schema.StatusJobRunning,
			wantErr:    nil,
		},
		{
			name: "tensorflow job status is invalid",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.TFKindGroupVersion.Kind,
					"apiVersion": schema.TFKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"conditions": []map[string]interface{}{
							{
								"type":    "xxx",
								"message": "kubeflow tensorflow job status is unknown",
							},
						},
					},
				},
			},
			wantStatus: "",
			wantErr:    fmt.Errorf("unexpected job status: xxx"),
		},
	}

	tfJob := KubeTFJob{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status, err := tfJob.JobStatus(tc.obj)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantStatus, status.Status)
			t.Logf("tensorflow job status %s", status)
		})
	}
}
