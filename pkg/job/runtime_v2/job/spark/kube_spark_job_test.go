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

package spark

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

var (
	extensionSparkYaml = `
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  creationTimestamp: null
  labels:
    owner: paddleflow
    paddleflow-job-id: job-normal-0c272d0a
  name: job-normal-0c272d0a
  namespace: default
spec:
  driver:
    podName: normal
  executor:
    instances: 1
  image: mockImage
  imagePullPolicy: IfNotPresent
  mainApplicationFile: null
  mode: cluster
  restartPolicy:
    onSubmissionFailureRetries: 3
    onSubmissionFailureRetryInterval: 5
    type: Never
  sparkVersion: 3.0.0
  type: Scala
`
	mockSparkJob = api.PFJob{
		ID:        "job-normal-0c272d0a",
		Name:      "",
		Namespace: "default",
		JobType:   schema.TypeDistributed,
		Framework: schema.FrameworkSpark,
		UserName:  "root",
		QueueID:   "mockQueueID",
		Conf: schema.Conf{
			Name:      "normal",
			Image:     "mockImage",
			Env:       map[string]string{},
			QueueName: "mockQueueName",
		},
		Tasks: []schema.Member{
			{
				ID:       "task-normal-0001",
				Replicas: 3,
				Role:     schema.RoleDriver,
				Conf: schema.Conf{
					Name:  "normal",
					Image: "mockImage",
					Env: map[string]string{
						schema.EnvJobType:           string(schema.TypeSparkJob),
						schema.EnvJobSparkMainFile:  "local:///opt/spark/examples/src/main/python/pi.py",
						schema.EnvJobSparkMainClass: "org.apache.spark.examples.SparkPi",
						schema.EnvJobSparkArguments: "a=b,c=d",
					},
					Flavour: schema.Flavour{
						Name: "",
						ResourceInfo: schema.ResourceInfo{
							CPU: "3",
							Mem: "3G",
						}},
				},
			},
			{
				ID:       "task-normal-0001",
				Replicas: 3,
				Role:     schema.RoleExecutor,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
					Env: map[string]string{
						schema.EnvJobType: string(schema.TypeSparkJob),
					},
					Flavour: schema.Flavour{
						Name: "",
						ResourceInfo: schema.ResourceInfo{
							CPU: "2",
							Mem: "2Gi",
							ScalarResources: schema.ScalarResourcesType{
								"nvidia.com/gpu": "2",
							},
						}},
				},
			},
		},
	}
	mockSparkJobWithYaml = api.PFJob{
		ID:        "job-mock-000002",
		Namespace: "default",
		JobType:   schema.TypeDistributed,
		Framework: schema.FrameworkSpark,
		UserName:  "root",
		QueueID:   "mockQueueID",
		Tasks: []schema.Member{
			{
				ID:       "task-normal-0001",
				Replicas: 3,
				Role:     schema.RoleDriver,
				Conf: schema.Conf{
					Name:  "normal",
					Image: "mockImage",
					Env: map[string]string{
						schema.EnvJobType: string(schema.TypeSparkJob),
					},
					Flavour: schema.Flavour{Name: "", ResourceInfo: schema.ResourceInfo{CPU: "1", Mem: "1G"}},
				},
			},
			{
				ID:       "task-normal-0001",
				Replicas: 3,
				Role:     schema.RoleExecutor,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
					Env: map[string]string{
						schema.EnvJobType: string(schema.TypeSparkJob),
					},
					Flavour: schema.Flavour{Name: "", ResourceInfo: schema.ResourceInfo{CPU: "2", Mem: "2Gi"}},
				},
			},
		},
		ExtensionTemplate: []byte(extensionSparkYaml),
	}
)

// TestSparkJob_CreateJob tests create spark app by single yaml
func TestSparkJob_CreateJob(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = "testSchedulerName"
	config.DefaultJobTemplate = map[string][]byte{
		"spark-job": []byte(extensionSparkYaml),
	}

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
			caseName: "create builtin spark job",
			jobObj:   &mockSparkJob,
			wantErr:  nil,
			wantMsg:  "",
		},
		{
			caseName: "with_custom_yaml",
			jobObj:   &mockSparkJobWithYaml,
			wantErr:  nil,
			wantMsg:  "",
		},
	}

	sparkJob := New(kubeRuntimeClient)
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			err := sparkJob.Submit(context.TODO(), test.jobObj)
			if test.wantErr == nil {
				assert.Equal(t, test.wantErr, err)
				t.Logf("case[%s] to CreateJob, paddleFlowJob=%+v", test.caseName, test.jobObj)
				_, err = kubeRuntimeClient.Get(test.jobObj.Namespace, test.jobObj.ID, schema.SparkKindGroupVersion)
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

func TestKubeSparkJob_JobStatus(t *testing.T) {
	testCases := []struct {
		name       string
		obj        interface{}
		wantStatus schema.JobStatus
		wantErr    error
	}{
		{
			name: "spark app is pending",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.SparkKindGroupVersion.Kind,
					"apiVersion": schema.SparkKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"applicationState": map[string]interface{}{
							"state":        v1beta2.SubmittedState,
							"errorMessage": "",
						},
					},
				},
			},
			wantStatus: schema.StatusJobPending,
			wantErr:    nil,
		},
		{
			name: "spark app is running",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.SparkKindGroupVersion.Kind,
					"apiVersion": schema.SparkKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"applicationState": map[string]interface{}{
							"state":        v1beta2.RunningState,
							"errorMessage": "",
						},
					},
				},
			},
			wantStatus: schema.StatusJobRunning,
			wantErr:    nil,
		},
		{
			name: "spark app is success",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.SparkKindGroupVersion.Kind,
					"apiVersion": schema.SparkKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"applicationState": map[string]interface{}{
							"state":        v1beta2.CompletedState,
							"errorMessage": "",
						},
					},
				},
			},
			wantStatus: schema.StatusJobSucceeded,
			wantErr:    nil,
		},
		{
			name: "spark app is failed",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.SparkKindGroupVersion.Kind,
					"apiVersion": schema.SparkKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"applicationState": map[string]interface{}{
							"state":        v1beta2.FailedState,
							"errorMessage": "",
						},
					},
				},
			},
			wantStatus: schema.StatusJobFailed,
			wantErr:    nil,
		},
		{
			name: "spark app status is unknown",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.SparkKindGroupVersion.Kind,
					"apiVersion": schema.SparkKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"applicationState": map[string]interface{}{
							"state":        "xxx",
							"errorMessage": "",
						},
					},
				},
			},
			wantStatus: "",
			wantErr:    fmt.Errorf("unexpected spark application status: xxx"),
		},
	}

	sparkJob := KubeSparkJob{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sparkStatus, err := sparkJob.JobStatus(tc.obj)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantStatus, sparkStatus.Status)
			t.Logf("spark job status: %v", sparkStatus)
		})
	}
}
