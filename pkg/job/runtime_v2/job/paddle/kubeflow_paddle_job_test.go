/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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
	"fmt"
	"testing"

	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"net/http/httptest"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

var (
	extPaddleJobKubeflowV1ColletiveYaml = `apiVersion: "kubeflow.org/v1"
kind: PaddleJob
metadata:
  name: paddle-simple-gpu
  namespace: kubeflow
spec:
  paddleReplicaSpecs:
    Worker:
      replicas: 2
      restartPolicy: OnFailure
      template:
        spec:
          containers:
            - name: paddle
              image: registry.baidubce.com/paddlepaddle/paddle:2.4.0rc0-gpu-cuda11.2-cudnn8.1-trt8.0
              command:
                - python
              args:
                - "-m"
                - paddle.distributed.launch
                - "run_check"
              ports:
                - containerPort: 37777
                  name: master
              imagePullPolicy: Always
              resources:
                  limits:
                      nvidia.com/gpu: 2
              volumeMounts:
                  - mountPath: /dev/shm
                    name: dshm
          volumes:
            - name: dshm
              emptyDir:
                medium: Memory
`
)

func TestKubeKFPaddleJob_CreateJob(t *testing.T) {
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
	testCases := []struct {
		name    string
		job     *api.PFJob
		wantErr error
		wantMsg string
	}{
		{
			name: "create builtin kubeflow paddle job success",
			job: &api.PFJob{
				ID:        "job-builtin-1",
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkPaddle,
				JobMode:   schema.EnvJobModeCollective,
				UserName:  "root",
				QueueID:   "mockQueueID",
				Conf: schema.Conf{
					KindGroupVersion: schema.KFPaddleKindGroupVersion,
				},
				Tasks: []schema.Member{
					{
						Replicas: 2,
						Role:     schema.RoleWorker,
						Conf: schema.Conf{
							Flavour: schema.Flavour{
								Name: "",
								ResourceInfo: schema.ResourceInfo{
									CPU: "1",
									Mem: "2Gi",
								},
							},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "create custom kubeflow paddle job success",
			job: &api.PFJob{
				ID:        "job-custom-1",
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkPaddle,
				JobMode:   schema.EnvJobModeCollective,
				UserName:  "root",
				QueueID:   "mockQueueID",
				Conf: schema.Conf{
					KindGroupVersion: schema.KFPaddleKindGroupVersion,
				},
				Tasks:             []schema.Member{},
				ExtensionTemplate: []byte(extPaddleJobKubeflowV1ColletiveYaml),
			},
			wantErr: nil,
		},
		{
			name: "create builtin kubeflow paddle job failed",
			job: &api.PFJob{
				ID:        "job-builtin-2",
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkPaddle,
				JobMode:   "xxx",
				UserName:  "root",
				QueueID:   "mockQueueID",
				Conf: schema.Conf{
					KindGroupVersion: schema.KFPaddleKindGroupVersion,
				},
				Tasks: []schema.Member{},
			},
			wantErr: fmt.Errorf("get default template failed, err: job template PaddleJob-kubeflow.org/v1-xxx is not found"),
		},
		{
			name: "create builtin kubeflow paddle job failed, when resources is invalid",
			job: &api.PFJob{
				ID:        "job-builtin-1",
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkPaddle,
				JobMode:   schema.EnvJobModeCollective,
				UserName:  "root",
				QueueID:   "mockQueueID",
				Conf: schema.Conf{
					KindGroupVersion: schema.KFPaddleKindGroupVersion,
				},
				Tasks: []schema.Member{
					{
						Replicas: 2,
						Role:     schema.RoleWorker,
						Conf: schema.Conf{
							Flavour: schema.Flavour{
								Name: "",
								ResourceInfo: schema.ResourceInfo{
									CPU: "1",
									Mem: "2x",
								},
							},
						},
					},
				},
			},
			wantErr: fmt.Errorf("quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'"),
		},
	}

	paddleJob := KFNew(kubeRuntimeClient)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := paddleJob.Submit(context.TODO(), tc.job)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestKubeKFPaddleJob_JobStatus(t *testing.T) {
	testCases := []struct {
		name    string
		obj     interface{}
		wantErr error
	}{
		{
			name: "kubeflow paddle job pending",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.KFPaddleKindGroupVersion.Kind,
					"apiVersion": schema.KFPaddleKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"conditions": []map[string]interface{}{
							{
								"type":    kubeflowv1.JobCreated,
								"message": "kubeflow paddle job is created",
							},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "kubeflow paddle job running",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.KFPaddleKindGroupVersion.Kind,
					"apiVersion": schema.KFPaddleKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"conditions": []map[string]interface{}{
							{
								"type":    kubeflowv1.JobRunning,
								"message": "kubeflow paddle job is running",
							},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "kubeflow paddle job status is invalid",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.KFPaddleKindGroupVersion.Kind,
					"apiVersion": schema.KFPaddleKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"conditions": []map[string]interface{}{
							{
								"type":    "xxx",
								"message": "kubeflow paddle job status is unknown",
							},
						},
					},
				},
			},
			wantErr: fmt.Errorf("unexpected job status: xxx"),
		},
	}

	paddleJob := KubeKFPaddleJob{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status, err := paddleJob.JobStatus(tc.obj)
			assert.Equal(t, tc.wantErr, err)
			t.Logf("job status: %v", status)
		})
	}

}
