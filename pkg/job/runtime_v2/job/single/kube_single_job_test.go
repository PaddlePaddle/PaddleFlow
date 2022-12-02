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

package single

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

var (
	taskImage             = "busybox:v1"
	taskCPU               = "2"
	taskMemory            = "2"
	extensionTemplateYaml = `
apiVersion: v1
kind: Pod
metadata:
  creationTimestamp: null
  labels:
    volcano.sh/queue-name: mockQueueName
  name: job-normal-0c272d0a
  namespace: default
spec:
  containers:
  - command:
    - bash
    - -c
    - cd /home/paddleflow/storage/mnt;sleep 300
    env:
    - name: PF_JOB_FLAVOUR
      value: mockFlavourName0
    image: mockImage
    name: job-normal-0c272d0a
    resources:
      limits:
        cpu: "1"
        memory: "1"
      requests:
        cpu: "1"
        memory: "1"
    volumeMounts:
    - mountPath: /home/paddleflow/storage/mnt
      name: fs-name_1
  priorityClassName: normal
  schedulerName: testSchedulerName
  volumes:
  - name: fs-name_1
    persistentVolumeClaim:
      claimName: pfs-fs-name_1-pvc
status: {}
`
	extensionTemplateYamlNilContainer = `
apiVersion: v1
kind: Pod
metadata:
  creationTimestamp: null
  labels:
    volcano.sh/queue-name: mockQueueName
  name: job-normal-0c272d0b
  namespace: default
spec:
  priorityClassName: normal
  schedulerName: testSchedulerName
  volumes:
  - name: fs-name_1
    persistentVolumeClaim:
      claimName: pfs-fs-name_1-pvc
status: {}
`
	mockSinglePod = api.PFJob{
		ID:        "job-normal-0c272d0a",
		Name:      "job-normal-0c272d0a",
		Namespace: "default",
		JobType:   schema.TypeSingle,
		UserName:  "root",
		QueueID:   "mockQueueID",
		Conf: schema.Conf{
			Name:    "normal",
			Command: "sleep 200",
			Image:   "mockImage",
			Env: map[string]string{
				"PF_FS_ID":          "fs-name_1",
				"PF_JOB_CLUSTER_ID": "testClusterID",
				"PF_JOB_FLAVOUR":    "mockFlavourName",
				"PF_JOB_ID":         "",
				"PF_JOB_NAMESPACE":  "paddleflow",
				"PF_JOB_PRIORITY":   "NORMAL",
				"PF_JOB_QUEUE_ID":   "mockQueueID",
				"PF_JOB_QUEUE_NAME": "mockQueueName",
				schema.EnvJobType:   string(schema.TypePodJob),
				"PF_USER_NAME":      "root",
			},
			Flavour: schema.Flavour{Name: "mockFlavourName", ResourceInfo: schema.ResourceInfo{CPU: taskCPU, Mem: taskMemory}},
		},
		Tasks: []schema.Member{
			{
				Replicas: 1,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   taskImage,
					Env: map[string]string{
						"PF_FS_ID":          "fs-name_1",
						"PF_JOB_CLUSTER_ID": "testClusterID",
						"PF_JOB_FLAVOUR":    "mockFlavourName",
						"PF_JOB_ID":         "",
						"PF_JOB_NAMESPACE":  "paddleflow",
						"PF_JOB_PRIORITY":   "NORMAL",
						"PF_JOB_QUEUE_ID":   "mockQueueID",
						"PF_JOB_QUEUE_NAME": "mockQueueName",
						schema.EnvJobType:   string(schema.TypePodJob),
						"PF_USER_NAME":      "root",
					},
					Flavour: schema.Flavour{Name: "mockFlavourName", ResourceInfo: schema.ResourceInfo{CPU: "2", Mem: "2"}},
				},
			},
		},
		ExtensionTemplate: []byte(extensionTemplateYaml),
	}
)

func TestSingleJob_Create(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = "testSchedulerName"
	defaultJobYamlPath := "../../../../../config/server/default/job/job_template.yaml"
	config.InitJobTemplate(defaultJobYamlPath)

	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeRuntimeClient := client.NewFakeKubeRuntimeClient(server)
	// mock db
	driver.InitMockDB()

	podBytes, err := json.Marshal(mockSinglePod)
	assert.NoError(t, err)
	var mockSinglePodWrongFlavour, mockSinglePodNilContainer api.PFJob
	//mockSinglePodWrongFlavour
	err = json.Unmarshal(podBytes, &mockSinglePodWrongFlavour)
	assert.NoError(t, err)
	mockSinglePodWrongFlavour.Tasks[0].Flavour = schema.Flavour{
		Name: "mockFlavourName",
		ResourceInfo: schema.ResourceInfo{
			CPU: "a",
			Mem: "2",
		},
	}
	// mockSinglePodNilContainer
	err = json.Unmarshal(podBytes, &mockSinglePodNilContainer)
	assert.NoError(t, err)
	mockSinglePodNilContainer.ID = "mockSinglePodNilContainer"
	mockSinglePodNilContainer.ExtensionTemplate = []byte(extensionTemplateYamlNilContainer)
	// create kubernetes resource with dynamic client
	tests := []struct {
		caseName string
		jobObj   *api.PFJob
		wantErr  error
		wantMsg  string
	}{
		{
			caseName: "pod_test1",
			jobObj: &api.PFJob{
				JobType: schema.TypeSingle,
			},
			wantErr: fmt.Errorf("create builtin /v1, Kind=Pod job / on cluster default-cluster with type Kubernetes failed, job member is nil"),
			wantMsg: "create builtin /v1, Kind=Pod job / on cluster default-cluster with type Kubernetes failed, job member is nil",
		},
		{
			caseName: "pod_test2",
			jobObj:   &mockSinglePod,
			wantErr:  nil,
			wantMsg:  "",
		},
		{
			caseName: "wrong flavour",
			jobObj:   &mockSinglePodWrongFlavour,
			wantErr:  fmt.Errorf("quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'"),
			wantMsg:  "",
		},
		{
			caseName: "nil container",
			jobObj:   &mockSinglePodNilContainer,
			wantErr:  nil,
			wantMsg:  "",
		},
		{
			caseName: "nil jobObj",
			jobObj:   nil,
			wantErr:  fmt.Errorf("job is nil"),
			wantMsg:  "job is nil",
		},
	}

	singleJob := New(kubeRuntimeClient)
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			err := singleJob.Submit(context.TODO(), test.jobObj)
			if test.wantErr == nil {
				assert.NoError(t, err)
				t.Logf("case[%s] to CreateJob, paddleFlowJob=%+v", test.caseName, test.jobObj)
				obj, err := kubeRuntimeClient.Get(test.jobObj.Namespace, test.jobObj.ID, KubeSingleFwVersion)
				assert.NoError(t, err)

				gettedPod := v1.Pod{}
				unObj := obj.(*unstructured.Unstructured)
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, &gettedPod); err != nil {
					t.Errorf("convert unstructured object [%+v] to %s job failed. error: %s", obj, test.jobObj.ID, err)
				}
				t.Logf("gettedPod.Spec is %+v", gettedPod.Spec)
				assert.Equal(t, taskImage, gettedPod.Spec.Containers[0].Image)
				assert.Equal(t, taskCPU, gettedPod.Spec.Containers[0].Resources.Requests.Cpu().String())
				assert.Equal(t, taskMemory, gettedPod.Spec.Containers[0].Resources.Requests.Memory().String())
			} else {
				assert.NotNil(t, err)
				assert.Equal(t, test.wantErr.Error(), err.Error())
			}
		})
	}
}
