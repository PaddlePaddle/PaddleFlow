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
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

var (
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
	mockSinglePod = api.PFJob{
		ID:        "job-normal-0c272d0a",
		Name:      "",
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
			Flavour: schema.Flavour{Name: "mockFlavourName", ResourceInfo: schema.ResourceInfo{CPU: "1", Mem: "1"}},
		},
		ExtensionTemplate: []byte(extensionTemplateYaml),
	}
)

func TestSinglePod_CreateJob(t *testing.T) {
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
				JobType: schema.TypeSingle,
			},
			wantErr: true,
			wantMsg: "namespace is empty",
		},
		{
			caseName: "pod_test2",
			jobObj:   &mockSinglePod,
			wantErr:  false,
			wantMsg:  "",
		},
	}

	for _, test := range tests {
		p, err := NewKubeJob(test.jobObj, dynamicClient)
		assert.Nil(t, err)
		assert.NotNil(t, p)
		t.Logf("case[%s] to NewKubeJob=%+v", test.caseName, p)

		jobID, err := p.CreateJob()
		if test.wantErr && assert.Error(t, err) {
			assert.Equal(t, test.wantMsg, err.Error())
		} else if !test.wantErr && assert.NotEmpty(t, jobID) {
			t.Logf("case[%s] to CreateJob, paddleFlowJob=%+v", test.caseName, p)
			jobObj, err := Get(test.jobObj.Namespace, test.jobObj.ID, k8s.PodGVK, dynamicClient)
			if !assert.NoError(t, err) {
				t.Errorf(err.Error())
				continue
			}
			t.Logf("obj=%#v", jobObj)
			pod := v1.Pod{}
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(jobObj.Object, &pod)
			assert.NoError(t, err)
			t.Logf("pod=%#v", pod)
			podYaml, err := yaml.Marshal(pod)
			assert.NoError(t, err)
			t.Logf("%s", string(podYaml))
		} else {
			t.Errorf("error case, %v", err)
		}
	}
}
