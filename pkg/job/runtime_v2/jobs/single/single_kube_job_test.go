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
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	restclient "k8s.io/client-go/rest"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
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
		Tasks: []schema.Member{
			{
				Replicas: 1,
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
			},
		},
		ExtensionTemplate: []byte(extensionTemplateYaml),
	}
)

func newFakeKubeRuntimeClient(server *httptest.Server) *client.KubeRuntimeClient {
	scheme := runtime.NewScheme()
	dynamicClient := fakedynamicclient.NewSimpleDynamicClient(scheme)
	fakeDiscovery := discovery.NewDiscoveryClientForConfigOrDie(&restclient.Config{Host: server.URL})

	return &client.KubeRuntimeClient{
		DynamicClient:   dynamicClient,
		DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
		DiscoveryClient: fakeDiscovery,
		ClusterInfo: &schema.Cluster{
			Name: "default-cluster",
			ID:   "cluster-123",
		},
		Config: &restclient.Config{Host: server.URL},
	}
}

func TestSingleJob_Create(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = "testSchedulerName"
	config.GlobalServerConfig.Job.DefaultJobYamlDir = "../../../../../config/server/default/job"
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeRuntimeClient := newFakeKubeRuntimeClient(server)
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
			wantMsg: "create builtin /v1, Kind=Pod job / on cluster name default-cluster with cluster type  failed, job member is nil",
		},
		{
			caseName: "pod_test2",
			jobObj:   &mockSinglePod,
			wantErr:  false,
			wantMsg:  "",
		},
	}

	singleJob := New(kubeRuntimeClient)
	frameworkVersion := schema.NewFrameworkVersion(k8s.PodGVK.Kind, k8s.PodGVK.GroupVersion().String())
	for _, test := range tests {
		t.Logf("case[%s] to create job", test.caseName)

		err := singleJob.Submit(context.TODO(), test.jobObj)
		if test.wantErr && assert.Error(t, err) {
			assert.Equal(t, test.wantMsg, err.Error())
		} else if !test.wantErr {
			t.Logf("case[%s] to CreateJob sucessfully", test.caseName)
			_, err := kubeRuntimeClient.Get(test.jobObj.Namespace, test.jobObj.ID, frameworkVersion)
			if !assert.NoError(t, err) {
				t.Errorf(err.Error())
				continue
			}
		} else {
			t.Errorf("error case, %v", err)
		}
	}
}
