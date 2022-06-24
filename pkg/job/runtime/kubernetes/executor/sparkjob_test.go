/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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
	"k8s.io/apimachinery/pkg/runtime"

	sparkapp "github.com/PaddlePaddle/PaddleFlow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
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
  batchScheduler: testSchedulerName
  batchSchedulerOptions: {}
  deps: {}
  driver:
	coreLimit: "3"
	cores: 3
	memory: 3G
	podName: normal
	serviceAccount: spark
  executor:
	coreLimit: "2"
	cores: 2
	instances: 1
	memory: 2Gi
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
status:
  applicationState:
	state: ""
  driverInfo: {}
  lastSubmissionAttemptTime: null
  terminationTime: null
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
			Name: "normal",
			// Command: "sleep 200",
			Image: "mockImage",
			Env:   map[string]string{
				// "PF_JOB_MODE":           "PS",
				// "PF_FS_ID":              "fs-name_1",
				// "PF_JOB_CLUSTER_ID":     "testClusterID",
				// "PF_JOB_ID":             "",
				// "PF_JOB_NAMESPACE":      "paddleflow",
				// "PF_JOB_PRIORITY":       "NORMAL",
				// "PF_JOB_QUEUE_ID":       "mockQueueID",
				// "PF_JOB_FLAVOUR":        "cpu",
				// "PF_JOB_WORKER_FLAVOUR": "cpu",
				// "PF_JOB_WORKER_COMMAND": "sleep 3600",
				// "PF_JOB_QUEUE_NAME":     "mockQueueName",
				// "PF_USER_NAME": "root",
			},
		},
		Tasks: []models.Member{
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
					Flavour: schema.Flavour{Name: "", ResourceInfo: schema.ResourceInfo{CPU: "3", Mem: "3G"}},
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
						// "PF_FS_ID":          "fs-name_1",
						// "PF_JOB_CLUSTER_ID": "testClusterID",
						// "PF_JOB_FLAVOUR":    "cpu",
						// "PF_JOB_ID":         "",
						// "PF_JOB_NAMESPACE":  "paddleflow",
						// "PF_JOB_PRIORITY":   "NORMAL",
						// "PF_JOB_QUEUE_ID":   "mockQueueID",
						// "PF_JOB_QUEUE_NAME": "mockQueueName",
						schema.EnvJobType: string(schema.TypeSparkJob),
						// "PF_USER_NAME":      "root",
					},
					Flavour: schema.Flavour{Name: "", ResourceInfo: schema.ResourceInfo{CPU: "2", Mem: "2Gi"}},
				},
			},
		},
		// ExtensionTemplate: extensionPaddleYaml,
	}
	mockSparkJobWithYaml = api.PFJob{
		ID:        "job-mock-000002",
		Namespace: "default",
		JobType:   schema.TypeDistributed,
		Framework: schema.FrameworkSpark,
		UserName:  "root",
		QueueID:   "mockQueueID",
		Tasks: []models.Member{
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
		ExtensionTemplate: extensionPaddleYaml,
	}
)

func initGlobalServerConfig() {
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = "testSchedulerName"
	config.GlobalServerConfig.Job.DefaultJobYamlDir = "../../../../../config/server/default/job"
}

// TestSparkApp_CreateJob tests create spark app by single yaml
func TestSparkApp_CreateJob(t *testing.T) {
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
			caseName: "spark",
			jobObj:   &mockSparkJob,
			wantErr:  false,
			wantMsg:  "",
		},
		{
			caseName: "with_custom_yaml",
			jobObj:   &mockSparkJobWithYaml,
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
			jobObj, err := Get(test.jobObj.Namespace, test.jobObj.ID, k8s.SparkAppGVK, dynamicClient)
			if !assert.NoError(t, err) {
				t.Errorf(err.Error())
				continue
			}
			t.Logf("obj=%#v", jobObj)
			application := sparkapp.SparkApplication{}
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(jobObj.Object, &application)
			assert.NoError(t, err)
			t.Logf("application=%#v", application)
			podYaml, err := yaml.Marshal(application)
			assert.NoError(t, err)
			t.Logf("%s", string(podYaml))
			t.Skipped()
		} else {
			t.Errorf("error case, %v", err)
		}
	}
}
