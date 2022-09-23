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

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/runtime"

	rayV1alpha1 "github.com/PaddlePaddle/PaddleFlow/pkg/apis/ray-operator/v1alpha1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestRayJob_CreateJob(t *testing.T) {
	initGlobalServerConfig()
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	dynamicClient := newFakeDynamicClient(server)
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
			caseName: "ray_test1",
			jobObj: &api.PFJob{
				Name:      "test-ray-job",
				ID:        "job-test-ray",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkRay,
				Conf: schema.Conf{
					Env: map[string]string{
						"PF_JOB_MODE": "PS",
					},
				},

				Tasks: []schema.Member{
					{
						Replicas: 1,
						Role:     schema.RoleMaster,
						Conf: schema.Conf{
							Flavour: schema.Flavour{Name: "", ResourceInfo: schema.ResourceInfo{CPU: "4", Mem: "4Gi"}},
							Image:   "rayproject/ray:2.0.0",
							Command: "python /home/ray/samples/sample_code.py",
							Args: []string{
								"port: '6379'",
								"object-store-memory: '100000000'",
								"dashboard-host: 0.0.0.0",
								"num-cpus: '2' ",
								"node-ip-address: $MY_POD_IP ",
								"block: 'true'",
							},
						},
					}, //Task0
					{
						Replicas: 2,
						Role:     schema.RoleWorker,
						Conf: schema.Conf{
							Flavour: schema.Flavour{Name: "", ResourceInfo: schema.ResourceInfo{CPU: "4", Mem: "4Gi"}},
							Image:   "rayproject/ray:2.0.0",
							Env: map[string]string{
								"groupName":   "small-group",
								"maxReplicas": "5",
								"minReplicas": "1",
							},
							Args: []string{
								"node-ip-address: $MY_POD_IP",
								"block: 'true'",
							},
						},
					},
				},
			},
			expectErr: nil,
			wantErr:   false,
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
		if err != nil {
			t.Logf("create job failed, err: %v", err)
		}
		assert.Equal(t, test.expectErr, err)
		if assert.NotEmpty(t, jobID) {
			t.Logf("case[%s] to CreateJob, paddleFlowJob=%+v", test.caseName, p)
			jobObj, err := Get(test.jobObj.Namespace, test.jobObj.ID, k8s.RayJobGVK, dynamicClient)
			if !assert.NoError(t, err) {
				t.Errorf(err.Error())
				continue
			}
			t.Logf("obj=%#v", jobObj)
			job := rayV1alpha1.RayJob{}
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(jobObj.Object, &job)
			assert.NoError(t, err)
			t.Logf("ray job %#v", job)
			podYaml, err := yaml.Marshal(job)
			assert.NoError(t, err)
			t.Logf("%s", string(podYaml))
			t.Skipped()
		} else {
			t.Errorf("error case, %v", err)
		}
	}
}
