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

	tfv1 "github.com/kubeflow/training-operator/pkg/apis/tensorflow/v1"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestTFJob_CreateJob(t *testing.T) {
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
			caseName: "tf_test1",
			jobObj: &api.PFJob{
				Name:      "test-tf-job",
				ID:        "job-test-tf",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				Framework: schema.FrameworkTF,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
					Env: map[string]string{
						"PF_JOB_MODE": "Collective",
					},
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
			jobObj, err := Get(test.jobObj.Namespace, test.jobObj.ID, k8s.TFJobGVK, dynamicClient)
			if !assert.NoError(t, err) {
				t.Errorf(err.Error())
				continue
			}
			t.Logf("obj=%#v", jobObj)
			tfJob := tfv1.TFJob{}
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(jobObj.Object, &tfJob)
			assert.NoError(t, err)
			t.Logf("torch job %#v", tfJob)
			podYaml, err := yaml.Marshal(tfJob)
			assert.NoError(t, err)
			t.Logf("%s", string(podYaml))
			t.Skipped()
		} else {
			t.Errorf("error case, %v", err)
		}
	}
}
