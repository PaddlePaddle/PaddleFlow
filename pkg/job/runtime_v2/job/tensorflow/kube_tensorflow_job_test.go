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
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestTFJob_CreateJob(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = "testSchedulerName"
	config.GlobalServerConfig.Job.DefaultJobYamlDir = "../../../../../config/server/default/job"

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
			caseName: "create job successfully",
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

	tfJob := New(kubeRuntimeClient)
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			err := tfJob.Submit(context.TODO(), test.jobObj)
			assert.Equal(t, test.expectErr, err)
			if err != nil {
				t.Logf("create job failed, err: %v", err)
			} else {
				jobObj, err := kubeRuntimeClient.Get(test.jobObj.Namespace, test.jobObj.ID, KubeTFFwVersion)
				if err != nil {
					t.Errorf(err.Error())
				} else {
					t.Logf("obj=%#v", jobObj)
				}
			}
		})
	}
}
