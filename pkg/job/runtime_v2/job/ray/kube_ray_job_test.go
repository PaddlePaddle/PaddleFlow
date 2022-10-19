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

package ray

import (
	"context"
	"net/http/httptest"
	"testing"

	rayV1alpha1 "github.com/PaddlePaddle/PaddleFlow/pkg/apis/ray-operator/v1alpha1"
	"github.com/stretchr/testify/assert"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestRayJob(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = "testSchedulerName"
	defaultJobYamlPath := "../../../../../config/server/default/job/job_template.yaml"
	err := config.InitJobTemplate(defaultJobYamlPath)
	assert.Equal(t, nil, err)

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
			caseName: "create ray job successfully",
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
				Labels: map[string]string{
					"job1-id": "xxx",
					"owner":   "paddleflow",
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

	rayJob := New(kubeRuntimeClient)
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			err = rayJob.Submit(context.TODO(), test.jobObj)
			assert.Equal(t, test.expectErr, err)
			if err != nil {
				t.Logf("create job failed, err: %v", err)
			} else {
				jobObj, err := kubeRuntimeClient.Get(test.jobObj.Namespace, test.jobObj.ID, KubeRayFwVersion)
				if err != nil {
					t.Errorf(err.Error())
				} else {
					t.Logf("obj=%#v", jobObj)
				}
			}

			t.Logf("stop ray job")
			err = rayJob.Stop(context.TODO(), test.jobObj)
			assert.Equal(t, nil, err)

			t.Logf("delete ray job")
			err = rayJob.Delete(context.TODO(), test.jobObj)
			assert.Equal(t, true, k8serrors.IsNotFound(err))
		})
	}
}

func TestRayJobListener(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeRuntimeClient := client.NewFakeKubeRuntimeClient(server)
	gvrMap, gvrErr := kubeRuntimeClient.GetGVR(JobGVK)
	assert.Equal(t, nil, gvrErr)
	// mock db
	driver.InitMockDB()
	// create kubernetes resource with dynamic client
	tests := []struct {
		caseName  string
		job       *rayV1alpha1.RayJob
		expectErr error
	}{
		{
			caseName: "register ray job listener",
			job: &rayV1alpha1.RayJob{
				ObjectMeta: v1.ObjectMeta{
					Name:      "ray-job-1",
					Namespace: "default",
				},
			},
			expectErr: nil,
		},
	}

	rayJob := New(kubeRuntimeClient)
	workQueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	informer := kubeRuntimeClient.DynamicFactory.ForResource(gvrMap.Resource).Informer()
	stopCh := make(chan struct{})
	defer close(stopCh)
	go kubeRuntimeClient.DynamicFactory.Start(stopCh)
	ok := cache.WaitForCacheSync(stopCh, informer.HasSynced)
	assert.Equal(t, true, ok)

	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			err := rayJob.AddEventListener(context.TODO(), schema.ListenerTypeJob, workQueue, informer)
			assert.Equal(t, test.expectErr, err)

			err = kubeRuntimeClient.Create(test.job, KubeRayFwVersion)
			assert.Equal(t, nil, err)

			err = kubeRuntimeClient.Delete(test.job.Namespace, test.job.Name, KubeRayFwVersion)
			assert.Equal(t, nil, err)
		})
	}
}
