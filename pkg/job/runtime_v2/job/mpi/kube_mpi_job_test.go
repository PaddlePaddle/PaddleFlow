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

package mpi

import (
	"context"
	"net/http/httptest"
	"testing"

	mpiv1 "github.com/kubeflow/training-operator/pkg/apis/mpi/v1"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

var (
	extensionMPIYaml = `
apiVersion: kubeflow.org/v1
kind: MPIJob
metadata:
  name: tensorflow-mnist
spec:
  slotsPerWorker: 1
  runPolicy:
    cleanPodPolicy: Running
  mpiReplicaSpecs:
    Launcher:
      replicas: 1
      template:
        spec:
          containers:
          - image: horovod/horovod:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu
            name: mpi
            command:
            - mpirun
            args:
            - -np
            - "2"
            - --allow-run-as-root
            - -bind-to
            - none
            - -map-by
            - slot
            - -x
            - LD_LIBRARY_PATH
            - -x
            - PATH
            - -mca
            - pml
            - ob1
            - -mca
            - btl
            - ^openib
            - python
            - /examples/tensorflow2_mnist.py
            resources:
              limits:
                cpu: 1
                memory: 2Gi
    Worker:
      replicas: 2
      template:
        spec:
          containers:
          - image: horovod/horovod:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu
            name: mpi
            resources:
              limits:
                cpu: 2
                memory: 4Gi
`

	extensionMPIYamlError = `
apiVersion: kubeflow.org/v1
kind: Paddle
metadata1:
  name: tensorflow-mnist
spec2:
  slotsPerWorker: 1
  runPolicy:
    cleanPodPolicy: Running
  mpiReplicaSpecs:
    Launcher:
      replicas: 1
      template:
        spec:
          containers:
          - image: horovod/horovod:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu
            name: mpi
            command:
            - mpirun
            args:
            - -np
            - "2"
            - --allow-run-as-root
            - -bind-to
            - none
            - -map-by
            - slot
            - -x
            - LD_LIBRARY_PATH
            - -x
            - PATH
            - -mca
            - pml
            - ob1
            - -mca
            - btl
            - ^openib
            - python
            - /examples/tensorflow2_mnist.py
            resources:
              limits:
                cpu: 1
                memory: 2Gi
    Worker:
      replicas: 2
      template:
        spec:
          containers:
          - image: horovod/horovod:0.20.0-tf2.3.0-torch1.6.0-mxnet1.5.0-py3.7-cpu
            name: mpi
            resources:
              limits:
                cpu: 2
                memory: 4Gi
`
)

func TestMPIJob_CreateJob(t *testing.T) {
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
	tests := []struct {
		caseName  string
		jobObj    *api.PFJob
		expectErr string
		wantErr   bool
		wantMsg   string
	}{
		{
			caseName: "create job successfully",
			jobObj: &api.PFJob{
				Name:      "test-mpi-job",
				ID:        "job-test-mpi1",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				JobMode:   schema.EnvJobModePS,
				Framework: schema.FrameworkMPI,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
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
			expectErr: "",
			wantErr:   false,
		},
		{
			caseName: "Member absent",
			jobObj: &api.PFJob{
				Name:      "test-mpi-job",
				ID:        uuid.GenerateIDWithLength("job", 5),
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				JobMode:   schema.EnvJobModePS,
				Framework: schema.FrameworkMPI,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
				},
				Tasks: []schema.Member{
					{
						Replicas: 1,
						Role:     schema.RoleMaster,
						Conf: schema.Conf{
							Flavour: schema.Flavour{Name: "", ResourceInfo: schema.ResourceInfo{CPU: "-1", Mem: "4Gi"}},
						},
					},
				},
			},
			expectErr: "negative resources not permitted: map[cpu:-1 memory:4Gi]",
			wantErr:   true,
		},
		{
			caseName: "flavour wrong",
			jobObj: &api.PFJob{
				Name:      "test-mpi-job",
				ID:        uuid.GenerateIDWithLength("job", 5),
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				JobMode:   schema.EnvJobModePS,
				Framework: schema.FrameworkMPI,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
				},
				Tasks: []schema.Member{
					{
						Replicas: 1,
						Role:     schema.RoleMaster,
						Conf: schema.Conf{
							Flavour: schema.Flavour{Name: "", ResourceInfo: schema.ResourceInfo{CPU: "4a", Mem: "4Gi"}},
						},
					},
				},
			},
			expectErr: "quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'",
			wantErr:   true,
		},
		{
			caseName: "ExtensionTemplate",
			jobObj: &api.PFJob{
				Name:      "test-mpi-job",
				ID:        "job-test-mpi2",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				JobMode:   schema.EnvJobModePS,
				Framework: schema.FrameworkMPI,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
				},
				ExtensionTemplate: []byte(extensionMPIYaml),
			},
			expectErr: "",
			wantErr:   false,
		},
		{
			caseName: "ExtensionTemplate wrong2",
			jobObj: &api.PFJob{
				Name:      "test-mpi-job",
				ID:        "job-test-mpi3",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				JobMode:   schema.EnvJobModePS,
				Framework: schema.FrameworkMPI,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
				},
				ExtensionTemplate: []byte(extensionMPIYamlError),
			},
			expectErr: "expect GroupVersionKind is kubeflow.org/v1, Kind=MPIJob, but got kubeflow.org/v1, Kind=Paddle",
			wantErr:   true,
		},
	}

	MPIJob := New(kubeRuntimeClient)
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			t.Logf("case[%s]", test.caseName)
			err := MPIJob.Submit(context.TODO(), test.jobObj)
			if test.wantErr {
				assert.Error(t, err)
				t.Logf("create job failed, err: %v", err)
				assert.Equal(t, err.Error(), test.expectErr)
			} else {
				// get log
				jobLogRequest := schema.JobLogRequest{
					JobID:       test.jobObj.ID,
					JobType:     string(test.jobObj.JobType),
					Namespace:   test.jobObj.Namespace,
					LogPageSize: 1,
					LogPageNo:   1,
				}
				_, err := MPIJob.GetLog(context.TODO(), jobLogRequest)
				assert.NoError(t, err)
				jobObj, err := kubeRuntimeClient.Get(test.jobObj.Namespace, test.jobObj.ID, KubeMPIFwVersion)
				if err != nil {
					t.Errorf(err.Error())
				} else {
					t.Logf("obj=%#v", jobObj)
				}
			}
		})
	}
}

func TestKubeMPIJob_Update(t *testing.T) {
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
	tests := []struct {
		caseName  string
		jobObj    *api.PFJob
		expectErr string
		wantErr   bool
		wantMsg   string
	}{
		{
			caseName: "create job successfully",
			jobObj: &api.PFJob{
				Name:      "test-mpi-job",
				ID:        "job-test-mpi1",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				JobMode:   schema.EnvJobModePS,
				Framework: schema.FrameworkMPI,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
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
			expectErr: "",
			wantErr:   false,
		},
	}

	MPIJob := New(kubeRuntimeClient)
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			t.Logf("case[%s]", test.caseName)
			err := MPIJob.Submit(context.TODO(), test.jobObj)
			// update
			err = MPIJob.Update(context.TODO(), test.jobObj)
			assert.NoError(t, err)
		})
	}
}

func TestKubeMPIJob_Stop(t *testing.T) {
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
	tests := []struct {
		caseName  string
		jobObj    *api.PFJob
		expectErr string
		wantErr   bool
		wantMsg   string
	}{
		{
			caseName: "create job successfully",
			jobObj: &api.PFJob{
				Name:      "test-mpi-job",
				ID:        "job-test-mpi1",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				JobMode:   schema.EnvJobModePS,
				Framework: schema.FrameworkMPI,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
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
			expectErr: "",
			wantErr:   false,
		},
	}

	MPIJob := New(kubeRuntimeClient)
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			t.Logf("case[%s]", test.caseName)
			err := MPIJob.Submit(context.TODO(), test.jobObj)
			assert.NoError(t, err)
			err = MPIJob.Stop(context.TODO(), test.jobObj)
			assert.NoError(t, err)
		})
	}
}

func TestKubeMPIJob_Delete(t *testing.T) {
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
	tests := []struct {
		caseName  string
		jobObj    *api.PFJob
		expectErr string
		wantErr   bool
		wantMsg   string
	}{
		{
			caseName: "create job successfully",
			jobObj: &api.PFJob{
				Name:      "test-mpi-job",
				ID:        "job-test-mpi1",
				Namespace: "default",
				JobType:   schema.TypeDistributed,
				JobMode:   schema.EnvJobModePS,
				Framework: schema.FrameworkMPI,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "mockImage",
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
			expectErr: "",
			wantErr:   false,
		},
	}

	MPIJob := New(kubeRuntimeClient)
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			t.Logf("case[%s]", test.caseName)
			err := MPIJob.Submit(context.TODO(), test.jobObj)
			assert.NoError(t, err)
			// Delete
			err = MPIJob.Delete(context.TODO(), test.jobObj)
			assert.NoError(t, err)

		})
	}
}

func TestMPIJobListener(t *testing.T) {
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
		job       *mpiv1.MPIJob
		expectErr error
	}{
		{
			caseName: "register ray job listener",
			job: &mpiv1.MPIJob{
				ObjectMeta: v1.ObjectMeta{
					Name:      "ray-job-1",
					Namespace: "default",
				},
			},
			expectErr: nil,
		},
	}

	mpiJob := New(kubeRuntimeClient)
	workQueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	informer := kubeRuntimeClient.DynamicFactory.ForResource(gvrMap.Resource).Informer()
	stopCh := make(chan struct{})
	defer close(stopCh)
	go kubeRuntimeClient.DynamicFactory.Start(stopCh)
	ok := cache.WaitForCacheSync(stopCh, informer.HasSynced)
	assert.Equal(t, true, ok)

	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			err := mpiJob.AddEventListener(context.TODO(), schema.ListenerTypeJob, workQueue, informer)
			assert.Equal(t, test.expectErr, err)

			err = kubeRuntimeClient.Create(test.job, KubeMPIFwVersion)
			assert.Equal(t, nil, err)

			err = kubeRuntimeClient.Delete(test.job.Namespace, test.job.Name, KubeMPIFwVersion)
			assert.Equal(t, nil, err)
		})
	}
}
