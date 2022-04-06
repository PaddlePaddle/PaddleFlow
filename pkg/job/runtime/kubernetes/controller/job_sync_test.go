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

package controller

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	restclient "k8s.io/client-go/rest"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/common/schema"
)

func newFakeJobSyncController() *JobSync {
	scheme := runtime.NewScheme()
	dynamicClient := fakedynamicclient.NewSimpleDynamicClient(scheme)

	var server = httptest.NewServer(DiscoveryHandlerFunc)
	defer server.Close()
	fakeDiscovery := discovery.NewDiscoveryClientForConfigOrDie(&restclient.Config{Host: server.URL})

	ctrl := &JobSync{}
	opt := &k8s.DynamicClientOption{
		DynamicClient:   dynamicClient,
		DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
		DiscoveryClient: fakeDiscovery,
	}
	ctrl.Initialize(opt)
	return ctrl
}

func TestJobSyncVCJob(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		oldObj    *unstructured.Unstructured
		newObj    *unstructured.Unstructured
		oldStatus *batchv1alpha1.JobStatus
		newStatus *batchv1alpha1.JobStatus
	}{
		{
			name:      "vcjob status from pending turn to running",
			namespace: "default",
			oldObj:    NewUnstructured(k8s.VCJobGVK, "default", "vcjob1"),
			oldStatus: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Pending,
				},
				Pending: 1,
				Running: 1,
			},
			newObj: NewUnstructured(k8s.VCJobGVK, "default", "vcjob1"),
			newStatus: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Running,
				},
				Pending: 1,
				Running: 1,
			},
		},
		{
			name:      "vcjob status from running turn to failed",
			namespace: "default",
			oldObj:    NewUnstructured(k8s.VCJobGVK, "default", "vcjob2"),
			oldStatus: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Pending,
				},
				Pending: 1,
				Running: 1,
			},
			newObj: NewUnstructured(k8s.VCJobGVK, "default", "vcjob2"),
			newStatus: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Failed,
				},
				Pending: 1,
				Running: 1,
			},
		},
	}

	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{
			Reclaim: config.ReclaimConfig{
				CleanJob: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database.InitMockDB()
			err := models.CreateJob(&models.Job{ID: test.newObj.GetName()})
			assert.Equal(t, nil, err)

			c := newFakeJobSyncController()
			stopCh := make(chan struct{})
			defer close(stopCh)

			test.oldObj.Object["status"], err = runtime.DefaultUnstructuredConverter.ToUnstructured(test.oldStatus)
			assert.Equal(t, nil, err)

			test.newObj.Object["status"], err = runtime.DefaultUnstructuredConverter.ToUnstructured(test.newStatus)
			assert.Equal(t, nil, err)

			c.update(test.oldObj, test.newObj)
			c.Run(stopCh)
			time.Sleep(3 * time.Second)
		})
	}
}

func TestJobSyncPod(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		oldObj    *unstructured.Unstructured
		newObj    *unstructured.Unstructured
		oldStatus *v1.PodStatus
		newStatus *v1.PodStatus
	}{
		{
			name:      "pod pull image failed",
			namespace: "default",
			oldObj:    NewUnstructured(k8s.PodGVK, "default", "pod1"),
			oldStatus: &v1.PodStatus{
				Phase:                 v1.PodPending,
				InitContainerStatuses: []v1.ContainerStatus{},
			},
			newObj: NewUnstructured(k8s.PodGVK, "default", "pod1"),
			newStatus: &v1.PodStatus{
				Phase: v1.PodPending,
				InitContainerStatuses: []v1.ContainerStatus{
					{
						Name: "container",
						State: v1.ContainerState{
							Waiting: &v1.ContainerStateWaiting{
								Reason:  "ErrImagePull",
								Message: `rpc error: code = Unknown desc = Error response from daemon: manifest for xx not found`,
							},
						},
					},
				},
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name: "container2",
						State: v1.ContainerState{
							Waiting: &v1.ContainerStateWaiting{
								Reason:  "ErrImagePull",
								Message: `rpc error: code = Unknown desc = Error response from daemon: manifest for xx not found`,
							},
						},
					},
				},
			},
		},
	}

	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{
			Reclaim: config.ReclaimConfig{
				JobPendingTTLSeconds: 1,
			},
		},
	}

	database.InitMockDB()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := newFakeJobSyncController()
			err := models.CreateJob(&models.Job{
				ID:     test.newObj.GetName(),
				Status: schema.StatusJobPending,
				Type:   string(schema.TypeVcJob),
				Config: schema.Conf{
					Env: map[string]string{
						schema.EnvJobNamespace: test.newObj.GetNamespace(),
					},
				},
			})
			assert.Equal(t, nil, err)

			test.oldObj.Object["status"], err = runtime.DefaultUnstructuredConverter.ToUnstructured(test.oldStatus)
			assert.Equal(t, nil, err)

			test.newObj.Object["status"], err = runtime.DefaultUnstructuredConverter.ToUnstructured(test.newStatus)
			assert.Equal(t, nil, err)

			stopCh := make(chan struct{})
			defer close(stopCh)
			// create volcano job
			vcjob := NewUnstructured(k8s.VCJobGVK, test.newObj.GetNamespace(), test.newObj.GetName())
			_, err = c.opt.DynamicClient.Resource(VCJobGVR).Namespace(vcjob.GetNamespace()).Create(context.TODO(), vcjob, metav1.CreateOptions{})
			assert.Equal(t, nil, err)

			c.updatePod(test.oldObj, test.newObj)
			c.Run(stopCh)
			time.Sleep(3 * time.Second)
		})
	}

}
