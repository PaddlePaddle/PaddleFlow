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

	wfv1 "github.com/argoproj/argo/pkg/apis/workflow/v1alpha1"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	restclient "k8s.io/client-go/rest"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func newFakeJobSyncController() *JobSync {
	scheme := runtime.NewScheme()
	dynamicClient := fakedynamicclient.NewSimpleDynamicClient(scheme)

	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	fakeDiscovery := discovery.NewDiscoveryClientForConfigOrDie(&restclient.Config{Host: server.URL})

	ctrl := &JobSync{}
	opt := &k8s.DynamicClientOption{
		DynamicClient:   dynamicClient,
		DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
		DiscoveryClient: fakeDiscovery,
		ClusterInfo:     &schema.Cluster{Name: "test-cluster"},
	}
	err := ctrl.Initialize(opt)
	if err != nil {
		log.Errorf("initialize controller failed: %v", err)
	}
	return ctrl
}

func TestJobSyncVCJob(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		oldObj    *unstructured.Unstructured
		newObj    *unstructured.Unstructured
		oldStatus interface{}
		newStatus interface{}
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
		{
			name:      "ArgoWorkflow status from pending turn to running",
			namespace: "default",
			oldObj:    NewUnstructured(k8s.ArgoWorkflowGVK, "default", "argowf1"),
			oldStatus: &wfv1.WorkflowStatus{
				Phase: wfv1.NodePending,
			},
			newObj: NewUnstructured(k8s.ArgoWorkflowGVK, "default", "argowf1"),
			newStatus: &wfv1.WorkflowStatus{
				Phase: wfv1.NodeRunning,
			},
		},
		{
			name:      "ArgoWorkflow status from running turn to failed",
			namespace: "default",
			oldObj:    NewUnstructured(k8s.ArgoWorkflowGVK, "default", "argowf2"),
			oldStatus: &wfv1.WorkflowStatus{
				Phase: wfv1.NodeRunning,
			},
			newObj: NewUnstructured(k8s.ArgoWorkflowGVK, "default", "argowf2"),
			newStatus: &wfv1.WorkflowStatus{
				Phase: wfv1.NodePending,
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
			driver.InitMockDB()
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
	jobName := "job-test"
	containerStatusMap := map[string]v1.ContainerStatus{
		"Running": {
			Name: "container",
			State: v1.ContainerState{
				Running: &v1.ContainerStateRunning{},
			},
		},
		"ImagePullBackOff": {
			Name: "container",
			State: v1.ContainerState{
				Waiting: &v1.ContainerStateWaiting{
					Reason:  "ImagePullBackOff",
					Message: `Back-off pulling image "xxx"`,
				},
			},
		},
		"ErrImagePull": {
			Name: "container",
			State: v1.ContainerState{
				Waiting: &v1.ContainerStateWaiting{
					Reason:  "ErrImagePull",
					Message: `rpc error: code = Unknown desc = Error response from daemon: manifest for xx not found`,
				},
			},
		},
		"CrashLoopBackOff": {
			Name: "container",
			State: v1.ContainerState{
				Waiting: &v1.ContainerStateWaiting{
					Reason:  "CrashLoopBackOff",
					Message: `back-off 5m0s restarting failed container=xxx pod=xxx`,
				},
			},
		},
	}
	objectMeta := metav1.ObjectMeta{
		Name:      "pod",
		Namespace: "default",
		Labels: map[string]string{
			schema.VolcanoJobNameLabel: jobName,
			schema.JobIDLabel:          jobName,
		},
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: "batch.volcano.sh/v1alpha1",
				Kind:       "Job",
				Name:       jobName,
			},
		},
	}

	tests := []struct {
		name      string
		namespace string
		UID       types.UID
		oldPod    *v1.Pod
		newPod    *v1.Pod
	}{
		{
			name:      "pod pull image failed",
			namespace: "default",
			UID:       "test-uid-1",
			oldPod: &v1.Pod{
				ObjectMeta: objectMeta,
				Status: v1.PodStatus{
					Phase: v1.PodPending,
					ContainerStatuses: []v1.ContainerStatus{
						containerStatusMap["ImagePullBackOff"],
					},
				},
			},
			newPod: &v1.Pod{
				ObjectMeta: objectMeta,
				Status: v1.PodStatus{
					Phase: v1.PodPending,
					ContainerStatuses: []v1.ContainerStatus{
						containerStatusMap["ErrImagePull"],
					},
				},
			},
		},
		{
			name:      "pod CrashLoopBackOff",
			namespace: "default",
			UID:       "test-uid-2",
			oldPod: &v1.Pod{
				ObjectMeta: objectMeta,
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					ContainerStatuses: []v1.ContainerStatus{
						containerStatusMap["Running"],
					},
				},
			},
			newPod: &v1.Pod{
				ObjectMeta: objectMeta,
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					ContainerStatuses: []v1.ContainerStatus{
						containerStatusMap["CrashLoopBackOff"],
					},
				},
			},
		},
		{
			name:      "pod CrashLoopBackOff 2",
			namespace: "default",
			UID:       "test-uid-3",
			oldPod: &v1.Pod{
				ObjectMeta: objectMeta,
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					ContainerStatuses: []v1.ContainerStatus{
						containerStatusMap["CrashLoopBackOff"],
					},
				},
			},
			newPod: &v1.Pod{
				ObjectMeta: objectMeta,
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					ContainerStatuses: []v1.ContainerStatus{
						containerStatusMap["CrashLoopBackOff"],
					},
				},
			},
		},
	}

	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{
			Reclaim: config.ReclaimConfig{
				PendingJobTTLSeconds: 1,
			},
		},
	}

	driver.InitMockDB()
	err := models.CreateJob(&models.Job{
		ID:     jobName,
		Name:   jobName,
		Status: schema.StatusJobPending,
		Type:   string(schema.TypeVcJob),
		Config: &schema.Conf{
			Env: map[string]string{
				schema.EnvJobNamespace: "default",
			},
		},
	})
	assert.Equal(t, nil, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := newFakeJobSyncController()

			test.oldPod.UID = test.UID
			test.newPod.UID = test.UID
			oldObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(test.oldPod)
			assert.Equal(t, nil, err)
			newObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(test.newPod)
			assert.Equal(t, nil, err)

			oldObj := &unstructured.Unstructured{Object: oldObject}
			newObj := &unstructured.Unstructured{Object: newObject}

			stopCh := make(chan struct{})
			defer close(stopCh)
			c.Run(stopCh)
			// create volcano job
			vcjob := NewUnstructured(k8s.VCJobGVK, test.newPod.GetNamespace(), test.newPod.GetName())
			_, err = c.opt.DynamicClient.Resource(VCJobGVR).Namespace(vcjob.GetNamespace()).Create(context.TODO(), vcjob, metav1.CreateOptions{})
			assert.Equal(t, nil, err)

			c.addPod(oldObj)
			c.updatePod(oldObj, newObj)
			c.deletePod(newObj)
			time.Sleep(3 * time.Second)
		})
	}

}
