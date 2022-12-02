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
	"net/http/httptest"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	_ "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func NewUnstructured(gvk k8sschema.GroupVersionKind, namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvk.GroupVersion().String(),
			"kind":       gvk.Kind,
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
				"labels": map[string]interface{}{
					schema.JobOwnerLabel:       schema.JobOwnerValue,
					schema.JobLabelFramework:   string(schema.FrameworkStandalone),
					schema.VolcanoJobNameLabel: name,
					schema.JobIDLabel:          name,
				},
			},
			"status": make(map[string]interface{}),
		},
	}
}

func newFakeJobSyncController() *JobSync {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	ctrl := NewJobSync()
	opt := client.NewFakeKubeRuntimeClient(server)
	err := ctrl.Initialize(opt)
	if err != nil {
		log.Errorf("initialize controller failed: %v", err)
	}
	return ctrl
}

func initJobData(mockQueueID, mockClusterID, mockJobID string) error {
	driver.InitMockDB()
	err := storage.Cluster.CreateCluster(&model.ClusterInfo{Model: model.Model{ID: mockClusterID}, Status: model.ClusterStatusOnLine})
	if err != nil {
		return err
	}

	err = storage.Queue.CreateQueue(&model.Queue{Model: model.Model{ID: mockQueueID}, ClusterId: mockClusterID})
	if err != nil {
		return err
	}

	return storage.Job.CreateJob(&model.Job{
		ID: mockJobID,
		Config: &schema.Conf{
			Env: map[string]string{
				schema.EnvJobNamespace: "default",
			},
		},
		Framework: schema.FrameworkStandalone,
		Status:    schema.StatusJobTerminating,
		QueueID:   mockQueueID})
}

func TestJobSync(t *testing.T) {
	mockJobID := "test-job-id"
	mockSubJobID := "test-sub-job-id"
	mockNamespace := "default"

	testCases := []struct {
		name        string
		jobSyncInfo *api.JobSyncInfo
	}{
		{
			name: "sync add job",
			jobSyncInfo: &api.JobSyncInfo{
				ID:        mockJobID,
				Namespace: mockNamespace,
				Status:    schema.StatusJobPending,
				Action:    schema.Create,
			},
		},
		{
			name: "sync parent job",
			jobSyncInfo: &api.JobSyncInfo{
				ID:          mockSubJobID,
				ParentJobID: mockJobID,
				Namespace:   mockNamespace,
				Status:      schema.StatusJobPending,
				Action:      schema.Create,
			},
		},
		{
			name: "sync terminate job",
			jobSyncInfo: &api.JobSyncInfo{
				ID:        mockSubJobID,
				Namespace: mockNamespace,
				Status:    schema.StatusJobPending,
				Action:    schema.Terminate,
			},
		},
		{
			name: "sync update job to Running",
			jobSyncInfo: &api.JobSyncInfo{
				ID:        mockJobID,
				Namespace: mockNamespace,
				Status:    schema.StatusJobRunning,
				Action:    schema.Update,
			},
		},
		{
			name: "sync update job to Succeed",
			jobSyncInfo: &api.JobSyncInfo{
				ID:        mockJobID,
				Namespace: mockNamespace,
				Status:    schema.StatusJobSucceeded,
				Action:    schema.Update,
			},
		},
		{
			name: "sync delete job",
			jobSyncInfo: &api.JobSyncInfo{
				ID:        mockJobID,
				Namespace: mockNamespace,
				Status:    schema.StatusJobFailed,
				Action:    schema.Delete,
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
	c := newFakeJobSyncController()
	// init mock db
	mockQueueID := "mock-queue-id"
	err := initJobData(mockQueueID, c.runtimeClient.ClusterID(), "mock-job-id")
	assert.Equal(t, nil, err)
	// init new job
	err = storage.Job.CreateJob(&model.Job{
		ID: mockJobID,
		Config: &schema.Conf{
			Env: map[string]string{
				schema.EnvJobNamespace: "default",
			},
		},
		Framework: schema.FrameworkStandalone,
		Status:    schema.StatusJobPending,
	})
	assert.Equal(t, nil, err)

	stopCh := make(chan struct{})
	defer close(stopCh)
	c.Run(stopCh)

	// run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c.jobQueue.Add(tc.jobSyncInfo)
		})
	}

	for c.jobQueue.Len() > 0 {
		time.Sleep(100 * time.Millisecond)
	}
}

func TestJobTaskSync(t *testing.T) {
	mockJobID := "mock-job-id"
	mockTaskID := "mock-task-id"

	testCases := []struct {
		name     string
		taskInfo *api.TaskSyncInfo
	}{
		{
			name: "sync task",
			taskInfo: &api.TaskSyncInfo{
				ID:     mockTaskID,
				JobID:  mockJobID,
				Status: schema.StatusTaskRunning,
				Action: schema.Update,
				PodStatus: &v1.PodStatus{
					Phase: v1.PodRunning,
				},
			},
		},
		{
			name: "sync task delete",
			taskInfo: &api.TaskSyncInfo{
				ID:     mockTaskID,
				JobID:  mockJobID,
				Status: schema.StatusTaskSucceeded,
				Action: schema.Delete,
			},
		},
		{
			name: "sync task failed",
			taskInfo: &api.TaskSyncInfo{
				ID:     mockTaskID,
				JobID:  "test-job-id",
				Status: schema.StatusTaskSucceeded,
			},
		},
	}

	c := newFakeJobSyncController()
	// init mock db
	mockQueueID := "mock-queue-id"
	err := initJobData(mockQueueID, c.runtimeClient.ClusterID(), mockJobID)
	assert.Equal(t, nil, err)
	// start job controller
	stopCh := make(chan struct{})
	defer close(stopCh)
	c.Run(stopCh)

	// run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c.taskQueue.Add(tc.taskInfo)
		})
	}

	for c.taskQueue.Len() > 0 {
		time.Sleep(100 * time.Millisecond)
	}
}

func TestJobGC(t *testing.T) {
	testCases := []struct {
		name            string
		finishedJobInfo *api.FinishedJobInfo
	}{
		{
			name: "gc succeeded job",
			finishedJobInfo: &api.FinishedJobInfo{
				Name:             "test-job",
				Namespace:        "default",
				FrameworkVersion: schema.NewFrameworkVersion(k8s.PodGVK.Kind, k8s.PodGVK.GroupVersion().String()),
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
	c := newFakeJobSyncController()
	// init mock db
	mockQueueID := "mock-queue-id"
	err := initJobData(mockQueueID, c.runtimeClient.ClusterID(), "mock-job-id")
	assert.Equal(t, nil, err)
	// start controller
	stopCh := make(chan struct{})
	defer close(stopCh)
	c.Run(stopCh)

	// run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c.waitedCleanQueue.Add(tc.finishedJobInfo)
		})
	}
	for c.waitedCleanQueue.Len() > 0 {
		time.Sleep(100 * time.Millisecond)
	}
}

func TestTaskSync(t *testing.T) {
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
			schema.JobIDLabel: jobName,
		},
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: "batch.paddlepaddle.org/v1",
				Kind:       "PaddleJob",
				Name:       jobName,
			},
		},
	}

	tests := []struct {
		caseName  string
		name      string
		namespace string
		UID       types.UID
		oldPod    *v1.Pod
		newPod    *v1.Pod
	}{
		{
			caseName:  "pod pull image failed",
			name:      "pod-test-1",
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
			caseName:  "pod CrashLoopBackOff",
			name:      "pod-test-2",
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
			caseName:  "pod CrashLoopBackOff 2",
			name:      "pod-test-3",
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
	err := storage.Job.CreateJob(&model.Job{
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

	podFrameworkVer := schema.NewFrameworkVersion(k8s.PodGVK.Kind, k8s.PodGVK.GroupVersion().String())
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			c := newFakeJobSyncController()

			test.oldPod.UID = test.UID
			test.oldPod.Name = test.name
			test.newPod.UID = test.UID
			test.newPod.Name = test.name
			oldObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(test.oldPod)
			assert.Equal(t, nil, err)
			newObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(test.newPod)
			assert.Equal(t, nil, err)

			oldObj := &unstructured.Unstructured{Object: oldObject}
			newObj := &unstructured.Unstructured{Object: newObject}

			stopCh := make(chan struct{})
			defer close(stopCh)
			c.Run(stopCh)
			// create paddle job
			paddleJob := NewUnstructured(k8s.PodGVK, test.newPod.GetNamespace(), test.newPod.GetName())
			err = c.runtimeClient.Create(paddleJob, podFrameworkVer)
			assert.Equal(t, nil, err)
			// create old pod
			err = c.runtimeClient.Update(oldObj, podFrameworkVer)
			assert.Equal(t, nil, err)

			err = c.runtimeClient.Update(newObj, podFrameworkVer)
			assert.Equal(t, nil, err)

			time.Sleep(3 * time.Second)
		})
	}
}
