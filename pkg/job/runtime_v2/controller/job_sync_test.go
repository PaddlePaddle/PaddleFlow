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
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
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
					schema.VolcanoJobNameLabel: name,
					schema.JobIDLabel:          name,
				},
			},
			"status": make(map[string]interface{}),
		},
	}
}

func newFakeJobSyncController() *JobSync {
	scheme := runtime.NewScheme()
	dynamicClient := fakedynamicclient.NewSimpleDynamicClient(scheme)

	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	fakeDiscovery := discovery.NewDiscoveryClientForConfigOrDie(&restclient.Config{Host: server.URL})

	ctrl := &JobSync{}
	opt := &client.KubeRuntimeClient{
		DynamicClient:   dynamicClient,
		DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
		DiscoveryClient: fakeDiscovery,
		ClusterInfo: &schema.Cluster{
			Name: "default-cluster",
			ID:   "cluster-123",
			Type: "Kubernetes",
		},
		JobInformerMap: make(map[k8sschema.GroupVersionKind]cache.SharedIndexInformer),
		Config:         &restclient.Config{Host: server.URL},
	}
	err := ctrl.Initialize(opt)
	if err != nil {
		log.Errorf("initialize controller failed: %v", err)
	}
	return ctrl
}

func TestJobSyncAndGC(t *testing.T) {
	tests := []struct {
		name             string
		namespace        string
		frameworkVersion schema.FrameworkVersion
		oldObj           *unstructured.Unstructured
		newObj           *unstructured.Unstructured
		oldStatus        interface{}
		newStatus        interface{}
	}{
		{
			name:             "job status from pending turn to running",
			namespace:        "default",
			frameworkVersion: schema.NewFrameworkVersion(k8s.PodGVK.Kind, k8s.PodGVK.GroupVersion().String()),
			oldObj:           NewUnstructured(k8s.PodGVK, "default", "test-job1"),
			oldStatus: &v1.PodStatus{
				Phase: v1.PodPending,
			},
			wantErr: nil,
		},
		{
			name: "client is nil",
			args: args{
				jobSync: ctrl,
				opt:     nil,
			},
			wantErr: fmt.Errorf("init JobSync failed"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			err := tt.args.jobSync.Initialize(tt.args.opt)
			if err != nil {
				assert.Equal(t, tt.wantErr, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

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
	mockJobID2 := "test-job-id2"
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
			name:             "job status from running turn to failed",
			namespace:        "default",
			frameworkVersion: schema.NewFrameworkVersion(k8s.PodGVK.Kind, k8s.PodGVK.GroupVersion().String()),
			oldObj:           NewUnstructured(k8s.PodGVK, "default", "test-job2"),
			oldStatus: &v1.PodStatus{
				Phase: v1.PodRunning,
			},
			newObj: NewUnstructured(k8s.PodGVK, "default", "test-job2"),
			newStatus: &v1.PodStatus{
				Phase: v1.PodFailed,
			},
		},
		{
			name:             "job status from running turn to succeeded",
			namespace:        "default",
			frameworkVersion: schema.NewFrameworkVersion(k8s.PodGVK.Kind, k8s.PodGVK.GroupVersion().String()),
			oldObj:           NewUnstructured(k8s.PodGVK, "default", "test-job3"),
			oldStatus: &v1.PodStatus{
				Phase: v1.PodRunning,
			},
			newObj: NewUnstructured(k8s.PodGVK, "default", "test-job3"),
			newStatus: &v1.PodStatus{
				Phase: v1.PodSucceeded,
			},
		},
		{
			name: "sync preempting job",
			jobSyncInfo: &api.JobSyncInfo{
				ID:        mockJobID2,
				Namespace: mockNamespace,
				Status:    schema.StatusJobPreempting,
				Action:    schema.Create,
			},
		},
		{
			name: "sync preempted job",
			jobSyncInfo: &api.JobSyncInfo{
				ID:        mockJobID2,
				Namespace: mockNamespace,
				Status:    schema.StatusJobPreempted,
				Action:    schema.Update,
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
	err = storage.Job.CreateJob(&model.Job{
		ID: mockJobID2,
		Config: &schema.Conf{
			Env: map[string]string{
				schema.EnvJobNamespace: "default",
			},
		},
		Framework: schema.FrameworkStandalone,
		Status:    schema.StatusJobPending,
	})
	assert.Equal(t, nil, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver.InitMockDB()
			err := storage.Job.CreateJob(&model.Job{ID: test.newObj.GetName()})
			assert.Equal(t, nil, err)

			c := newFakeJobSyncController()
			stopCh := make(chan struct{})
			defer close(stopCh)
			c.Run(stopCh)

			test.oldObj.Object["status"], err = runtime.DefaultUnstructuredConverter.ToUnstructured(test.oldStatus)
			assert.Equal(t, nil, err)

			test.newObj.Object["status"], err = runtime.DefaultUnstructuredConverter.ToUnstructured(test.newStatus)
			assert.Equal(t, nil, err)

			err = c.runtimeClient.Create(test.oldObj, test.frameworkVersion)
			assert.Equal(t, nil, err)
			// create old pod
			err = c.runtimeClient.Update(test.newObj, test.frameworkVersion)
			assert.Equal(t, nil, err)

			time.Sleep(5 * time.Second)
		})
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
