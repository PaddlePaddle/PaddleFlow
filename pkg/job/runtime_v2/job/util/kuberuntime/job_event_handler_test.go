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

package kuberuntime

import (
	"fmt"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func TestGetParentJobID(t *testing.T) {
	testCases := []struct {
		name   string
		obj    *unstructured.Unstructured
		pJobID string
	}{
		{
			name: "no parent job",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"namespace": "default",
						"name":      "test-job",
					},
					"status": make(map[string]interface{}),
				}},
			pJobID: "",
		},
		{
			name: "parent job",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"namespace": "default",
						"name":      "test-job",
						"ownerReferences": []interface{}{
							map[string]interface{}{
								"apiVersion": k8s.ArgoWorkflowGVK.GroupVersion().String(),
								"kind":       k8s.ArgoWorkflowGVK.Kind,
								"name":       "argo-workflow1",
							},
						},
					},
					"status": make(map[string]interface{}),
				}},
			pJobID: "argo-workflow1",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			jobID := GetParentJobID(testCase.obj)
			assert.Equal(t, testCase.pJobID, jobID)
		})
	}
}

func TestJobAddFunc(t *testing.T) {
	testCases := []struct {
		name       string
		obj        *unstructured.Unstructured
		statusFunc api.GetStatusFunc
	}{
		{
			name: "job is created",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"namespace":       "default",
						"name":            "test-job",
						"resourceVersion": "1",
						"labels": map[string]interface{}{
							schema.JobOwnerLabel: schema.JobOwnerValue,
							schema.JobIDLabel:    "test-job",
						},
					},
					"status": map[string]interface{}{
						"phase": string(v1.PodPending),
					},
				},
			},
			statusFunc: func(i interface{}) (api.StatusInfo, error) {
				return api.StatusInfo{Status: schema.StatusJobPending}, nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			syncInfo, err := JobAddFunc(tc.obj, tc.statusFunc)
			t.Logf("sync info: %v, err: %v", syncInfo, err)
		})
	}
}

func TestJobUpdateFunc(t *testing.T) {
	testCases := []struct {
		name       string
		old        *unstructured.Unstructured
		new        *unstructured.Unstructured
		statusFunc api.GetStatusFunc
	}{
		{
			name: "job status changed from pending to running",
			old: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"namespace":       "default",
						"name":            "test-job",
						"resourceVersion": "1",
						"labels": map[string]interface{}{
							schema.JobOwnerLabel: schema.JobOwnerValue,
							schema.JobIDLabel:    "test-job",
						},
					},
					"status": map[string]interface{}{
						"phase": v1.PodPending,
					},
				},
			},
			new: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"namespace":       "default",
						"name":            "test-job",
						"resourceVersion": "2",
						"labels": map[string]interface{}{
							schema.JobOwnerLabel: schema.JobOwnerValue,
							schema.JobIDLabel:    "test-job",
						},
					},
					"status": map[string]interface{}{
						"phase": v1.PodRunning,
					},
				},
			},
			statusFunc: func(i interface{}) (api.StatusInfo, error) {
				obj := i.(*unstructured.Unstructured)
				job := &v1.Pod{}
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, job); err != nil {
					return api.StatusInfo{}, err
				}
				return api.StatusInfo{Status: schema.JobStatus(job.Status.Phase)}, nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			syncInfo, err := JobUpdateFunc(tc.old, tc.new, tc.statusFunc)
			t.Logf("sync info: %v, err: %v", syncInfo, err)
		})
	}
}

func TestJobDeleteFunc(t *testing.T) {
	testCases := []struct {
		name       string
		obj        *unstructured.Unstructured
		statusFunc api.GetStatusFunc
		expectErr  error
	}{
		{
			name: "delete single job",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"namespace": "default",
						"name":      "test-job",
						"labels": map[string]interface{}{
							schema.JobOwnerLabel: schema.JobOwnerValue,
							schema.JobIDLabel:    "test-job",
						},
					},
					"status": make(map[string]interface{}),
				},
			},
			statusFunc: func(i interface{}) (api.StatusInfo, error) {
				return api.StatusInfo{}, nil
			},
			expectErr: nil,
		},
		{
			name: "delete job status failed",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"namespace": "default",
						"name":      "test-job",
						"labels": map[string]interface{}{
							schema.JobOwnerLabel: schema.JobOwnerValue,
							schema.JobIDLabel:    "test-job",
						},
					},
					"status": make(map[string]interface{}),
				},
			},
			statusFunc: func(i interface{}) (api.StatusInfo, error) {
				return api.StatusInfo{}, fmt.Errorf("status is unknown")
			},
			expectErr: fmt.Errorf("status is unknown"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			jobSyncInfo, err := JobDeleteFunc(tc.obj, tc.statusFunc)
			t.Logf("job sync info: %v, err: %v", jobSyncInfo, err)
			assert.Equal(t, tc.expectErr, err)
		})
	}
}

func TestTaskUpdate(t *testing.T) {
	testCases := []struct {
		name string
		old  *unstructured.Unstructured
		new  *unstructured.Unstructured
	}{
		{
			name: "task status is pending",
			old: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"name":      "test-name",
						"namespace": "default",
						"labels": map[string]interface{}{
							schema.JobOwnerLabel: schema.JobOwnerValue,
							schema.JobIDLabel:    "test-job",
						},
					},
					"status": map[string]interface{}{
						"phase": v1.PodPending,
					},
				},
			},
			new: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"name":      "test-name",
						"namespace": "default",
						"labels": map[string]interface{}{
							schema.JobOwnerLabel: schema.JobOwnerValue,
							schema.JobIDLabel:    "test-job",
						},
					},
					"status": map[string]interface{}{
						"phase": v1.PodPending,
						"initContainerStatuses": []interface{}{
							map[string]interface{}{
								"name": "init-c",
								"state": map[string]interface{}{
									"waiting": map[string]interface{}{
										"reason":  "ImagePullBackOff",
										"message": "Back-off pulling image xxx",
									},
								},
							},
						},
						"containerStatuses": []interface{}{
							map[string]interface{}{
								"name": "init-c",
								"state": map[string]interface{}{
									"waiting": map[string]interface{}{
										"reason":  "ImagePullBackOff",
										"message": "Back-off pulling image xxx",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "task status change from pending to running",
			old: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"name":      "test-name",
						"namespace": "default",
						"labels": map[string]interface{}{
							schema.JobOwnerLabel: schema.JobOwnerValue,
							schema.JobIDLabel:    "test-job",
						},
					},
					"status": map[string]interface{}{
						"phase": v1.PodPending,
					},
				},
			},
			new: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"name":      "test-name",
						"namespace": "default",
					},
					"status": map[string]interface{}{
						"phase": v1.PodRunning,
					},
				},
			},
		},
	}

	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{
			Reclaim: config.ReclaimConfig{
				PendingJobTTLSeconds: 60,
			},
		},
	}
	taskQueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	jobQueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			TaskUpdate(tc.old, tc.new, taskQueue, jobQueue)
		})
	}
}

func TestHandlePendingPod(t *testing.T) {
	testCases := []struct {
		name      string
		podName   string
		namespace string
		podStatus *v1.PodStatus
		jobName   string
	}{
		{
			name:      "init container is creating",
			podName:   "test-pod-name",
			namespace: "default",
			jobName:   "test-job-name",
			podStatus: &v1.PodStatus{
				Phase: v1.PodPending,
				InitContainerStatuses: []v1.ContainerStatus{
					{
						Name: "init-c",
						State: v1.ContainerState{
							Waiting: &v1.ContainerStateWaiting{
								Reason:  "ImagePullBackOff",
								Message: "Back-off pulling image xxx",
							},
						},
					},
				},
			},
		},
		{
			name:      "container is creating",
			podName:   "test-pod-name",
			namespace: "default",
			jobName:   "test-job-name",
			podStatus: &v1.PodStatus{
				Phase: v1.PodPending,
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name: "test-c",
						State: v1.ContainerState{
							Waiting: &v1.ContainerStateWaiting{
								Reason:  "ImagePullBackOff",
								Message: "Back-off pulling image xxx",
							},
						},
					},
				},
			},
		},
		{
			name:      "pod is initializing",
			podName:   "test-pod-name",
			namespace: "default",
			jobName:   "test-job-name",
			podStatus: &v1.PodStatus{
				Phase: v1.PodPending,
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name: "test-c",
						State: v1.ContainerState{
							Waiting: &v1.ContainerStateWaiting{
								Reason:  PodInitializing,
								Message: "xxx",
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
				PendingJobTTLSeconds: 60,
			},
		},
	}
	jobQueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			handlePendingPod(tc.podStatus, tc.jobName, tc.podName, tc.namespace, jobQueue)
		})
	}
}

func TestTaskUpdateFunc(t *testing.T) {
	testCases := []struct {
		name   string
		obj    *unstructured.Unstructured
		action schema.ActionType
	}{
		{
			name: "task is pending",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"name":      "test-name",
						"namespace": "default",
						"labels": map[string]interface{}{
							schema.JobOwnerLabel: schema.JobOwnerValue,
							schema.JobIDLabel:    "test-job",
						},
					},
					"status": map[string]interface{}{
						"phase": v1.PodPending,
					},
				},
			},
			action: schema.Update,
		},
		{
			name: "the job of task is nil",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"labels": map[string]interface{}{
						"a": "b",
					},
				},
			},
			action: schema.Update,
		},
	}

	taskQueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			TaskUpdateFunc(tc.obj, tc.action, taskQueue)
		})
	}
}

func TestGetJobByTask(t *testing.T) {
	testCases := []struct {
		name      string
		obj       *unstructured.Unstructured
		expectRet string
	}{
		{
			name:      "the task is nil",
			obj:       nil,
			expectRet: "",
		},
		{
			name: "the job of task is nil",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"labels": map[string]interface{}{
						"a": "b",
					},
				},
			},
			expectRet: "",
		},
		{
			name: "get job id from ownerReferences",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							schema.JobOwnerLabel: schema.JobOwnerValue,
						},
						"ownerReferences": []interface{}{
							map[string]interface{}{
								"kind":       k8s.PaddleJobGVK.Kind,
								"apiVersion": k8s.PaddleJobGVK.GroupVersion().String(),
								"name":       "test-paddle-job",
							},
						},
					},
				},
			},
			expectRet: "test-paddle-job",
		},
		{
			name: "get job id from job id label",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							schema.JobOwnerLabel: schema.JobOwnerValue,
							schema.JobIDLabel:    "test-job",
						},
					},
				},
			},
			expectRet: "test-job",
		},
		{
			name: "get job id from job id label failed",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": k8s.PodGVK.GroupVersion().String(),
					"kind":       k8s.PodGVK.Kind,
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							schema.JobOwnerLabel: schema.JobOwnerValue,
						},
					},
				},
			},
			expectRet: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := getJobByTask(tc.obj)
			t.Logf("get job by task: %s", msg)
			assert.Equal(t, tc.expectRet, msg)
		})
	}
}

func TestGetTaskStatus(t *testing.T) {
	testCases := []struct {
		name         string
		status       *v1.PodStatus
		expectStatus schema.TaskStatus
		expectErr    error
	}{
		{
			name: "task is pending",
			status: &v1.PodStatus{
				Phase: v1.PodPending,
			},
			expectStatus: schema.StatusTaskPending,
			expectErr:    nil,
		},
		{
			name: "task is running",
			status: &v1.PodStatus{
				Phase: v1.PodRunning,
			},
			expectStatus: schema.StatusTaskRunning,
			expectErr:    nil,
		},
		{
			name: "task is succeeded",
			status: &v1.PodStatus{
				Phase: v1.PodSucceeded,
			},
			expectStatus: schema.StatusTaskSucceeded,
			expectErr:    nil,
		},
		{
			name: "task is failed",
			status: &v1.PodStatus{
				Phase: v1.PodFailed,
			},
			expectStatus: schema.StatusTaskFailed,
			expectErr:    nil,
		},
		{
			name:         "task status is nil",
			status:       nil,
			expectStatus: schema.TaskStatus(""),
			expectErr:    fmt.Errorf("the status of pod is nil"),
		},
		{
			name: "task phase is not expect",
			status: &v1.PodStatus{
				Phase: v1.PodPhase("xx"),
			},
			expectStatus: schema.TaskStatus(""),
			expectErr:    fmt.Errorf("unexpected task status: xx"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			taskStatus, err := GetTaskStatus(tc.status)
			assert.Equal(t, tc.expectStatus, taskStatus)
			assert.Equal(t, tc.expectErr, err)
		})
	}
}

func TestPodStatusMessage(t *testing.T) {
	testCases := []struct {
		name      string
		PodStatus PodStatusMessage
		expectStr string
	}{
		{
			name: "pod status",
			PodStatus: PodStatusMessage{
				Phase:   v1.PodPending,
				Message: "Container Creating",
				Reason:  "ImgPullFailed",
				ContainerMessages: []ContainerStatusMessage{
					{
						Name:         "test-1",
						ContainerID:  "test-id",
						RestartCount: 0,
						WaitingState: &v1.ContainerStateWaiting{
							Reason:  "xx",
							Message: "ImgPullFailed",
						},
						TerminatedState: &v1.ContainerStateTerminated{
							Message: "xx",
						},
					},
				},
			},
			expectStr: "pod phase is Pending with reason ImgPullFailed, detail message: Container Creating. " +
				"Containers status: container test-1 with restart count 0, id is test-id, wating with reason xx, " +
				"message: ImgPullFailed, terminated with exitCode 0, reason is , message: xx;",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			psStr := tc.PodStatus.String()
			t.Logf("pod status string: %s", psStr)
			assert.Equal(t, tc.expectStr, psStr)
		})
	}
}

func TestGetTaskMessage(t *testing.T) {
	testCases := []struct {
		name      string
		status    *v1.PodStatus
		expectRet string
	}{
		{
			name: "init task is pending",
			status: &v1.PodStatus{
				Phase:   v1.PodPending,
				Reason:  "",
				Message: "Container is creating",
				InitContainerStatuses: []v1.ContainerStatus{
					{
						Name:         "init-test",
						ContainerID:  "init-id",
						RestartCount: 0,
						State: v1.ContainerState{
							Waiting: &v1.ContainerStateWaiting{
								Reason: "ImgPullErr",
							},
						},
					},
				},
			},
			expectRet: "pod phase is Pending, detail message: Container is creating. Containers status: " +
				"container init-test with restart count 0, id is init-id, wating with reason ImgPullErr;",
		},
		{
			name: "task is pending",
			status: &v1.PodStatus{
				Phase:   v1.PodPending,
				Reason:  "",
				Message: "Container is creating",
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name:         "test",
						ContainerID:  "test-id",
						RestartCount: 0,
						State: v1.ContainerState{
							Waiting: &v1.ContainerStateWaiting{
								Reason: "ImgPullErr",
							},
						},
					},
				},
			},
			expectRet: "pod phase is Pending, detail message: Container is creating. " +
				"Containers status: container test with restart count 0, id is test-id, wating with reason ImgPullErr;",
		},
		{
			name:      "task status is nil",
			status:    nil,
			expectRet: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := GetTaskMessage(tc.status)
			t.Logf("task message: %s", msg)
			assert.Equal(t, tc.expectRet, msg)
		})
	}
}
