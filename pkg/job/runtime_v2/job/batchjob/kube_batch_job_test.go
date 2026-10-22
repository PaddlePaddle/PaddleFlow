/*
Copyright (c) 2024 PaddlePaddle Authors. All Rights Reserve.

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

package batchjob

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	customBatchJobYaml = `
apiVersion: batch/v1
kind: Job
metadata:
  name: custom-batch-job
  namespace: default
  labels:
    template-label: template-value
spec:
  backoffLimit: 3
  completions: 3
  parallelism: 3
  completionMode: Indexed
  template:
    spec:
      containers:
      - image: mockImage
        name: main
        resources:
          limits:
            cpu: "1"
            memory: "1"
      - image: sidecar:latest
        name: sidecar
      restartPolicy: OnFailure
status: {}
`
	batchJobWithoutRestartPolicyYaml = `
apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      containers:
      - image: mockImage
        name: main
`
	batchJobWithoutContainerYaml = `
apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      restartPolicy: Never
`
	batchJobWithWrongGVKYaml = `
apiVersion: v1
kind: Pod
spec:
  containers:
  - image: mockImage
    name: main
`
	batchJobWithAlwaysRestartPolicyYaml = `
apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      restartPolicy: Always
      containers:
      - image: mockImage
        name: main
`
)

func newMockBatchJob(id string) *api.PFJob {
	return &api.PFJob{
		ID:                id,
		Name:              id,
		Namespace:         "default",
		JobType:           schema.TypeBatch,
		Framework:         schema.FrameworkK8SBatch,
		QueueName:         "mockQueueName",
		ExtensionTemplate: []byte(customBatchJobYaml),
		Conf: schema.Conf{
			Name:             "normal",
			Priority:         schema.EnvJobHighPriority,
			KindGroupVersion: schema.BatchJobKindGroupVersion,
		},
	}
}

func TestKubeBatchJob_Submit(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = "testSchedulerName"
	defaultJobYamlPath := "../../../../../config/server/default/job/job_template.yaml"
	assert.NoError(t, config.InitJobTemplate(defaultJobYamlPath))

	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeRuntimeClient := client.NewFakeKubeRuntimeClient(server)
	driver.InitMockDB()

	batchJob := New(kubeRuntimeClient)

	t.Run("submit batch job template", func(t *testing.T) {
		jobObj := newMockBatchJob("job-batch-template")
		err := batchJob.Submit(context.TODO(), jobObj)
		assert.NoError(t, err)

		gettedJob := getBatchJob(t, kubeRuntimeClient, jobObj)
		// all fields unknown to the typed client should be preserved
		completions, found, err := unstructured.NestedInt64(gettedJob.Object, "spec", "completions")
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, int64(3), completions)
		parallelism, found, err := unstructured.NestedInt64(gettedJob.Object, "spec", "parallelism")
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, int64(3), parallelism)
		completionMode, found, err := unstructured.NestedString(gettedJob.Object, "spec", "completionMode")
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "Indexed", completionMode)
		backoffLimit, found, err := unstructured.NestedInt64(gettedJob.Object, "spec", "backoffLimit")
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, int64(3), backoffLimit)

		assert.Equal(t, jobObj.ID, gettedJob.GetName())
		assert.Equal(t, "default", gettedJob.GetNamespace())
		assert.Equal(t, schema.JobOwnerValue, gettedJob.GetLabels()[schema.JobOwnerLabel])
		assert.Equal(t, string(schema.FrameworkK8SBatch), gettedJob.GetLabels()[schema.JobLabelFramework])
		assert.Equal(t, jobObj.ID, gettedJob.GetLabels()[schema.JobIDLabel])
		assert.Equal(t, "mockQueueName", gettedJob.GetLabels()[schema.QueueLabelKey])
		assert.Equal(t, "mockQueueName", gettedJob.GetAnnotations()[schema.QueueLabelKey])
		assert.Equal(t, "mockQueueName", gettedJob.GetAnnotations()[schema.SchedulingQueueLabelKey])
		assert.Equal(t, "template-value", gettedJob.GetLabels()["template-label"])

		// child pods inherit labels and annotations from the pod template only
		podLabels, _, err := unstructured.NestedStringMap(gettedJob.Object, "spec", "template", "metadata", "labels")
		assert.NoError(t, err)
		assert.Equal(t, jobObj.ID, podLabels[schema.JobIDLabel])
		assert.Equal(t, schema.JobOwnerValue, podLabels[schema.JobOwnerLabel])
		assert.Equal(t, "mockQueueName", podLabels[schema.QueueLabelKey])
		_, find := podLabels[schema.JobLabelFramework]
		assert.False(t, find)
		podAnnotations, _, err := unstructured.NestedStringMap(gettedJob.Object,
			"spec", "template", "metadata", "annotations")
		assert.NoError(t, err)
		assert.Equal(t, "mockQueueName", podAnnotations[schema.QueueLabelKey])
		assert.Equal(t, "mockQueueName", podAnnotations[schema.SchedulingQueueLabelKey])

		// scheduling policy is injected into the pod template
		priorityClassName, _, err := unstructured.NestedString(gettedJob.Object,
			"spec", "template", "spec", "priorityClassName")
		assert.NoError(t, err)
		assert.Equal(t, schema.PriorityClassHigh, priorityClassName)
		schedulerName, _, err := unstructured.NestedString(gettedJob.Object,
			"spec", "template", "spec", "schedulerName")
		assert.NoError(t, err)
		assert.Equal(t, "testSchedulerName", schedulerName)

		// the sidecar container should be preserved
		containers, _, err := unstructured.NestedSlice(gettedJob.Object, "spec", "template", "spec", "containers")
		assert.NoError(t, err)
		assert.Equal(t, 2, len(containers))
	})

	t.Run("restart policy is defaulted to Never", func(t *testing.T) {
		jobObj := newMockBatchJob("job-batch-default-restart-policy")
		jobObj.ExtensionTemplate = []byte(batchJobWithoutRestartPolicyYaml)
		err := batchJob.Submit(context.TODO(), jobObj)
		assert.NoError(t, err)

		gettedJob := getBatchJob(t, kubeRuntimeClient, jobObj)
		restartPolicy, _, err := unstructured.NestedString(gettedJob.Object,
			"spec", "template", "spec", "restartPolicy")
		assert.NoError(t, err)
		assert.Equal(t, string(corev1.RestartPolicyNever), restartPolicy)
	})

	t.Run("job without extension template", func(t *testing.T) {
		jobObj := newMockBatchJob("job-batch-empty-template")
		jobObj.ExtensionTemplate = nil
		err := batchJob.Submit(context.TODO(), jobObj)
		assert.EqualError(t, err, "extensionTemplate for batch job default/job-batch-empty-template is needed")
	})

	t.Run("job without containers", func(t *testing.T) {
		jobObj := newMockBatchJob("job-batch-nil-container")
		jobObj.ExtensionTemplate = []byte(batchJobWithoutContainerYaml)
		err := batchJob.Submit(context.TODO(), jobObj)
		assert.EqualError(t, err, "spec.template.spec.containers of batch job is needed")
	})

	t.Run("job with wrong group version kind", func(t *testing.T) {
		jobObj := newMockBatchJob("job-batch-wrong-gvk")
		jobObj.ExtensionTemplate = []byte(batchJobWithWrongGVKYaml)
		err := batchJob.Submit(context.TODO(), jobObj)
		assert.EqualError(t, err, "expect GroupVersionKind is batch/v1, Kind=Job, but got /v1, Kind=Pod")
	})

	t.Run("job with unsupported restart policy", func(t *testing.T) {
		jobObj := newMockBatchJob("job-batch-always-restart-policy")
		jobObj.ExtensionTemplate = []byte(batchJobWithAlwaysRestartPolicyYaml)
		err := batchJob.Submit(context.TODO(), jobObj)
		assert.EqualError(t, err, "restartPolicy of batch job pod template must be Never or OnFailure, but got Always")
	})

	t.Run("nil job", func(t *testing.T) {
		err := batchJob.Submit(context.TODO(), nil)
		assert.EqualError(t, err, "job is nil")
	})

	t.Run("unsupported job type", func(t *testing.T) {
		jobObj := newMockBatchJob("job-batch-wrong-type")
		jobObj.JobType = schema.TypeDistributed
		err := batchJob.Submit(context.TODO(), jobObj)
		assert.EqualError(t, err, "batch job executor only supports job type batch, but got distributed")
	})
}

func getBatchJob(t *testing.T, kubeRuntimeClient *client.KubeRuntimeClient, jobObj *api.PFJob) *unstructured.Unstructured {
	obj, err := kubeRuntimeClient.Get(jobObj.Namespace, jobObj.ID, schema.BatchJobKindGroupVersion)
	if err != nil {
		t.Fatalf("get batch job %s failed, err: %v", jobObj.ID, err)
	}
	unObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("get batch job %s failed, unexpected object type %T", jobObj.ID, obj)
	}
	return unObj
}

func TestKubeBatchJob_JobStatus(t *testing.T) {
	now := metav1.NewTime(time.Now())
	testCases := []struct {
		name         string
		obj          interface{}
		wantStatus   schema.JobStatus
		wantFinished bool
		wantErr      bool
	}{
		{
			name:       "batch job is pending",
			obj:        newBatchJobUnstructured(t, batchv1.JobStatus{}),
			wantStatus: schema.StatusJobPending,
		},
		{
			name: "batch job is running",
			obj: newBatchJobUnstructured(t, batchv1.JobStatus{
				Active:    1,
				StartTime: &now,
			}),
			wantStatus: schema.StatusJobRunning,
		},
		{
			name: "batch job is running with succeeded pods",
			obj: newBatchJobUnstructured(t, batchv1.JobStatus{
				Succeeded: 1,
			}),
			wantStatus: schema.StatusJobRunning,
		},
		{
			name: "batch job is succeeded",
			obj: newBatchJobUnstructured(t, batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{
					{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
				},
				CompletionTime: &now,
			}),
			wantStatus:   schema.StatusJobSucceeded,
			wantFinished: true,
		},
		{
			name: "batch job is succeeded by completion time",
			obj: newBatchJobUnstructured(t, batchv1.JobStatus{
				CompletionTime: &now,
			}),
			wantStatus:   schema.StatusJobSucceeded,
			wantFinished: true,
		},
		{
			name: "batch job is failed",
			obj: newBatchJobUnstructured(t, batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{
					{
						Type:               batchv1.JobFailed,
						Status:             corev1.ConditionTrue,
						Message:            "job failed",
						LastTransitionTime: now,
					},
				},
			}),
			wantStatus:   schema.StatusJobFailed,
			wantFinished: true,
		},
		{
			name:    "non unstructured object",
			obj:     &batchv1.Job{},
			wantErr: true,
		},
	}

	batchJob := New(nil)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			statusInfo, err := batchJob.(*KubeBatchJob).JobStatus(tc.obj)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantStatus, statusInfo.Status)
			if tc.wantFinished {
				assert.NotNil(t, statusInfo.FinishedTime)
			}
		})
	}
}

func newBatchJobUnstructured(t *testing.T, status batchv1.JobStatus) *unstructured.Unstructured {
	job := &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Job",
			APIVersion: "batch/v1",
		},
		Status: status,
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(job)
	assert.NoError(t, err)
	return &unstructured.Unstructured{Object: obj}
}

func TestFilterFunc(t *testing.T) {
	testCases := []struct {
		name string
		obj  interface{}
		want bool
	}{
		{
			name: "batch job belongs to paddleflow",
			obj: &unstructured.Unstructured{Object: map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": map[string]interface{}{
						schema.JobOwnerLabel:     schema.JobOwnerValue,
						schema.JobLabelFramework: string(schema.FrameworkK8SBatch),
					},
				},
			}},
			want: true,
		},
		{
			name: "legacy batch job with standalone label",
			obj: &unstructured.Unstructured{Object: map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": map[string]interface{}{
						schema.JobOwnerLabel:     schema.JobOwnerValue,
						schema.JobLabelFramework: string(schema.FrameworkStandalone),
					},
				},
			}},
			want: true,
		},
		{
			name: "missing framework label",
			obj: &unstructured.Unstructured{Object: map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": map[string]interface{}{
						schema.JobOwnerLabel: schema.JobOwnerValue,
					},
				},
			}},
			want: false,
		},
		{
			name: "wrong framework label",
			obj: &unstructured.Unstructured{Object: map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": map[string]interface{}{
						schema.JobOwnerLabel:     schema.JobOwnerValue,
						schema.JobLabelFramework: "paddle",
					},
				},
			}},
			want: false,
		},
		{
			name: "not owned by paddleflow",
			obj: &unstructured.Unstructured{Object: map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": map[string]interface{}{
						schema.JobOwnerLabel:     "other",
						schema.JobLabelFramework: schema.FrameworkStandalone,
					},
				},
			}},
			want: false,
		},
		{
			name: "non unstructured object",
			obj:  &batchv1.Job{},
			want: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, filterFunc(tc.obj))
		})
	}
}
