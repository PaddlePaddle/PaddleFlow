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

package k8s

import (
	"fmt"
	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"testing"

	paddlejobv1 "github.com/paddleflow/paddle-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	sparkoperatorv1beta2 "github.com/PaddlePaddle/PaddleFlow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const unknown = "unknown"

func TestSparkAppStatus(t *testing.T) {
	tests := []struct {
		name               string
		status             *sparkoperatorv1beta2.SparkApplicationStatus
		expectError        error
		expectStatus       schema.JobStatus
		expectOriginStatus string
	}{
		{
			name: "spark state nwe",
			status: &sparkoperatorv1beta2.SparkApplicationStatus{
				AppState: sparkoperatorv1beta2.ApplicationState{
					State: sparkoperatorv1beta2.NewState,
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobPending,
			expectOriginStatus: string(sparkoperatorv1beta2.NewState),
		},
		{
			name: "spark state running",
			status: &sparkoperatorv1beta2.SparkApplicationStatus{
				AppState: sparkoperatorv1beta2.ApplicationState{
					State: sparkoperatorv1beta2.RunningState,
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobRunning,
			expectOriginStatus: string(sparkoperatorv1beta2.RunningState),
		},
		{
			name: "spark state completed",
			status: &sparkoperatorv1beta2.SparkApplicationStatus{
				AppState: sparkoperatorv1beta2.ApplicationState{
					State: sparkoperatorv1beta2.CompletedState,
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobSucceeded,
			expectOriginStatus: string(sparkoperatorv1beta2.CompletedState),
		},
		{
			name: "spark state failed",
			status: &sparkoperatorv1beta2.SparkApplicationStatus{
				AppState: sparkoperatorv1beta2.ApplicationState{
					State: sparkoperatorv1beta2.FailedState,
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobFailed,
			expectOriginStatus: string(sparkoperatorv1beta2.FailedState),
		},
		{
			name: "spark state unknown",
			status: &sparkoperatorv1beta2.SparkApplicationStatus{
				AppState: sparkoperatorv1beta2.ApplicationState{
					State: unknown,
				},
			},
			expectError:        fmt.Errorf("unexpected spark application status: %s", unknown),
			expectStatus:       "",
			expectOriginStatus: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var statusInfo StatusInfo
			obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(test.status)
			assert.Equal(t, nil, err)
			unstructuredObj := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"status": obj,
				},
			}
			statusInfo, err = SparkAppStatus(unstructuredObj)
			assert.Equal(t, test.expectError, err)
			assert.Equal(t, test.expectStatus, statusInfo.Status)
			assert.Equal(t, test.expectOriginStatus, statusInfo.OriginStatus)
		})
	}
}

func TestVCJobStatus(t *testing.T) {
	tests := []struct {
		name               string
		status             *batchv1alpha1.JobStatus
		expectError        error
		expectStatus       schema.JobStatus
		expectOriginStatus string
	}{
		{
			name: "vc job state Pending",
			status: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Pending,
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobPending,
			expectOriginStatus: string(batchv1alpha1.Pending),
		},
		{
			name: "vc job state Running",
			status: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Running,
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobRunning,
			expectOriginStatus: string(batchv1alpha1.Running),
		},
		{
			name: "vc job state Terminating",
			status: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Terminating,
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobTerminating,
			expectOriginStatus: string(batchv1alpha1.Terminating),
		},
		{
			name: "vc job state Completed",
			status: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Completed,
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobSucceeded,
			expectOriginStatus: string(batchv1alpha1.Completed),
		},
		{
			name: "vc job state Aborted",
			status: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Aborted,
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobTerminated,
			expectOriginStatus: string(batchv1alpha1.Aborted),
		},
		{
			name: "vc job state Failed",
			status: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Failed,
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobFailed,
			expectOriginStatus: string(batchv1alpha1.Failed),
		},
		{
			name: "vc job state unknown",
			status: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: unknown,
				},
			},
			expectError:        fmt.Errorf("unexpected vcjob status: %s", unknown),
			expectStatus:       "",
			expectOriginStatus: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var statusInfo StatusInfo
			obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(test.status)
			assert.Equal(t, nil, err)
			unstructuredObj := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"status": obj,
				},
			}
			statusInfo, err = VCJobStatus(unstructuredObj)
			assert.Equal(t, test.expectError, err)
			assert.Equal(t, test.expectStatus, statusInfo.Status)
			assert.Equal(t, test.expectOriginStatus, statusInfo.OriginStatus)
		})
	}
}

func TestPaddleJobStatus(t *testing.T) {
	tests := []struct {
		name               string
		status             *paddlejobv1.PaddleJobStatus
		expectError        error
		expectStatus       schema.JobStatus
		expectOriginStatus string
	}{
		{
			name: "paddle job state Pending",
			status: &paddlejobv1.PaddleJobStatus{
				Phase: paddlejobv1.Pending,
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobPending,
			expectOriginStatus: string(paddlejobv1.Pending),
		},
		{
			name: "paddle job state Running",
			status: &paddlejobv1.PaddleJobStatus{
				Phase: paddlejobv1.Running,
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobRunning,
			expectOriginStatus: string(paddlejobv1.Running),
		},
		{
			name: "paddle job state Terminating",
			status: &paddlejobv1.PaddleJobStatus{
				Phase: paddlejobv1.Terminating,
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobTerminating,
			expectOriginStatus: string(paddlejobv1.Terminating),
		},
		{
			name: "paddle job state Completed",
			status: &paddlejobv1.PaddleJobStatus{
				Phase: paddlejobv1.Completed,
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobSucceeded,
			expectOriginStatus: string(paddlejobv1.Completed),
		},
		{
			name: "paddle job state Completed",
			status: &paddlejobv1.PaddleJobStatus{
				Phase: paddlejobv1.Aborted,
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobTerminated,
			expectOriginStatus: string(paddlejobv1.Aborted),
		},
		{
			name: "paddle job state Failed",
			status: &paddlejobv1.PaddleJobStatus{
				Phase: paddlejobv1.Failed,
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobFailed,
			expectOriginStatus: string(paddlejobv1.Failed),
		},
		{
			name: "paddle job state unknown",
			status: &paddlejobv1.PaddleJobStatus{
				Phase: unknown,
			},
			expectError:        fmt.Errorf("unexpected paddlejob status: %s", unknown),
			expectStatus:       "",
			expectOriginStatus: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var statusInfo StatusInfo
			obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(test.status)
			assert.Equal(t, nil, err)
			unstructuredObj := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"status": obj,
				},
			}
			statusInfo, err = PaddleJobStatus(unstructuredObj)
			assert.Equal(t, test.expectError, err)
			assert.Equal(t, test.expectStatus, statusInfo.Status)
			assert.Equal(t, test.expectOriginStatus, statusInfo.OriginStatus)
		})
	}
}

func TestKubeflowJobStatus(t *testing.T) {
	tests := []struct {
		name               string
		status             *kubeflowv1.JobStatus
		expectError        error
		expectStatus       schema.JobStatus
		expectOriginStatus string
	}{
		{
			name: "kubeflow job state Created",
			status: &kubeflowv1.JobStatus{
				Conditions: []kubeflowv1.JobCondition{
					{
						Message: "PyTorchJob pytorch-dist-basic-sendrecv is created.",
						Reason:  "PyTorchJobCreated",
						Type:    kubeflowv1.JobCreated,
					},
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobPending,
			expectOriginStatus: string(kubeflowv1.JobCreated),
		},
		{
			name: "kubeflow job state Running",
			status: &kubeflowv1.JobStatus{
				Conditions: []kubeflowv1.JobCondition{
					{
						Message: "PyTorchJob pytorch-dist-basic-sendrecv is created.",
						Reason:  "PyTorchJobCreated",
						Type:    kubeflowv1.JobCreated,
					},
					{
						Message: "PyTorchJob pytorch-dist-basic-sendrecv is running.",
						Reason:  "JobRunning",
						Type:    kubeflowv1.JobRunning,
					},
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobRunning,
			expectOriginStatus: string(kubeflowv1.JobRunning),
		},
		{
			name: "kubeflow job state Restarting",
			status: &kubeflowv1.JobStatus{
				Conditions: []kubeflowv1.JobCondition{
					{
						Message: "PyTorchJob pytorch-dist-basic-sendrecv is created.",
						Reason:  "PyTorchJobCreated",
						Type:    kubeflowv1.JobCreated,
					},
					{
						Message: "PyTorchJob pytorch-dist-basic-sendrecv is running.",
						Reason:  "JobRunning",
						Type:    kubeflowv1.JobRunning,
					},
					{
						Message: "PyTorchJob pytorch-dist-basic-sendrecv is running.",
						Reason:  "JobRunning",
						Type:    kubeflowv1.JobRestarting,
					},
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobRunning,
			expectOriginStatus: string(kubeflowv1.JobRestarting),
		},
		{
			name: "kubeflow job state succeeded",
			status: &kubeflowv1.JobStatus{
				Conditions: []kubeflowv1.JobCondition{
					{
						Message: "PyTorchJob pytorch-dist-basic-sendrecv is created.",
						Reason:  "PyTorchJobCreated",
						Type:    kubeflowv1.JobCreated,
					},
					{
						Message: "PyTorchJob pytorch-dist-basic-sendrecv is running.",
						Reason:  "JobRunning",
						Type:    kubeflowv1.JobRunning,
					},
					{
						Message: "PyTorchJob pytorch-dist-basic-sendrecv is successfully completed.",
						Reason:  "JobSucceeded",
						Type:    kubeflowv1.JobSucceeded,
					},
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobSucceeded,
			expectOriginStatus: string(kubeflowv1.JobSucceeded),
		},
		{
			name: "kubeflow job state Failed",
			status: &kubeflowv1.JobStatus{
				Conditions: []kubeflowv1.JobCondition{
					{
						Message: "PyTorchJob pytorch-dist-basic-sendrecv is created.",
						Reason:  "PyTorchJobCreated",
						Type:    kubeflowv1.JobCreated,
					},
					{
						Message: "PyTorchJob pytorch-dist-basic-sendrecv is running.",
						Reason:  "JobRunning",
						Type:    kubeflowv1.JobRunning,
					},
					{
						Message: "PyTorchJob pytorch-test-failed-2 is failed because 1 Worker replica(s) failed.",
						Reason:  "JobFailed",
						Type:    kubeflowv1.JobFailed,
					},
				},
			},
			expectError:        nil,
			expectStatus:       schema.StatusJobFailed,
			expectOriginStatus: string(kubeflowv1.JobFailed),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var statusInfo StatusInfo
			obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(test.status)
			assert.Equal(t, nil, err)
			unstructuredObj := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"status": obj,
				},
			}
			statusInfo, err = PytorchJobStatus(unstructuredObj.DeepCopy())
			assert.Equal(t, test.expectError, err)
			assert.Equal(t, test.expectStatus, statusInfo.Status)
			assert.Equal(t, test.expectOriginStatus, statusInfo.OriginStatus)

			statusInfo, err = TFJobStatus(unstructuredObj.DeepCopy())
			assert.Equal(t, test.expectError, err)
			assert.Equal(t, test.expectStatus, statusInfo.Status)
			assert.Equal(t, test.expectOriginStatus, statusInfo.OriginStatus)

			statusInfo, err = MPIJobStatus(unstructuredObj.DeepCopy())
			assert.Equal(t, test.expectError, err)
			assert.Equal(t, test.expectStatus, statusInfo.Status)
			assert.Equal(t, test.expectOriginStatus, statusInfo.OriginStatus)
		})
	}
}
