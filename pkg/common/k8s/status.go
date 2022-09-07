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

	wfv1 "github.com/argoproj/argo/pkg/apis/workflow/v1alpha1"
	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	paddlejobv1 "github.com/paddleflow/paddle-operator/api/v1"
	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	rayV1alpha1 "github.com/PaddlePaddle/PaddleFlow/pkg/apis/ray-operator/v1alpha1"
	sparkoperatorv1beta2 "github.com/PaddlePaddle/PaddleFlow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func ConvertToStatus(obj interface{}, gvk k8sschema.GroupVersionKind) (interface{}, error) {
	var realStatus interface{}
	switch gvk {
	case SparkAppGVK:
		realStatus = &sparkoperatorv1beta2.SparkApplicationStatus{}
	case VCJobGVK:
		realStatus = &batchv1alpha1.JobStatus{}
	case PaddleJobGVK:
		realStatus = &paddlejobv1.PaddleJobStatus{}
	case PyTorchJobGVK, TFJobGVK, MPIJobGVK, MXNetJobGVK, XGBoostJobGVK:
		realStatus = &kubeflowv1.JobStatus{}
	case ArgoWorkflowGVK:
		realStatus = &wfv1.WorkflowStatus{}
	case PodGVK:
		realStatus = &v1.PodStatus{}
	case RayJobGVK:
		realStatus = &rayV1alpha1.RayJobStatus{}
	default:
		return nil, fmt.Errorf("the group version kind %s is not supported", gvk.String())
	}
	if obj == nil {
		return realStatus, nil
	}
	// Get status from unstructured object
	jobObj := obj.(*unstructured.Unstructured).DeepCopy()
	status, ok, unerr := unstructured.NestedFieldCopy(jobObj.Object, "status")
	if !ok {
		if unerr != nil {
			log.Errorf("NestedFieldCopy unstructured to status error: %v", unerr)
			return realStatus, unerr
		}
		log.Info("NestedFieldCopy unstructured to status error: Status is not found in job")
		return realStatus, nil
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(status.(map[string]interface{}), realStatus); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s status failed. error: %s", obj, gvk.String(), err.Error())
		return nil, err
	}
	return realStatus, nil
}

func GetTaskStatus(podStatus *v1.PodStatus) (schema.TaskStatus, error) {
	status := schema.TaskStatus("")
	if podStatus == nil {
		return status, fmt.Errorf("the status of pod is nil")
	}
	switch podStatus.Phase {
	case v1.PodPending:
		status = schema.StatusTaskPending
	case v1.PodRunning:
		status = schema.StatusTaskRunning
	case v1.PodSucceeded:
		status = schema.StatusTaskSucceeded
	case v1.PodFailed, v1.PodUnknown:
		status = schema.StatusTaskFailed
	default:
		return status, fmt.Errorf("unexpected task status: %s", podStatus.Phase)
	}
	return status, nil
}

// SparkAppStatus get spark application status, message from interface{}, and covert to JobStatus
func SparkAppStatus(obj interface{}) (StatusInfo, error) {
	status, err := ConvertToStatus(obj, SparkAppGVK)
	if err != nil {
		return StatusInfo{}, err
	}
	jobStatus := status.(*sparkoperatorv1beta2.SparkApplicationStatus)
	state, msg, err := getSparkJobStatus(jobStatus.AppState.State)
	if err != nil {
		log.Errorf("convert VCJob status to JobStatus failed, err: %v", err)
		return StatusInfo{}, err
	}
	log.Infof("Spark application status: %s", state)
	return StatusInfo{
		OriginStatus: string(jobStatus.AppState.State),
		Status:       state,
		Message:      msg,
	}, nil
}

func getSparkJobStatus(state sparkoperatorv1beta2.ApplicationStateType) (schema.JobStatus, string, error) {
	status := schema.JobStatus("")
	msg := ""
	switch state {
	case sparkoperatorv1beta2.NewState, sparkoperatorv1beta2.SubmittedState:
		status = schema.StatusJobPending
		msg = "spark application is pending"
	case sparkoperatorv1beta2.RunningState, sparkoperatorv1beta2.SucceedingState, sparkoperatorv1beta2.FailingState,
		sparkoperatorv1beta2.InvalidatingState, sparkoperatorv1beta2.PendingRerunState:
		status = schema.StatusJobRunning
		msg = "spark application is running"
	case sparkoperatorv1beta2.CompletedState:
		status = schema.StatusJobSucceeded
		msg = "spark application is succeeded"
	case sparkoperatorv1beta2.FailedState, sparkoperatorv1beta2.FailedSubmissionState, sparkoperatorv1beta2.UnknownState:
		status = schema.StatusJobFailed
		msg = "spark application is failed"
	default:
		return status, msg, fmt.Errorf("unexpected spark application status: %s", state)
	}
	return status, msg, nil
}

// VCJobStatus get vc job status, message from interface{}, and covert to JobStatus
func VCJobStatus(obj interface{}) (StatusInfo, error) {
	status, err := ConvertToStatus(obj, VCJobGVK)
	if err != nil {
		log.Errorf("convert VCJob status failed, err: %v", err)
		return StatusInfo{}, err
	}
	jobStatus := status.(*batchv1alpha1.JobStatus)
	state, msg, err := getVCJobStatus(jobStatus.State.Phase)
	if err != nil {
		log.Errorf("convert VCJob status to JobStatus failed, err: %v", err)
		return StatusInfo{}, err
	}
	log.Infof("VCJob status: %s", state)

	return StatusInfo{
		OriginStatus: string(jobStatus.State.Phase),
		Status:       state,
		Message:      msg,
	}, nil
}

func getVCJobStatus(phase batchv1alpha1.JobPhase) (schema.JobStatus, string, error) {
	status := schema.JobStatus("")
	msg := ""
	switch phase {
	case batchv1alpha1.Pending:
		status = schema.StatusJobPending
		msg = "job is pending"
	case batchv1alpha1.Running, batchv1alpha1.Restarting, batchv1alpha1.Completing:
		status = schema.StatusJobRunning
		msg = "job is running"
	case batchv1alpha1.Terminating, batchv1alpha1.Aborting:
		status = schema.StatusJobTerminating
		msg = "job is terminating"
	case batchv1alpha1.Completed:
		status = schema.StatusJobSucceeded
		msg = "job is succeeded"
	case batchv1alpha1.Aborted:
		status = schema.StatusJobTerminated
		msg = "job is terminated"
	case batchv1alpha1.Failed, batchv1alpha1.Terminated:
		status = schema.StatusJobFailed
		msg = "job is failed"
	default:
		return status, msg, fmt.Errorf("unexpected vcjob status: %s", phase)
	}
	return status, msg, nil
}

// PaddleJobStatus get paddle job status, message from interface{}, and covert to JobStatus
func PaddleJobStatus(obj interface{}) (StatusInfo, error) {
	status, err := ConvertToStatus(obj, PaddleJobGVK)
	if err != nil {
		log.Errorf("convert PaddleJob status failed, err: %v", err)
		return StatusInfo{}, err
	}
	jobStatus := status.(*paddlejobv1.PaddleJobStatus)
	state, msg, err := getPaddleJobStatus(jobStatus.Phase)
	if err != nil {
		log.Errorf("get PaddleJob status failed, err: %v", err)
		return StatusInfo{}, err
	}
	log.Infof("Paddle job status: %s", state)
	return StatusInfo{
		OriginStatus: string(jobStatus.Phase),
		Status:       state,
		Message:      msg,
	}, nil
}

func getPaddleJobStatus(phase paddlejobv1.PaddleJobPhase) (schema.JobStatus, string, error) {
	status := schema.JobStatus("")
	msg := ""
	switch phase {
	case paddlejobv1.Starting, paddlejobv1.Pending:
		status = schema.StatusJobPending
		msg = "paddle job is pending"
	case paddlejobv1.Running, paddlejobv1.Restarting, paddlejobv1.Completing, paddlejobv1.Scaling:
		status = schema.StatusJobRunning
		msg = "paddle job is running"
	case paddlejobv1.Terminating, paddlejobv1.Aborting:
		status = schema.StatusJobTerminating
		msg = "paddle job is terminating"
	case paddlejobv1.Completed, paddlejobv1.Succeed:
		status = schema.StatusJobSucceeded
		msg = "paddle job is succeeded"
	case paddlejobv1.Aborted:
		status = schema.StatusJobTerminated
		msg = "paddle job is terminated"
	case paddlejobv1.Failed, paddlejobv1.Terminated, paddlejobv1.Unknown:
		status = schema.StatusJobFailed
		msg = "paddle job is failed"
	default:
		return status, msg, fmt.Errorf("unexpected paddlejob status: %s", phase)
	}
	return status, msg, nil
}

// PytorchJobStatus get job status, message for PyTorchJob
func PytorchJobStatus(obj interface{}) (StatusInfo, error) {
	return kubeflowJobStatus(obj, PyTorchJobGVK)
}

// TFJobStatus get job status, message for TFJob
func TFJobStatus(obj interface{}) (StatusInfo, error) {
	return kubeflowJobStatus(obj, TFJobGVK)
}

// MXNetJobStatus get job status, message for MXNetJob
func MXNetJobStatus(obj interface{}) (StatusInfo, error) {
	return kubeflowJobStatus(obj, MXNetJobGVK)
}

// MPIJobStatus get job status, message for MPIJob
func MPIJobStatus(obj interface{}) (StatusInfo, error) {
	return kubeflowJobStatus(obj, MPIJobGVK)
}

// kubeflowJobStatus get job status and message for PyTorch, TFJob, MXJob, MPIJob, and covert origin status to PF JobStatus
func kubeflowJobStatus(obj interface{}, gvk k8sschema.GroupVersionKind) (StatusInfo, error) {
	status, err := ConvertToStatus(obj, gvk)
	if err != nil {
		log.Errorf("convert kubeflow %s job status failed, err: %v", gvk.String(), err)
		return StatusInfo{}, err
	}
	jobStatus := status.(*kubeflowv1.JobStatus)
	condLen := len(jobStatus.Conditions)
	var jobCond kubeflowv1.JobCondition
	if condLen >= 1 {
		jobCond = jobStatus.Conditions[condLen-1]
	}

	state, msg, err := getKubeflowJobStatus(jobCond)
	if err != nil {
		log.Errorf("get kubeflow %s job status failed, err: %v", gvk.String(), err)
		return StatusInfo{}, err
	}
	log.Infof("kubeflow %s job status: %s", gvk.String(), state)
	return StatusInfo{
		OriginStatus: string(jobCond.Type),
		Status:       state,
		Message:      msg,
	}, nil
}

func getKubeflowJobStatus(jobStatus kubeflowv1.JobCondition) (schema.JobStatus, string, error) {
	status := schema.JobStatus("")
	msg := jobStatus.Message
	switch jobStatus.Type {
	case kubeflowv1.JobCreated:
		status = schema.StatusJobPending
	case kubeflowv1.JobRunning, kubeflowv1.JobRestarting:
		status = schema.StatusJobRunning
	case kubeflowv1.JobSucceeded:
		status = schema.StatusJobSucceeded
	case kubeflowv1.JobFailed:
		status = schema.StatusJobFailed
	default:
		return status, msg, fmt.Errorf("unexpected job status: %s", jobStatus.Type)
	}
	return status, msg, nil
}

// SingleJobStatus get single job status, message from interface{}, and covert to JobStatus
func SingleJobStatus(obj interface{}) (StatusInfo, error) {
	status, err := ConvertToStatus(obj, PodGVK)
	if err != nil {
		log.Errorf("convert SingleJob status failed, err: %v", err)
		return StatusInfo{}, err
	}
	jobStatus := status.(*v1.PodStatus)
	state, msg, err := getSingleJobStatus(jobStatus)
	if err != nil {
		log.Errorf("get SingleJob status failed, err: %v", err)
		return StatusInfo{}, err
	}
	log.Infof("Single job status: %s", state)
	return StatusInfo{
		OriginStatus: string(jobStatus.Phase),
		Status:       state,
		Message:      msg,
	}, nil
}

func getSingleJobStatus(jobStatus *v1.PodStatus) (schema.JobStatus, string, error) {
	status := schema.JobStatus("")
	msg := ""
	switch jobStatus.Phase {
	case v1.PodPending:
		status = schema.StatusJobPending
		msg = "job is pending"
	case v1.PodRunning:
		status = schema.StatusJobRunning
		msg = "job is running"
	case v1.PodSucceeded:
		status = schema.StatusJobSucceeded
		msg = "job is succeeded"
	case v1.PodFailed, v1.PodUnknown:
		status = schema.StatusJobFailed
		msg = getSingleJobMessage(jobStatus)
	default:
		return status, msg, fmt.Errorf("unexpected single job status: %s", jobStatus.Phase)
	}
	return status, msg, nil
}

func getSingleJobMessage(jobStatus *v1.PodStatus) string {
	if jobStatus.Phase != v1.PodFailed && jobStatus.Phase != v1.PodUnknown {
		return ""
	}
	errMessage := "job is failed, "
	for _, initConStatus := range jobStatus.InitContainerStatuses {
		if initConStatus.State.Terminated != nil {
			errMessage += fmt.Sprintf("init container: %s exited with code: %d, reason: %s, message: %s",
				initConStatus.Name,
				initConStatus.State.Terminated.ExitCode,
				initConStatus.State.Terminated.Reason,
				initConStatus.State.Terminated.Message)
		}
	}
	for _, conStatus := range jobStatus.ContainerStatuses {
		if conStatus.State.Terminated != nil {
			errMessage += fmt.Sprintf("container %s exited with code: %d, reason: %s, message: %s",
				conStatus.Name,
				conStatus.State.Terminated.ExitCode,
				conStatus.State.Terminated.Reason,
				conStatus.State.Terminated.Message)
		}
	}
	return errMessage
}

// ArgoWorkflowStatus get argo workflow status, message from interface{}, and covert to JobStatus
func ArgoWorkflowStatus(obj interface{}) (StatusInfo, error) {
	status, err := ConvertToStatus(obj, ArgoWorkflowGVK)
	if err != nil {
		log.Errorf("convert ArgoWorkflow status failed, err: %v", err)
		return StatusInfo{}, err
	}
	wfStatus := status.(*wfv1.WorkflowStatus)
	state, err := getArgoWorkflowStatus(wfStatus.Phase)
	if err != nil {
		log.Errorf("get ArgoWorkflow status failed, err: %v", err)
		return StatusInfo{}, err
	}
	log.Infof("ArgoWorkflow status: %s", state)
	return StatusInfo{
		OriginStatus: string(wfStatus.Phase),
		Status:       state,
		Message:      wfStatus.Message,
	}, nil
}

func getArgoWorkflowStatus(phase wfv1.NodePhase) (schema.JobStatus, error) {
	status := schema.JobStatus("")
	switch phase {
	case wfv1.NodePending, wfv1.NodeOmitted, wfv1.NodeSkipped:
		status = schema.StatusJobPending
	case wfv1.NodeRunning:
		status = schema.StatusJobRunning
	case wfv1.NodeSucceeded:
		status = schema.StatusJobSucceeded
	case wfv1.NodeFailed, wfv1.NodeError:
		status = schema.StatusJobFailed
	default:
		return status, fmt.Errorf("unexpected ArgoWorkflow status: %s", phase)
	}
	return status, nil
}

// RayJobStatus get job status, message for RayJob
func RayJobStatus(obj interface{}) (StatusInfo, error) {
	status, err := ConvertToStatus(obj, RayJobGVK)
	if err != nil {
		log.Errorf("convert ray job %s status failed, err: %v", RayJobGVK.String(), err)
		return StatusInfo{}, err
	}
	jobStatus := status.(*rayV1alpha1.RayJobStatus)

	state, msg, err := getRayJobStatus(jobStatus)
	if err != nil {
		log.Errorf("get ray job %s status failed, err: %v", RayJobGVK.String(), err)
		return StatusInfo{}, err
	}
	log.Infof("ray job %s status: %s", RayJobGVK.String(), state)
	return StatusInfo{
		OriginStatus: string(state),
		Status:       state,
		Message:      msg,
	}, nil
}

func getRayJobStatus(rayJobStatus *rayV1alpha1.RayJobStatus) (schema.JobStatus, string, error) {
	status := schema.JobStatus("")
	msg := ""
	jobStatus := rayJobStatus.JobDeploymentStatus
	switch jobStatus {
	// todo if change StatusJobPending to "Initializing"
	case rayV1alpha1.JobDeploymentStatusInitializing, rayV1alpha1.JobDeploymentStatusWaitForDashboard:
		status = schema.StatusJobPending
		msg = "job is pending"
	case rayV1alpha1.JobDeploymentStatusRunning:
		status = schema.StatusJobRunning
		msg = "job is running"
	case rayV1alpha1.JobDeploymentStatusComplete:
		status = schema.StatusJobSucceeded
		msg = "job is succeeded"
	case rayV1alpha1.JobDeploymentStatusFailedToGetOrCreateRayCluster, rayV1alpha1.JobDeploymentStatusFailedJobDeploy,
		rayV1alpha1.JobDeploymentStatusFailedToGetJobStatus:
		status = schema.StatusJobFailed
		msg = getRayJobMessage(rayJobStatus)

	default:
		return status, msg, fmt.Errorf("unexpected ray job status: %s", jobStatus)
	}
	return status, msg, nil
}

func getRayJobMessage(rayJobStatus *rayV1alpha1.RayJobStatus) string {
	return fmt.Sprintf("RayJob status in [jobDeploymentStatus/rayClusterStatus/jobStatus] is %s/%s/%s, message: %s",
		rayJobStatus.JobDeploymentStatus, rayJobStatus.RayClusterStatus.State, rayJobStatus.JobStatus, rayJobStatus.Message)
}
