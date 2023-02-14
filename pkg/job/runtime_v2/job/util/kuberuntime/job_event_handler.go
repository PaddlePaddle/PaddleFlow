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
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
)

const (
	DefaultJobPendingTTLSeconds = 300

	RuntimeStatusKey = "status"

	PodInitializing   = "PodInitializing"
	ContainerCreating = "ContainerCreating"
)

func GetParentJobID(obj *unstructured.Unstructured) string {
	name := obj.GetName()
	namespace := obj.GetNamespace()
	ownerReferences := obj.GetOwnerReferences()
	if len(ownerReferences) == 0 {
		return ""
	}
	owner := ownerReferences[0]
	gvk := k8sschema.FromAPIVersionAndKind(owner.APIVersion, owner.Kind)
	if gvk != k8s.ArgoWorkflowGVK {
		log.Warnf("job %s/%s is not belong to ArgoWorkflow, skip it", namespace, name)
		return ""
	}
	// parent job
	return owner.Name
}

func JobAddFunc(obj interface{}, getStatusFunc api.GetStatusFunc) (*api.JobSyncInfo, error) {
	jobObj := obj.(*unstructured.Unstructured)
	gvk := jobObj.GroupVersionKind()

	log.Infof("begin add %s job. jobName: %s, namespace: %s", gvk.String(), jobObj.GetName(), jobObj.GetNamespace())
	// get job status
	statusInfo, err := getStatusFunc(obj)
	if err != nil {
		return nil, err
	}
	jobStatus := statusInfo.Status

	if jobStatus == "" {
		jobStatus = schema.StatusJobPending
	}
	parentJobID := GetParentJobID(jobObj)
	// get runtime status and info
	runtimeStatus := jobObj.Object[RuntimeStatusKey]
	runtimeInfo := jobObj.DeepCopy().Object
	delete(runtimeInfo, RuntimeStatusKey)
	// get framework version
	frameworkVersion := schema.NewFrameworkVersion(gvk.Kind, gvk.GroupVersion().String())
	jobInfo := &api.JobSyncInfo{
		ID:               jobObj.GetName(),
		Namespace:        jobObj.GetNamespace(),
		Annotations:      jobObj.GetAnnotations(),
		ParentJobID:      parentJobID,
		FrameworkVersion: frameworkVersion,
		Status:           jobStatus,
		RuntimeInfo:      runtimeInfo,
		RuntimeStatus:    runtimeStatus,
		Message:          statusInfo.Message,
		Action:           schema.Create,
	}
	log.Infof("add %s job enqueue. jobID: %s, status: %s, message: %s", gvk.String(),
		jobInfo.ID, jobInfo.Status, jobInfo.Message)
	return jobInfo, nil
}

func JobUpdateFunc(old, new interface{}, getStatusFunc api.GetStatusFunc) (*api.JobSyncInfo, error) {
	oldObj := old.(*unstructured.Unstructured)
	newObj := new.(*unstructured.Unstructured)
	// get job id
	gvk := newObj.GroupVersionKind()
	labels := newObj.GetLabels()
	jobID := labels[schema.JobIDLabel]
	log.Infof("update %s job, jobName: %s, namespace: %s, jobID: %s",
		gvk.String(), newObj.GetName(), newObj.GetNamespace(), jobID)

	// get job status
	oldStatusInfo, err := getStatusFunc(old)
	if err != nil {
		return nil, err
	}
	newStatusInfo, err := getStatusFunc(new)
	if err != nil {
		return nil, err
	}
	if oldObj.GetResourceVersion() == newObj.GetResourceVersion() &&
		oldStatusInfo.OriginStatus == newStatusInfo.OriginStatus {
		err = fmt.Errorf("skip update %s job. jobID: %s, resourceVersion: %s, state: %s", gvk.String(),
			newObj.GetName(), newObj.GetResourceVersion(), newStatusInfo.Status)
		log.Debugf("%s", err)
		return nil, err
	}
	// construct job sync info
	jobStatus := newStatusInfo.Status
	if jobStatus == "" {
		jobStatus = schema.StatusJobPending
	}
	// get framework version
	frameworkVersion := schema.NewFrameworkVersion(gvk.Kind, gvk.GroupVersion().String())
	jobInfo := &api.JobSyncInfo{
		ID:               newObj.GetName(),
		Namespace:        newObj.GetNamespace(),
		Annotations:      newObj.GetAnnotations(),
		ParentJobID:      GetParentJobID(newObj),
		FrameworkVersion: frameworkVersion,
		Status:           jobStatus,
		RuntimeStatus:    newObj.Object[RuntimeStatusKey],
		Message:          newStatusInfo.Message,
		Action:           schema.Update,
	}
	log.Infof("update %s job enqueue. jobID: %s, status: %s, message: %s", gvk.String(),
		jobInfo.ID, jobInfo.Status, jobInfo.Message)
	return jobInfo, nil
}

func JobDeleteFunc(obj interface{}, getStatusFunc api.GetStatusFunc) (*api.JobSyncInfo, error) {
	jobObj := obj.(*unstructured.Unstructured)
	// get job id and GroupVersionKind
	gvk := jobObj.GroupVersionKind()
	labels := jobObj.GetLabels()
	jobID := labels[schema.JobIDLabel]
	log.Infof("delete %s job. jobName: %s, namespace: %s, jobID: %s", gvk.String(), jobObj.GetName(), jobObj.GetNamespace(), jobID)
	// get job status
	statusInfo, err := getStatusFunc(obj)
	if err != nil {
		log.Errorf("get job status failed, and jobID: %s, error: %s", jobID, err)
		return nil, err
	}
	// get framework version
	frameworkVersion := schema.NewFrameworkVersion(gvk.Kind, gvk.GroupVersion().String())
	jobInfo := &api.JobSyncInfo{
		ID:               jobObj.GetName(),
		Namespace:        jobObj.GetNamespace(),
		Annotations:      jobObj.GetAnnotations(),
		ParentJobID:      GetParentJobID(jobObj),
		FrameworkVersion: frameworkVersion,
		Status:           statusInfo.Status,
		RuntimeStatus:    jobObj.Object[RuntimeStatusKey],
		Message:          statusInfo.Message,
		Action:           schema.Delete,
	}
	log.Infof("delete %s job enqueue, jobID: %s", gvk.String(), jobInfo.ID)
	return jobInfo, nil
}

func TaskUpdate(oldObj, newObj interface{}, taskQueue, jobQueue workqueue.RateLimitingInterface) {
	oldPodObj := oldObj.(*unstructured.Unstructured)
	newPodObj := newObj.(*unstructured.Unstructured)
	jobName := getJobByTask(newPodObj)
	if len(jobName) == 0 {
		log.Debugf("pod %s/%s not belong to paddlefow job, skip it.", newPodObj.GetNamespace(), newPodObj.GetName())
		return
	}

	oldPod := &v1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(oldPodObj.Object, oldPod); err != nil {
		log.Errorf("convert unstructured object [%+v] to pod failed. error: %s", oldPodObj.Object, err.Error())
		return
	}
	oldPodStatus := &oldPod.Status

	newPod := &v1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(newPodObj.Object, newPod); err != nil {
		log.Errorf("convert unstructured object [%+v] to pod failed. error: %s", newPodObj.Object, err.Error())
		return
	}
	newPodStatus := &newPod.Status

	if oldPodStatus.Phase != newPodStatus.Phase {
		// update pod status when pod phase is changed
		TaskUpdateFunc(newObj, schema.Update, taskQueue)
	} else {
		oldFingerprint := podStatusFingerprint(oldPodStatus)
		newFingerprint := podStatusFingerprint(newPodStatus)
		log.Infof("status fingerprint for pod %s/%s, old [%s], new: [%s]", newPodObj.GetNamespace(),
			newPodObj.GetName(), oldFingerprint, newFingerprint)
		if oldFingerprint != newFingerprint {
			TaskUpdateFunc(newObj, schema.Update, taskQueue)
		}
		if newPodStatus.Phase == v1.PodPending {
			handlePendingPod(newPodStatus, jobName, newPodObj.GetName(), newPodObj.GetNamespace(), jobQueue)
		}
	}
}

func podStatusFingerprint(podStatus *v1.PodStatus) string {
	if podStatus == nil {
		return ""
	}
	fingerprint := string(podStatus.Phase)
	for _, containerStatus := range podStatus.InitContainerStatuses {
		if containerStatus.State.Waiting != nil {
			fingerprint += fmt.Sprintf(";%s:%s:%s", containerStatus.ContainerID, containerStatus.Name, containerStatus.State.Waiting.Reason)
		}
	}
	for _, containerStatus := range podStatus.ContainerStatuses {
		if containerStatus.State.Waiting != nil {
			fingerprint += fmt.Sprintf(";%s:%s:%s", containerStatus.ContainerID, containerStatus.Name, containerStatus.State.Waiting.Reason)
		}
	}
	return fingerprint
}

func isValidWaitingState(s *v1.ContainerStateWaiting) bool {
	if s != nil && (s.Reason == PodInitializing || s.Reason == ContainerCreating) {
		return true
	}
	return false
}

func handlePendingPod(podStatus *v1.PodStatus, jobName, podName, namespace string, jobQueue workqueue.RateLimitingInterface) {
	message := ""
	isValidWaiting := false
	for _, containerStatus := range podStatus.InitContainerStatuses {
		isValidWaiting = isValidWaitingState(containerStatus.State.Waiting)
		if containerStatus.State.Waiting != nil {
			message = fmt.Sprintf("%s:%s", containerStatus.State.Waiting.Reason, containerStatus.State.Waiting.Message)
			break
		}
	}
	for _, containerStatus := range podStatus.ContainerStatuses {
		isValidWaiting = isValidWaitingState(containerStatus.State.Waiting)
		if containerStatus.State.Waiting != nil {
			message = fmt.Sprintf("%s:%s", containerStatus.State.Waiting.Reason, containerStatus.State.Waiting.Message)
			break
		}
	}
	if message == "" {
		return
	}
	log.Infof("update pod. newPodName: %s, namespace: %s, jobName: %s, message: %s",
		podName, namespace, jobName, message)
	jobInfo := &api.JobSyncInfo{
		ID:      jobName,
		Message: message,
		Action:  schema.Update,
	}
	jobQueue.Add(jobInfo)

	if isValidWaiting {
		return
	}
	terminateJobInfo := &api.JobSyncInfo{
		ID:     jobName,
		Action: schema.Terminate,
	}
	terminateDuration := DefaultJobPendingTTLSeconds
	if config.GlobalServerConfig.Job.Reclaim.PendingJobTTLSeconds > 0 {
		terminateDuration = config.GlobalServerConfig.Job.Reclaim.PendingJobTTLSeconds
	}
	log.Infof("terminate job. namespace: %s, jobName: %s", namespace, jobName)
	jobQueue.AddAfter(terminateJobInfo, time.Duration(terminateDuration)*time.Second)
}

func TaskUpdateFunc(obj interface{}, action schema.ActionType, taskQueue workqueue.RateLimitingInterface) {
	podObj := obj.(*unstructured.Unstructured)
	uid := podObj.GetUID()
	name := podObj.GetName()
	namespace := podObj.GetNamespace()

	jobName := getJobByTask(podObj)
	if len(jobName) == 0 {
		log.Debugf("pod %s/%s not belong to paddlefow job, skip it.", namespace, name)
		return
	}
	log.Debugf("pod %s/%s belongs to job %s", namespace, name, jobName)
	pod := &v1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(podObj.Object, pod); err != nil {
		log.Errorf("convert unstructured object [%+v] to pod failed. error: %s", podObj.Object, err.Error())
		return
	}
	// TODO: get role name from pod

	// convert to task status
	taskStatus, err := GetTaskStatus(&pod.Status)
	if err != nil {
		log.Errorf("convert to task status for pod %s/%s failed, err: %v", namespace, name, err)
		return
	}
	message := GetTaskMessage(&pod.Status)

	taskInfo := &api.TaskSyncInfo{
		ID:        string(uid),
		Name:      name,
		Namespace: namespace,
		JobID:     jobName,
		NodeName:  pod.Spec.NodeName,
		Status:    taskStatus,
		Message:   message,
		PodStatus: pod.Status,
		Action:    action,
	}
	taskQueue.Add(taskInfo)
	log.Infof("%s event for task %s/%s enqueue, job: %s", action, namespace, name, jobName)
	log.Debugf("task status: %s", taskInfo.Status)
}

func getJobByTask(obj *unstructured.Unstructured) string {
	if obj == nil {
		log.Errorf("get job by task failed, obj is nil")
		return ""
	}
	labels := obj.GetLabels()
	ownerReferences := obj.GetOwnerReferences()

	if labels == nil || labels[schema.JobOwnerLabel] != schema.JobOwnerValue {
		return ""
	}
	// 1. get job name from ownerReferences, including workflow, PaddleJob
	if len(ownerReferences) > 0 {
		// get job name for distributed job
		ownerReference := ownerReferences[0]
		gvk := k8sschema.FromAPIVersionAndKind(ownerReference.APIVersion, ownerReference.Kind)
		_, find := k8s.GVKJobStatusMap[gvk]
		if find {
			return ownerReference.Name
		}
	}
	// 2. get job name from pod labels
	jobName, find := labels[schema.JobIDLabel]
	if find {
		return jobName
	}
	return ""
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

type PodStatusMessage struct {
	Phase             v1.PodPhase              `json:"phase,omitempty"`
	Message           string                   `json:"message,omitempty"`
	Reason            string                   `json:"reason,omitempty"`
	ContainerMessages []ContainerStatusMessage `json:"containerMessages,omitempty"`
}

func (ps *PodStatusMessage) String() string {
	msg := fmt.Sprintf("pod phase is %s", ps.Phase)
	if len(ps.Reason) != 0 {
		msg += fmt.Sprintf(" with reason %s", ps.Reason)
	}
	if len(ps.Message) != 0 {
		msg += fmt.Sprintf(", detail message: %s", ps.Message)
	}
	// Container status message
	if len(ps.ContainerMessages) != 0 {
		msg += ". Containers status:"
	}
	for _, cs := range ps.ContainerMessages {
		msg += fmt.Sprintf(" %s;", cs.String())
	}
	return msg
}

type ContainerStatusMessage struct {
	Name            string                       `json:"name,omitempty"`
	ContainerID     string                       `json:"containerID,omitempty"`
	RestartCount    int32                        `json:"restartCount,omitempty"`
	WaitingState    *v1.ContainerStateWaiting    `json:"waitingState,omitempty"`
	TerminatedState *v1.ContainerStateTerminated `json:"terminatedState,omitempty"`
}

func (cs *ContainerStatusMessage) String() string {
	msg := fmt.Sprintf("container %s with restart count %d", cs.Name, cs.RestartCount)
	if len(cs.ContainerID) != 0 {
		msg += fmt.Sprintf(", id is %s", cs.ContainerID)
	}
	if cs.WaitingState != nil {
		msg += fmt.Sprintf(", wating with reason %s", cs.WaitingState.Reason)
		if len(cs.WaitingState.Message) != 0 {
			msg += fmt.Sprintf(", message: %s", cs.WaitingState.Message)
		}
	}
	if cs.TerminatedState != nil {
		msg += fmt.Sprintf(", terminated with exitCode %d, reason is %s", cs.TerminatedState.ExitCode, cs.TerminatedState.Reason)
		if len(cs.TerminatedState.Message) != 0 {
			msg += fmt.Sprintf(", message: %s", cs.TerminatedState.Message)
		}
	}
	return msg
}

// GetTaskMessage construct message from pod status
func GetTaskMessage(podStatus *v1.PodStatus) string {
	if podStatus == nil {
		return ""
	}
	statusMessage := PodStatusMessage{
		Phase:             podStatus.Phase,
		Reason:            podStatus.Reason,
		Message:           podStatus.Message,
		ContainerMessages: []ContainerStatusMessage{},
	}
	for _, initCS := range podStatus.InitContainerStatuses {
		statusMessage.ContainerMessages = append(statusMessage.ContainerMessages, ContainerStatusMessage{
			Name:            initCS.Name,
			ContainerID:     initCS.ContainerID,
			RestartCount:    initCS.RestartCount,
			WaitingState:    initCS.State.Waiting,
			TerminatedState: initCS.State.Terminated,
		})
	}
	for _, cs := range podStatus.ContainerStatuses {
		statusMessage.ContainerMessages = append(statusMessage.ContainerMessages, ContainerStatusMessage{
			Name:            cs.Name,
			ContainerID:     cs.ContainerID,
			RestartCount:    cs.RestartCount,
			WaitingState:    cs.State.Waiting,
			TerminatedState: cs.State.Terminated,
		})
	}
	return statusMessage.String()
}
