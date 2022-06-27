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
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const (
	DefaultJobPendingTTLSeconds = 300

	PodInitializing   = "PodInitializing"
	ContainerCreating = "ContainerCreating"
)

func (j *JobSync) add(obj interface{}) {
	jobObj := obj.(*unstructured.Unstructured)
	// get job id
	labels := jobObj.GetLabels()
	jobID := labels[schema.JobIDLabel]
	// get job status
	gvk := jobObj.GroupVersionKind()
	getStatusFunc := k8s.GVKJobStatusMap[gvk]
	statusInfo, err := getStatusFunc(obj)
	if err != nil {
		return
	}
	jobStatus := statusInfo.Status
	log.Infof("add %s job. jobName: %s, namespace: %s, jobID: %s, status: %s",
		gvk.String(), jobObj.GetName(), jobObj.GetNamespace(), jobID, jobStatus)
	if jobStatus == "" {
		jobStatus = schema.StatusJobPending
	}
	parentJobID := j.getParentJobID(jobObj)
	jobInfo := &JobSyncInfo{
		ID:          jobObj.GetName(),
		Namespace:   jobObj.GetNamespace(),
		ParentJobID: parentJobID,
		GVK:         jobObj.GroupVersionKind(),
		Status:      jobStatus,
		Runtime:     obj,
		Message:     statusInfo.Message,
		Action:      schema.Create,
	}
	j.jobQueue.Add(jobInfo)
}

func (j *JobSync) update(old, new interface{}) {
	oldObj := old.(*unstructured.Unstructured)
	newObj := new.(*unstructured.Unstructured)
	// get job id
	gvk := newObj.GroupVersionKind()
	labels := newObj.GetLabels()
	jobID := labels[schema.JobIDLabel]
	log.Infof("update %s job, jobName: %s, namespace: %s, jobID: %s",
		gvk.String(), newObj.GetName(), newObj.GetNamespace(), jobID)

	// get job status
	getStatusFunc := k8s.GVKJobStatusMap[gvk]
	oldStatusInfo, err := getStatusFunc(old)
	if err != nil {
		return
	}
	newStatusInfo, err := getStatusFunc(new)
	if err != nil {
		return
	}
	if oldObj.GetResourceVersion() == newObj.GetResourceVersion() &&
		oldStatusInfo.OriginStatus == newStatusInfo.OriginStatus {
		log.Debugf("skip update %s job. jobID: %s, resourceVersion: %s, state: %s", gvk.String(),
			newObj.GetName(), newObj.GetResourceVersion(), newStatusInfo.Status)
		return
	}
	// construct job sync info
	jobStatus := newStatusInfo.Status
	if jobStatus == "" {
		jobStatus = schema.StatusJobPending
	}
	jobInfo := &JobSyncInfo{
		ID:          newObj.GetName(),
		Namespace:   newObj.GetNamespace(),
		ParentJobID: j.getParentJobID(newObj),
		GVK:         newObj.GroupVersionKind(),
		Status:      jobStatus,
		Runtime:     new,
		Message:     newStatusInfo.Message,
		Action:      schema.Update,
	}
	j.jobQueue.Add(jobInfo)
	log.Infof("update %s job enqueue. jobID: %s, status: %s, message: %s", gvk.String(),
		jobInfo.ID, jobInfo.Status, jobInfo.Message)
}

func (j *JobSync) delete(obj interface{}) {
	jobObj := obj.(*unstructured.Unstructured)
	// get job id and GroupVersionKind
	gvk := jobObj.GroupVersionKind()
	labels := jobObj.GetLabels()
	jobID := labels[schema.JobIDLabel]
	log.Infof("delete %s job. jobName: %s, namespace: %s, jobID: %s", gvk.String(), jobObj.GetName(), jobObj.GetNamespace(), jobID)
	// get job status
	getStatusFunc := k8s.GVKJobStatusMap[gvk]
	statusInfo, err := getStatusFunc(obj)
	if err != nil {
		log.Errorf("get job status failed, and jobID: %s, error: %s", jobID, err)
		return
	}
	jobInfo := &JobSyncInfo{
		ID:          jobObj.GetName(),
		Namespace:   jobObj.GetNamespace(),
		ParentJobID: j.getParentJobID(jobObj),
		GVK:         jobObj.GroupVersionKind(),
		Status:      statusInfo.Status,
		Runtime:     obj,
		Message:     statusInfo.Message,
		Action:      schema.Delete,
	}
	j.jobQueue.Add(jobInfo)
	log.Infof("delete %s job enqueue, jobID: %s", gvk.String(), jobInfo.ID)
}

// subJob handle node for ArgoWorkflow
func (j *JobSync) getParentJobID(obj *unstructured.Unstructured) string {
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

// addPod watch add pod event
func (j *JobSync) addPod(obj interface{}) {
	j.updatePodStatus(obj, schema.Create)
}

// deletePod watch delete pod event
func (j *JobSync) deletePod(obj interface{}) {
	j.updatePodStatus(obj, schema.Delete)
}

// updatePod watch update pod event
func (j *JobSync) updatePod(oldObj, newObj interface{}) {
	newPodObj := newObj.(*unstructured.Unstructured)
	jobName := getJobByTask(newPodObj)
	if len(jobName) == 0 {
		log.Debugf("pod %s/%s not belong to paddlefow job, skip it.", newPodObj.GetNamespace(), newPodObj.GetName())
		return
	}

	oldStatus, err := k8s.ConvertToStatus(oldObj, k8s.PodGVK)
	if err != nil {
		return
	}
	oldPodStatus := oldStatus.(*v1.PodStatus)

	newStatus, err := k8s.ConvertToStatus(newObj, k8s.PodGVK)
	if err != nil {
		return
	}
	newPodStatus := newStatus.(*v1.PodStatus)

	if oldPodStatus.Phase != newPodStatus.Phase {
		// update pod status when pod phase is changed
		j.updatePodStatus(newObj, schema.Update)
	} else {
		oldFingerprint := podStatusFingerprint(oldPodStatus)
		newFingerprint := podStatusFingerprint(newPodStatus)
		log.Infof("status fingerprint for pod %s/%s, old [%s], new: [%s]", newPodObj.GetNamespace(),
			newPodObj.GetName(), oldFingerprint, newFingerprint)
		if oldFingerprint != newFingerprint {
			j.updatePodStatus(newObj, schema.Update)
		}
		if newPodStatus.Phase == v1.PodPending {
			j.handlePendingPod(newPodStatus, jobName, newPodObj)
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

func (j *JobSync) isValidWaitingState(s *v1.ContainerStateWaiting) bool {
	if s != nil && (s.Reason == PodInitializing || s.Reason == ContainerCreating) {
		return true
	}
	return false
}

func (j *JobSync) handlePendingPod(podStatus *v1.PodStatus, jobName string, newPodObj *unstructured.Unstructured) {
	message := ""
	isValidWaitingState := false
	for _, containerStatus := range podStatus.InitContainerStatuses {
		isValidWaitingState = j.isValidWaitingState(containerStatus.State.Waiting)
		if containerStatus.State.Waiting != nil {
			message = fmt.Sprintf("%s:%s", containerStatus.State.Waiting.Reason, containerStatus.State.Waiting.Message)
			break
		}
	}
	for _, containerStatus := range podStatus.ContainerStatuses {
		isValidWaitingState = j.isValidWaitingState(containerStatus.State.Waiting)
		if containerStatus.State.Waiting != nil {
			message = fmt.Sprintf("%s:%s", containerStatus.State.Waiting.Reason, containerStatus.State.Waiting.Message)
			break
		}
	}
	if message == "" {
		return
	}
	log.Infof("update pod. newPodName: %s, namespace: %s, jobName: %s, message: %s",
		newPodObj.GetName(), newPodObj.GetNamespace(), jobName, message)
	jobInfo := &JobSyncInfo{
		ID:      jobName,
		Message: message,
		Action:  schema.Update,
	}
	j.jobQueue.Add(jobInfo)

	if isValidWaitingState {
		return
	}
	terminateJobInfo := &JobSyncInfo{
		ID:     jobName,
		Action: schema.Terminate,
	}
	terminateDuration := DefaultJobPendingTTLSeconds
	if config.GlobalServerConfig.Job.Reclaim.PendingJobTTLSeconds > 0 {
		terminateDuration = config.GlobalServerConfig.Job.Reclaim.PendingJobTTLSeconds
	}
	log.Infof("terminate job. namespace: %s, jobName: %s", newPodObj.GetNamespace(), jobName)
	j.jobQueue.AddAfter(terminateJobInfo, time.Duration(terminateDuration)*time.Second)
}

// updatePodStatus sync status of pod to database
func (j *JobSync) updatePodStatus(obj interface{}, action schema.ActionType) {
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
	taskStatus, err := k8s.GetTaskStatus(&pod.Status)
	if err != nil {
		log.Errorf("convert to task status for pod %s/%s failed, err: %v", namespace, name, err)
		return
	}
	message := k8s.GetTaskMessage(&pod.Status)

	taskInfo := &TaskSyncInfo{
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
	j.taskQueue.Add(taskInfo)
	log.Infof("%s event for task %s/%s enqueue, job: %s", action, namespace, name, jobName)
	log.Debugf("task status: %s", taskInfo.Status)
}

func getJobByTask(obj *unstructured.Unstructured) string {
	if obj == nil {
		log.Errorf("get job by task failed, obj is nil")
		return ""
	}
	name := obj.GetName()
	namespace := obj.GetNamespace()
	labels := obj.GetLabels()
	ownerReferences := obj.GetOwnerReferences()

	if len(ownerReferences) == 0 {
		// get job name for single job
		if labels != nil && labels[schema.JobOwnerLabel] == schema.JobOwnerValue {
			return name
		} else {
			log.Debugf("pod %s/%s not belong to paddlefow job, skip it.", namespace, name)
			return ""
		}
	}
	// get job name for distributed job
	ownerReference := ownerReferences[0]
	gvk := k8sschema.FromAPIVersionAndKind(ownerReference.APIVersion, ownerReference.Kind)
	_, find := k8s.GVKJobStatusMap[gvk]
	if !find {
		return ""
	}
	return ownerReference.Name
}
