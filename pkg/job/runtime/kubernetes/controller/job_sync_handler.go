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
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"

	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/common/schema"
)

const (
	DefaultJobPendingTTLSeconds = 300

	PodInitializing   = "PodInitializing"
	ContainerCreating = "ContainerCreating"
)

func (j *JobSync) add(obj interface{}) {
	jobObj := obj.(*unstructured.Unstructured)
	// get job id
	jobType := k8s.GVKToJobType[jobObj.GroupVersionKind()]
	labels := jobObj.GetLabels()
	jobID := labels[schema.JobIDLabel]
	// get job status
	getStatusFunc := k8s.GVKJobStatusMap[jobObj.GroupVersionKind()]
	statusInfo, err := getStatusFunc(obj)
	if err != nil {
		return
	}
	jobStatus := statusInfo.Status
	log.Infof("add %s job. jobName: %s, namespace: %s, jobID: %s, status: %s",
		jobType, jobObj.GetName(), jobObj.GetNamespace(), jobID, jobStatus)
	if jobStatus == "" {
		jobStatus = schema.StatusJobPending
	}
	jobInfo := &JobSyncInfo{
		ID:      jobID,
		Status:  jobStatus,
		Runtime: obj,
		Message: statusInfo.Message,
		Type:    jobType,
		Action:  schema.Update,
	}
	j.jobQueue.Add(jobInfo)
}

func (j *JobSync) update(old, new interface{}) {
	oldObj := old.(*unstructured.Unstructured)
	newObj := new.(*unstructured.Unstructured)
	// get job id and job type
	jobType := k8s.GVKToJobType[newObj.GroupVersionKind()]
	labels := newObj.GetLabels()
	jobID := labels[schema.JobIDLabel]
	log.Infof("update %s job, jobName: %s, namespace: %s, jobID: %s",
		jobType, newObj.GetName(), newObj.GetNamespace(), jobID)

	// get job status
	getStatusFunc := k8s.GVKJobStatusMap[newObj.GroupVersionKind()]
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
		log.Debugf("skip update spark job. jobID: %s, resourceVersion: %s, state: %s",
			newObj.GetName(), newObj.GetResourceVersion(), newStatusInfo.Status)
		return
	}
	// construct job sync info
	jobStatus := newStatusInfo.Status
	if jobStatus == "" {
		jobStatus = schema.StatusJobPending
	}
	jobInfo := &JobSyncInfo{
		ID:      jobID,
		Status:  jobStatus,
		Runtime: new,
		Message: newStatusInfo.Message,
		Type:    jobType,
		Action:  schema.Update,
	}
	j.jobQueue.Add(jobInfo)
	log.Infof("update %s job enqueue. jobID: %s, status: %s, message: %s", jobInfo.Type,
		jobInfo.ID, jobInfo.Status, jobInfo.Message)
}

func (j *JobSync) delete(obj interface{}) {
	jobObj := obj.(*unstructured.Unstructured)
	// get job id and job Type
	jobType := k8s.GVKToJobType[jobObj.GroupVersionKind()]
	labels := jobObj.GetLabels()
	jobID := labels[schema.JobIDLabel]
	log.Infof("delete %s job. jobName: %s, namespace: %s, jobID: %s", jobType, jobObj.GetName(), jobObj.GetNamespace(), jobID)
	// get job status
	getStatusFunc := k8s.GVKJobStatusMap[jobObj.GroupVersionKind()]
	statusInfo, err := getStatusFunc(obj)
	if err != nil {
		log.Errorf("get job status failed, and jobID: %s, error: %s", jobID, err)
		return
	}
	jobInfo := &JobSyncInfo{
		ID:      jobID,
		Status:  statusInfo.Status,
		Runtime: obj,
		Message: statusInfo.Message,
		Type:    jobType,
		Action:  schema.Delete,
	}
	j.jobQueue.Add(jobInfo)
	log.Infof("delete %s job enqueue, jobID: %s", jobInfo.Type, jobInfo.ID)
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
	oldPodObj := oldObj.(*unstructured.Unstructured)
	newPodObj := newObj.(*unstructured.Unstructured)
	oldPodLabels := oldPodObj.GetLabels()
	_, ok := oldPodLabels[schema.VolcanoJobNameLabel]
	if !ok {
		return
	}
	newPodLabels := newPodObj.GetLabels()
	jobName, ok := newPodLabels[schema.VolcanoJobNameLabel]
	if !ok {
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
	if config.GlobalServerConfig.Job.Reclaim.JobPendingTTLSeconds > 0 {
		terminateDuration = config.GlobalServerConfig.Job.Reclaim.JobPendingTTLSeconds
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
	status, err := k8s.ConvertToStatus(obj, k8s.PodGVK)
	if err != nil {
		log.Errorf("get status from pod %s/%s failed, err: %v", namespace, name, err)
		return
	}
	// TODO: get role name from pod

	// convert to task status
	podStatus := status.(*v1.PodStatus)
	taskStatus, err := k8s.GetTaskStatus(podStatus)
	if err != nil {
		log.Errorf("convert to task status for pod %s/%s failed, err: %v", namespace, name, err)
		return
	}
	message := k8s.GetTaskMessage(podStatus)

	taskInfo := &TaskSyncInfo{
		ID:        string(uid),
		Name:      name,
		Namespace: namespace,
		JobID:     jobName,
		Status:    taskStatus,
		Message:   message,
		PodStatus: status,
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
	// get job name for single job
	if labels != nil && labels[schema.JobOwnerLabel] == schema.JobOwnerValue {
		return name
	}
	// get job name for distributed job
	jobName := ""
	ownerReferences := obj.GetOwnerReferences()
	if len(ownerReferences) == 0 {
		log.Warnf("pod %s/%s does not has owner references, skip it.", namespace, name)
		return jobName
	}
	ownerReference := ownerReferences[0]
	gvk := k8sschema.FromAPIVersionAndKind(ownerReference.APIVersion, ownerReference.Kind)
	_, find := k8s.GVKToJobType[gvk]
	if !find {
		return jobName
	}
	return jobName
}
