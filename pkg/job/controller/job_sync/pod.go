/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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

package job_sync

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"paddleflow/pkg/common/config"
	commonschema "paddleflow/pkg/common/schema"
)

const (
	DefaultJobPendingTTLSeconds = 300

	PodInitializing   = "PodInitializing"
	ContainerCreating = "ContainerCreating"
)

func (j *JobSync) convertToPodObj(obj interface{}) (*v1.Pod, error) {
	unstructuredPod := obj.(*unstructured.Unstructured)
	pod := &v1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPod.Object, pod); err != nil {
		log.Errorf("convert unstructured object[%#v] to pod failed. error:[%s]", pod, err.Error())
		return nil, err
	}
	return pod, nil
}

func (j *JobSync) isValidWaitingState(s *v1.ContainerStateWaiting) bool {
	if s != nil && (s.Reason == PodInitializing || s.Reason == ContainerCreating) {
		return true
	}
	return false
}

func (j *JobSync) updatePod(oldObj, newObj interface{}) {
	oldPod, err := j.convertToPodObj(oldObj)
	if err != nil {
		return
	}
	_, ok := oldPod.Labels[commonschema.VolcanoJobNameLabel]
	if !ok {
		return
	}

	newPod, err := j.convertToPodObj(newObj)
	if err != nil {
		return
	}
	jobName, ok := newPod.Labels[commonschema.VolcanoJobNameLabel]
	if !ok {
		return
	}
	if newPod.Status.Phase != v1.PodPending {
		return
	}

	message := ""
	isValidWaitingState := false
	for _, containerStatus := range newPod.Status.InitContainerStatuses {
		isValidWaitingState = j.isValidWaitingState(containerStatus.State.Waiting)
		if containerStatus.State.Waiting != nil {
			message = fmt.Sprintf("%s:%s", containerStatus.State.Waiting.Reason, containerStatus.State.Waiting.Message)
			break
		}
	}
	for _, containerStatus := range newPod.Status.ContainerStatuses {
		isValidWaitingState = j.isValidWaitingState(containerStatus.State.Waiting)
		if containerStatus.State.Waiting != nil {
			message = fmt.Sprintf("%s:%s", containerStatus.State.Waiting.Reason, containerStatus.State.Waiting.Message)
			break
		}
	}
	if message == "" {
		return
	}
	log.Infof("update pod. newPodName:[%s] namespace:[%s] jobName:[%s] message:[%s]",
		newPod.Name, newPod.Namespace, jobName, message)
	jobInfo := &JobSyncInfo{
		ID:      jobName,
		Message: message,
		Action:  commonschema.Update,
	}
	j.jobQueue.Add(jobInfo)

	if isValidWaitingState {
		return
	}
	terminateJobInfo := &JobSyncInfo{
		ID:     jobName,
		Action: commonschema.Terminate,
	}
	terminateDuration := DefaultJobPendingTTLSeconds
	if config.GlobalServerConfig.Job.Reclaim.JobPendingTTLSeconds > 0 {
		terminateDuration = config.GlobalServerConfig.Job.Reclaim.JobPendingTTLSeconds
	}
	log.Infof("terminate job. namespace:[%s] jobName:[%s]", newPod.Namespace, jobName)
	j.jobQueue.AddAfter(terminateJobInfo, time.Duration(terminateDuration)*time.Second)
}
