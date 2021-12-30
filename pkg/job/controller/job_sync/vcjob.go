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
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	commonschema "paddleflow/pkg/common/schema"
)

func (j *JobSync) convertToVCJobObj(obj interface{}) (*batchv1alpha1.Job, error) {
	job := obj.(*unstructured.Unstructured)
	vcjob := &batchv1alpha1.Job{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(job.Object, vcjob); err != nil {
		log.Errorf("convert unstructured object[%#v] to VCJob failed. error:[%s]", job, err.Error())
		return nil, err
	}
	return vcjob, nil
}

func (j *JobSync) responsibleForVCJob(obj interface{}) bool {
	vcjob, err := j.convertToVCJobObj(obj)
	if err != nil {
		log.Errorf("responsible for vcjob skip job. jobName:[%s]", vcjob.Name)
		return false
	}
	if vcjob.Labels[commonschema.JobOwnerLabel] == commonschema.JobOwnerValue {
		log.Debugf("responsible for vcjob handle job. jobName:[%s]", vcjob.Name)
		return true
	}
	log.Debugf("responsible for vcjob skip job. jobName:[%s]", vcjob.Name)
	return false
}

func (j *JobSync) addVCJob(obj interface{}) {
	vcjob, err := j.convertToVCJobObj(obj)
	if err != nil {
		return
	}

	jobID := vcjob.Labels[commonschema.JobIDLabel]
	log.Infof("add vcjob. jobName:[%s] namespace:[%s] jobID:[%s]", vcjob.Name, vcjob.Namespace, jobID)

	jobStatus, _ := commonschema.GetVCJobStatus(vcjob.Status.State.Phase)

	if jobStatus == "" {
		jobStatus = commonschema.StatusJobPending
	}
	jobInfo := &JobSyncInfo{
		ID:      jobID,
		Status:  jobStatus,
		Runtime: obj,
		Message: vcjob.Status.State.Message,
		Type:    commonschema.TypeVcJob,
		Action:  commonschema.Update,
	}
	j.jobQueue.Add(jobInfo)
}

func (j *JobSync) updateVCJob(oldObj, newObj interface{}) {
	log.Info("update vcjob")
	oldVCJob, err := j.convertToVCJobObj(oldObj)
	if err != nil {
		return
	}
	newVCJob, err := j.convertToVCJobObj(newObj)
	if err != nil {
		return
	}

	log.Debugf("update vcjob. newJobName:[%s] namespace:[%s]", newVCJob.Name, newVCJob.Namespace)

	if oldVCJob.ResourceVersion == newVCJob.ResourceVersion &&
		oldVCJob.Status.State.Phase == newVCJob.Status.State.Phase {
		log.Debugf("skip update vcjob. jobID:[%s] resourceVersion:[%s] phase:[%s]",
			newVCJob.Name, newVCJob.ResourceVersion, newVCJob.Status.State.Phase)
		return
	}

	jobID := newVCJob.Labels[commonschema.JobIDLabel]
	jobStatus, err := commonschema.GetVCJobStatus(newVCJob.Status.State.Phase)
	if err != nil {
		log.Errorf("update vcjob sync get job status failed. jobID:[%s] error:[%s]", jobID, err.Error())
		return
	}
	jobInfo := &JobSyncInfo{
		ID:      jobID,
		Status:  jobStatus,
		Runtime: newObj,
		Message: newVCJob.Status.State.Message,
		Type:    commonschema.TypeVcJob,
		Action:  commonschema.Update,
	}
	j.jobQueue.Add(jobInfo)
	log.Infof("update vcjob enqueue. jobID:[%s] status:[%s] message:[%s]",
		jobInfo.ID, jobInfo.Status, jobInfo.Message)
}

func (j *JobSync) deleteVCJob(obj interface{}) {
	// TODO(qinduohao):consider cache.DeletedFinalStateUnknown
	log.Info("delete vcjob")
	vcjob, err := j.convertToVCJobObj(obj)
	if err != nil {
		return
	}
	jobID := vcjob.Labels[commonschema.JobIDLabel]
	jobStatus, err := commonschema.GetVCJobStatus(vcjob.Status.State.Phase)
	if err != nil {
		log.Errorf("add vcjob sync get job status failed. jobID:[%s] error:[%s]", jobID, err)
		return
	}
	jobInfo := &JobSyncInfo{
		ID:      jobID,
		Status:  jobStatus,
		Runtime: obj,
		Message: vcjob.Status.State.Message,
		Type:    commonschema.TypeVcJob,
		Action:  commonschema.Delete,
	}
	j.jobQueue.Add(jobInfo)
	log.Infof("delete vcjob enqueue. jobID:[%s]", jobInfo.ID)
}
