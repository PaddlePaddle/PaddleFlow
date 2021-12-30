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
	sparkoperatorv1beta2 "paddleflow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"

	commonschema "paddleflow/pkg/common/schema"
)

func (j *JobSync) convertToSparkJobObj(obj interface{}) (*sparkoperatorv1beta2.SparkApplication, error) {
	job := obj.(*unstructured.Unstructured)
	sparkjob := &sparkoperatorv1beta2.SparkApplication{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(job.Object, sparkjob); err != nil {
		log.Errorf("convert unstructured object[%#v] to VCJob failed. error:[%s]", job, err.Error())
		return nil, err
	}
	return sparkjob, nil
}

func (j *JobSync) responsibleForSparkJob(obj interface{}) bool {
	sparkjob, err := j.convertToSparkJobObj(obj)
	if err != nil {
		log.Errorf("responsible for sparkjob skip job. jobName:[%s]", sparkjob.Name)
		return false
	}
	if sparkjob.Labels[commonschema.JobOwnerLabel] == commonschema.JobOwnerValue {
		log.Debugf("responsible for sparkjob handle job. jobName:[%s]", sparkjob.Name)
		return true
	}
	log.Debugf("responsible for sparkjob skip job. jobName:[%s]", sparkjob.Name)
	return false
}

func (j *JobSync) addSparkJob(obj interface{}) {
	sparkjob, err := j.convertToSparkJobObj(obj)
	if err != nil {
		return
	}

	jobID := sparkjob.Labels[commonschema.JobIDLabel]
	log.Infof("add spark job. jobName:[%s] namespace:[%s] jobID:[%s]", sparkjob.Name, sparkjob.Namespace, jobID)

	jobStatus, _ := commonschema.GetSparkJobStatus(sparkjob.Status.AppState.State)

	if jobStatus == "" {
		jobStatus = commonschema.StatusJobPending
	}
	jobInfo := &JobSyncInfo{
		ID:      jobID,
		Status:  jobStatus,
		Runtime: obj,
		Message: sparkjob.Status.AppState.ErrorMessage,
		Type:    commonschema.TypeVcJob,
		Action:  commonschema.Update,
	}
	j.jobQueue.Add(jobInfo)
}

func (j *JobSync) updateSparkJob(oldObj, newObj interface{}) {
	log.Info("update spark job")
	oldSparkJob, err := j.convertToSparkJobObj(oldObj)
	if err != nil {
		return
	}
	newSparkJob, err := j.convertToSparkJobObj(newObj)
	if err != nil {
		return
	}

	log.Debugf("update spark job. newJobName:[%s] namespace:[%s]", newSparkJob.Name, newSparkJob.Namespace)

	if oldSparkJob.ResourceVersion == newSparkJob.ResourceVersion &&
		oldSparkJob.Status.AppState.State == newSparkJob.Status.AppState.State {
		log.Debugf("skip update spark job. jobID:[%s] resourceVersion:[%s] state:[%s]",
			newSparkJob.Name, newSparkJob.ResourceVersion, newSparkJob.Status.AppState.State)
		return
	}

	jobID := newSparkJob.Labels[commonschema.JobIDLabel]
	jobStatus, err := commonschema.GetSparkJobStatus(newSparkJob.Status.AppState.State)
	if err != nil {
		log.Errorf("update sparkjob get job status failed. jobID:[%s] error:[%s]", jobID, err.Error())
		return
	}
	jobInfo := &JobSyncInfo{
		ID:      jobID,
		Status:  jobStatus,
		Runtime: newObj,
		Message: newSparkJob.Status.AppState.ErrorMessage,
		Type:    commonschema.TypeVcJob,
		Action:  commonschema.Update,
	}
	j.jobQueue.Add(jobInfo)
	log.Infof("update spark job enqueue. jobID:[%s] status:[%s] message:[%s]",
		jobInfo.ID, jobInfo.Status, jobInfo.Message)
}

func (j *JobSync) deleteSparkJob(obj interface{}) {
	// TODO(qinduohao):consider cache.DeletedFinalStateUnknown
	log.Info("delete spark job")
	sparkjob, err := j.convertToSparkJobObj(obj)
	if err != nil {
		return
	}
	log.Debugf("delete spark job. jobName:[%s] namespace:[%s]", sparkjob.Name, sparkjob.Namespace)

	jobID := sparkjob.Labels[commonschema.JobIDLabel]
	jobStatus, err := commonschema.GetSparkJobStatus(sparkjob.Status.AppState.State)
	if err != nil {
		log.Errorf("add vcjob sync get job status failed. jobID:[%s] error:[%s]", jobID, err)
		return
	}
	jobInfo := &JobSyncInfo{
		ID:      jobID,
		Status:  jobStatus,
		Runtime: obj,
		Message: sparkjob.Status.AppState.ErrorMessage,
		Type:    commonschema.TypeVcJob,
		Action:  commonschema.Delete,
	}
	j.jobQueue.Add(jobInfo)
	log.Infof("delete spark job enqueue. jobID:[%s]", jobInfo.ID)
}
