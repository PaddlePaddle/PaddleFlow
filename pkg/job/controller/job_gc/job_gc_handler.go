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

package job_gc

import (
	"reflect"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	sparkoperatorv1beta2 "paddleflow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/k8s"
	commonschema "paddleflow/pkg/common/schema"
	"paddleflow/pkg/job/controller/framework"
)

func (j *JobGarbageCollector) preCleanVCJob() error {
	vcjobs, err := j.vcJobLister.List(labels.NewSelector())
	if err != nil {
		log.Errorf("list VC job with dynamic client failed: [%+v].", err)
		return err
	}
	for _, job := range vcjobs {
		vcjob := job.(*unstructured.Unstructured)
		j.updateVCJob(nil, vcjob)
	}
	return nil
}

func (j *JobGarbageCollector) preCleanSparkApp() error {
	sparkApps, err := j.sparkApplicationLister.List(labels.NewSelector())
	if err != nil {
		log.Errorf("list spark application with dynamic client failed: [%+v].", err)
		return err
	}
	for _, job := range sparkApps {
		sparkApp := job.(*unstructured.Unstructured)
		j.updateSparkApp(nil, sparkApp)
	}
	return nil
}

func (j *JobGarbageCollector) updateVCJob(old, new interface{}) {
	log.Infof("update VCJob")

	oldVCJob := &batchv1alpha1.Job{}
	if old != nil {
		oldJob := old.(*unstructured.Unstructured)
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(oldJob.Object, oldVCJob); err != nil {
			log.Errorf("convert unstructured object[%+v] to VCJob failed: %v", old, err)
			return
		}
	}
	job := new.(*unstructured.Unstructured)
	vcjob := &batchv1alpha1.Job{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(job.Object, vcjob); err != nil {
		log.Errorf("convert unstructured object[%+v] to VCJob failed: %v", new, err)
		return
	}
	if vcjob.Status.State.Phase != oldVCJob.Status.State.Phase &&
		!reflect.DeepEqual(vcjob.Status, batchv1alpha1.JobStatus{}) &&
		!reflect.DeepEqual(vcjob.Status.State, batchv1alpha1.JobState{}) &&
		vcjob.Status.State.Phase != "" {
		log.Infof("update VCJob status=[%s]", vcjob.Status.State.Phase)
		// 当job 结束时：Succeeded 或 Failed 入队
		jobStatus, _ := commonschema.GetVCJobStatus(vcjob.Status.State.Phase)
		if j.isCleanJob(jobStatus) {
			finishedJob := FinishedJobInfo{
				Name:            vcjob.Name,
				Namespace:       vcjob.Namespace,
				GVK:             k8s.VCJobGVK,
				OwnerReferences: vcjob.OwnerReferences,
			}
			j.finishedJobDelayEnqueue(finishedJob)
		}
	}
}

func (j *JobGarbageCollector) updateSparkApp(old, new interface{}) {
	log.Infof("update SparkApp")

	oldSparkApp := &sparkoperatorv1beta2.SparkApplication{}
	if old != nil {
		oldJob := old.(*unstructured.Unstructured)
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(oldJob.Object, oldSparkApp); err != nil {
			log.Errorf("convert unstructured object[%+v] to sparkApp failed: %v", old, err)
			return
		}
	}
	job := new.(*unstructured.Unstructured)
	sparkApp := &sparkoperatorv1beta2.SparkApplication{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(job.Object, sparkApp); err != nil {
		log.Errorf("convert unstructured object[%+v] to sparkApp failed: %v", new, err)
		return
	}
	if sparkApp.Status.AppState.State != oldSparkApp.Status.AppState.State {
		jobStatus, _ := commonschema.GetSparkJobStatus(sparkApp.Status.AppState.State)
		if j.isCleanJob(jobStatus) {
			finishedJob := FinishedJobInfo{
				Name:            sparkApp.Name,
				Namespace:       sparkApp.Namespace,
				GVK:             k8s.SparkAppGVK,
				OwnerReferences: sparkApp.OwnerReferences,
			}
			j.finishedJobDelayEnqueue(finishedJob)
		}
	}
}

func (j *JobGarbageCollector) isCleanJob(jobStatus commonschema.JobStatus) bool {
	if !config.GlobalServerConfig.Job.Reclaim.CleanJob {
		return false
	}
	if config.GlobalServerConfig.Job.Reclaim.SkipCleanFailedJob {
		return commonschema.StatusJobSucceeded == jobStatus
	}
	return commonschema.StatusJobSucceeded == jobStatus || commonschema.StatusJobTerminated == jobStatus || commonschema.StatusJobFailed == jobStatus
}

type FinishedJobInfo struct {
	Name               string
	Namespace          string
	GVK                schema.GroupVersionKind
	LastTransitionTime v1.Time
	OwnerReferences    []v1.OwnerReference
}

func (j *JobGarbageCollector) finishedJobDelayEnqueue(job FinishedJobInfo) {
	duration := time.Duration(config.GlobalServerConfig.Job.Reclaim.JobTTLSeconds) * time.Second
	if !job.LastTransitionTime.IsZero() && time.Now().After(job.LastTransitionTime.Add(duration)) {
		duration = 0
	}
	ownerName := framework.FindOwnerReferenceName(job.OwnerReferences)
	log.Infof("finishedJobDelayEnqueue vcjob[%s] ownerName[%s] in ns[%s] duration[%v]",
		job.Name, ownerName, job.Namespace, duration)
	j.WaitedCleanQueue.AddAfter(&framework.FinishedJobInfo{
		GVK:       job.GVK,
		Namespace: job.Namespace,
		Name:      job.Name,
		OwnerName: ownerName,
	}, duration)
}
