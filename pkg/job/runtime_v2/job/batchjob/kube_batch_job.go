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
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/util/kuberuntime"
)

// KubeBatchJob is an executor that runs a batch job as a kubernetes native batch/v1 Job.
// The whole Job spec is provided by the user's extensionTemplate, so that all fields of
// the native Job (completions, parallelism, completionMode, backoffLimit, etc.) are kept.
type KubeBatchJob struct {
	kuberuntime.KubeBaseJob
}

func New(kubeClient framework.RuntimeClientInterface) framework.JobInterface {
	return &KubeBatchJob{
		KubeBaseJob: kuberuntime.NewKubeBaseJob(pfschema.BatchJobKindGroupVersion, kubeClient),
	}
}

func (bj *KubeBatchJob) Submit(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	// the native job executor is only used by batch jobs
	if job.JobType != pfschema.TypeBatch {
		return fmt.Errorf("batch job executor only supports job type %s, but got %s", pfschema.TypeBatch, job.JobType)
	}
	if len(job.ExtensionTemplate) == 0 {
		return fmt.Errorf("extensionTemplate for batch job %s is needed", job.NamespacedName())
	}
	jobName := job.NamespacedName()
	log.Debugf("begin to create %s", bj.String(jobName))

	// decode the template into an unstructured object, so fields unknown to the current
	// kubernetes client dependencies (e.g. completionMode) are not dropped
	batchJob, err := kuberuntime.CreateKubeUnstructuredFromYaml(bj.KindGroupVersion, job)
	if err != nil {
		log.Errorf("create %s failed, err %v", bj.String(jobName), err)
		return err
	}

	// merge labels and annotations of the job config, and the job config takes precedence
	mergeJobMeta(job)
	// set metadata field
	if err = bj.buildMetadata(batchJob, job); err != nil {
		log.Errorf("build metadata for %s failed, err: %v", bj.String(jobName), err)
		return err
	}
	// set scheduling policy for the native job
	if err = bj.buildSchedulingPolicy(batchJob, job); err != nil {
		log.Errorf("build scheduling policy for %s failed, err: %v", bj.String(jobName), err)
		return err
	}
	// validate and normalize the pod template
	if err = bj.buildPodTemplate(batchJob, job); err != nil {
		log.Errorf("build pod template for %s failed, err: %v", bj.String(jobName), err)
		return err
	}

	log.Debugf("begin to create %s, batchJob: %s", bj.String(jobName), batchJob)
	if err = bj.RuntimeClient.Create(batchJob, bj.KindGroupVersion); err != nil {
		log.Errorf("create %s failed, err %v", bj.String(jobName), err)
		return err
	}
	return nil
}

// mergeJobMeta merges the labels and annotations of the job config into the job.
// Values explicitly set on the job take precedence over the job config.
func mergeJobMeta(job *api.PFJob) {
	if job.Labels == nil {
		job.Labels = make(map[string]string)
	}
	if job.Annotations == nil {
		job.Annotations = make(map[string]string)
	}
	for key, value := range job.Conf.GetLabels() {
		if _, ok := job.Labels[key]; !ok {
			job.Labels[key] = value
		}
	}
	for key, value := range job.Conf.GetAnnotations() {
		if _, ok := job.Annotations[key]; !ok {
			job.Annotations[key] = value
		}
	}
}

func (bj *KubeBatchJob) buildMetadata(jobObj *unstructured.Unstructured, job *api.PFJob) error {
	if jobObj == nil || job == nil {
		return fmt.Errorf("job object or PFJob is nil")
	}
	accessor, err := meta.Accessor(jobObj)
	if err != nil {
		return err
	}
	objectMeta := metav1.ObjectMeta{
		Name:        accessor.GetName(),
		Namespace:   accessor.GetNamespace(),
		Labels:      accessor.GetLabels(),
		Annotations: accessor.GetAnnotations(),
	}
	kuberuntime.BuildJobMetadata(&objectMeta, job)
	// mark the job so that the batch job event filter can recognize it
	objectMeta.Labels[pfschema.JobLabelFramework] = string(pfschema.FrameworkK8SBatch)
	accessor.SetName(objectMeta.Name)
	accessor.SetNamespace(objectMeta.Namespace)
	accessor.SetLabels(objectMeta.Labels)
	accessor.SetAnnotations(objectMeta.Annotations)
	return nil
}

func (bj *KubeBatchJob) buildSchedulingPolicy(jobObj *unstructured.Unstructured, job *api.PFJob) error {
	if jobObj == nil || job == nil {
		return fmt.Errorf("job object or PFJob is nil")
	}
	// the queue of the job metadata is set by BuildJobMetadata, here we set it on the
	// pod template too, because child pods only inherit labels and annotations from it
	if len(job.QueueName) > 0 {
		templateAnnotations, _, err := unstructured.NestedStringMap(jobObj.Object,
			"spec", "template", "metadata", "annotations")
		if err != nil {
			return err
		}
		templateAnnotations = appendMapIfAbsent(templateAnnotations, map[string]string{
			pfschema.QueueLabelKey:           job.QueueName,
			pfschema.SchedulingQueueLabelKey: job.QueueName,
		})
		if err = unstructured.SetNestedStringMap(jobObj.Object, templateAnnotations,
			"spec", "template", "metadata", "annotations"); err != nil {
			return err
		}

		templateLabels, _, err := unstructured.NestedStringMap(jobObj.Object,
			"spec", "template", "metadata", "labels")
		if err != nil {
			return err
		}
		templateLabels = appendMapIfAbsent(templateLabels, map[string]string{
			pfschema.QueueLabelKey: job.QueueName,
		})
		if err = unstructured.SetNestedStringMap(jobObj.Object, templateLabels,
			"spec", "template", "metadata", "labels"); err != nil {
			return err
		}
	}
	// set priority, keep the priorityClassName in the template if no priority is specified
	priority := job.PriorityClassName
	if priority == "" {
		priority = job.Conf.GetPriority()
	}
	if priority != "" {
		if err := unstructured.SetNestedField(jobObj.Object, kuberuntime.KubePriorityClass(priority),
			"spec", "template", "spec", "priorityClassName"); err != nil {
			return err
		}
	}
	// set scheduler name, do not override the template with an empty value
	if schedulerName := config.GlobalServerConfig.Job.SchedulerName; schedulerName != "" {
		if err := unstructured.SetNestedField(jobObj.Object, schedulerName,
			"spec", "template", "spec", "schedulerName"); err != nil {
			return err
		}
	}
	return nil
}

// buildPodTemplate validates the pod template and normalizes the fields that PaddleFlow relies on.
func (bj *KubeBatchJob) buildPodTemplate(jobObj *unstructured.Unstructured, job *api.PFJob) error {
	if jobObj == nil || job == nil {
		return fmt.Errorf("job object or PFJob is nil")
	}
	containers, found, err := unstructured.NestedSlice(jobObj.Object, "spec", "template", "spec", "containers")
	if err != nil {
		return err
	}
	if !found || len(containers) == 0 {
		return fmt.Errorf("spec.template.spec.containers of batch job is needed")
	}

	// the pod template of a batch/v1 Job only supports Never or OnFailure
	restartPolicy, _, err := unstructured.NestedString(jobObj.Object, "spec", "template", "spec", "restartPolicy")
	if err != nil {
		return err
	}
	switch restartPolicy {
	case "":
		restartPolicy = string(corev1.RestartPolicyNever)
	case string(corev1.RestartPolicyNever), string(corev1.RestartPolicyOnFailure):
	default:
		return fmt.Errorf("restartPolicy of batch job pod template must be %s or %s, but got %s",
			corev1.RestartPolicyNever, corev1.RestartPolicyOnFailure, restartPolicy)
	}
	if err = unstructured.SetNestedField(jobObj.Object, restartPolicy,
		"spec", "template", "spec", "restartPolicy"); err != nil {
		return err
	}

	// child pods inherit labels and annotations from the pod template only, so add the
	// labels that PaddleFlow relies on to retrieve logs and tasks
	podLabels, _, err := unstructured.NestedStringMap(jobObj.Object, "spec", "template", "metadata", "labels")
	if err != nil {
		return err
	}
	podLabels = kuberuntime.BuildTaskLabels(podLabels, job.ID)
	return unstructured.SetNestedStringMap(jobObj.Object, podLabels, "spec", "template", "metadata", "labels")
}

func appendMapIfAbsent(maps map[string]string, addMaps map[string]string) map[string]string {
	if maps == nil {
		maps = make(map[string]string)
	}
	for key, value := range addMaps {
		if _, ok := maps[key]; !ok {
			maps[key] = value
		}
	}
	return maps
}

func (bj *KubeBatchJob) AddEventListener(ctx context.Context, listenerType string,
	jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	switch listenerType {
	case pfschema.ListenerTypeJob:
		return bj.AddJobEventListener(ctx, jobQueue, listener, bj.JobStatus, filterFunc)
	default:
		// task events are handled by the single job plugin (Pod), skip them here
		return fmt.Errorf("listenerType %s is not supported", listenerType)
	}
}

func filterFunc(obj interface{}) bool {
	jobObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return false
	}
	labels := jobObj.GetLabels()
	if labels == nil {
		return false
	}
	if labels[pfschema.JobOwnerLabel] != pfschema.JobOwnerValue {
		return false
	}
	framework := labels[pfschema.JobLabelFramework]
	// standalone jobs were submitted as batch jobs before batch got its own framework label
	return framework == string(pfschema.FrameworkK8SBatch) || framework == string(pfschema.FrameworkStandalone)
}

// JobStatus get batch job status, message from interface{}, and covert to JobStatus
func (bj *KubeBatchJob) JobStatus(obj interface{}) (api.StatusInfo, error) {
	unObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return api.StatusInfo{}, fmt.Errorf("convert object %v to unstructured failed", obj)
	}
	batchJob := &batchv1.Job{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, batchJob); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s job failed. error: %s", obj, bj.KindGroupVersion, err.Error())
		return api.StatusInfo{}, err
	}
	state, msg, originStatus := getBatchJobStatus(&batchJob.Status)
	log.Infof("Batch job status: %s", state)
	return api.StatusInfo{
		OriginStatus: originStatus,
		Status:       state,
		Message:      msg,
		StartTime:    kuberuntime.GetKubeTime(batchJob.Status.StartTime),
		FinishedTime: getBatchJobFinishedTime(&batchJob.Status),
	}, nil
}

func getBatchJobStatus(jobStatus *batchv1.JobStatus) (pfschema.JobStatus, string, string) {
	if jobStatus == nil {
		return pfschema.StatusJobPending, "job is pending", "Pending"
	}
	for idx := range jobStatus.Conditions {
		condition := &jobStatus.Conditions[idx]
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case batchv1.JobComplete:
			msg := condition.Message
			if msg == "" {
				msg = "job is succeeded"
			}
			return pfschema.StatusJobSucceeded, msg, string(condition.Type)
		case batchv1.JobFailed:
			msg := condition.Message
			if msg == "" {
				msg = "job is failed"
			}
			return pfschema.StatusJobFailed, msg, string(condition.Type)
		}
	}
	if jobStatus.CompletionTime != nil {
		return pfschema.StatusJobSucceeded, "job is succeeded", string(batchv1.JobComplete)
	}
	// the job is still running as long as some pods are active or have succeeded
	if jobStatus.Active > 0 || jobStatus.Succeeded > 0 {
		return pfschema.StatusJobRunning, "job is running", "Running"
	}
	return pfschema.StatusJobPending, "job is pending", "Pending"
}

func getBatchJobFinishedTime(jobStatus *batchv1.JobStatus) *time.Time {
	if jobStatus == nil {
		return nil
	}
	if finishedTime := kuberuntime.GetKubeTime(jobStatus.CompletionTime); finishedTime != nil {
		return finishedTime
	}
	// CompletionTime may be empty for failed jobs, fallback to the job failed condition
	for idx := range jobStatus.Conditions {
		condition := &jobStatus.Conditions[idx]
		if condition.Status == corev1.ConditionTrue && condition.Type == batchv1.JobFailed {
			return kuberuntime.GetKubeTime(&condition.LastTransitionTime)
		}
	}
	return nil
}
