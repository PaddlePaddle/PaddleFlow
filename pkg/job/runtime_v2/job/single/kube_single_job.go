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

package single

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/util/kuberuntime"
)

var (
	JobGVK              = k8s.PodGVK
	KubeSingleFwVersion = client.KubeFrameworkVersion(JobGVK)
)

// KubeSingleJob is an executor struct that runs a single job
type KubeSingleJob struct {
	GVK              schema.GroupVersionKind
	frameworkVersion pfschema.FrameworkVersion
	runtimeClient    framework.RuntimeClientInterface
	jobQueue         workqueue.RateLimitingInterface
	taskQueue        workqueue.RateLimitingInterface
}

func New(kubeClient framework.RuntimeClientInterface) framework.JobInterface {
	singleJob := &KubeSingleJob{
		runtimeClient:    kubeClient,
		GVK:              JobGVK,
		frameworkVersion: KubeSingleFwVersion,
	}
	return singleJob
}

func (sp *KubeSingleJob) String(name string) string {
	return fmt.Sprintf("%s job %s on %s", sp.GVK.String(), name, sp.runtimeClient.Cluster())
}

func (sp *KubeSingleJob) Submit(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Debugf("begin to create %s", sp.String(jobName))

	singlePod := &v1.Pod{}
	err := kuberuntime.CreateKubeJobFromYaml(singlePod, sp.GVK, job)
	if err != nil {
		log.Errorf("create %s failed, err %v", sp.String(jobName), err)
		return err
	}

	// set metadata field
	kuberuntime.BuildJobMetadata(&singlePod.ObjectMeta, job)
	// set framework for single job
	singlePod.Labels[pfschema.JobLabelFramework] = string(pfschema.FrameworkStandalone)

	// set scheduling policy for single job
	if err = sp.buildSchedulingPolicy(singlePod, job); err != nil {
		log.Errorf("build scheduling policy for %s failed, err: %v", sp.String(jobName), err)
		return err
	}

	// build job spec field
	if job.IsCustomYaml {
		// set custom single job Spec from user
		err = sp.customSingleJob(singlePod, job)
	} else {
		// set builtin single job Spec
		err = sp.builtinSingleJob(singlePod, job)
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", sp.String(jobName), err)
		return err
	}
	log.Debugf("begin to create %s, singlePod: %s", sp.String(jobName), singlePod)
	err = sp.runtimeClient.Create(singlePod, sp.frameworkVersion)
	if err != nil {
		log.Errorf("create %s failed, err %v", sp.String(jobName), err)
		return err
	}
	return nil
}

func (sp *KubeSingleJob) buildSchedulingPolicy(jobPod *v1.Pod, job *api.PFJob) error {
	if jobPod == nil || job == nil {
		return fmt.Errorf("jobSpec or PFJob is nil")
	}
	// set queue
	if len(job.QueueName) > 0 {
		jobPod.Annotations[pfschema.QueueLabelKey] = job.QueueName
	}
	// set priority
	jobPod.Spec.PriorityClassName = kuberuntime.KubePriorityClass(job.PriorityClassName)
	return nil
}

func (sp *KubeSingleJob) customSingleJob(jobPod *v1.Pod, job *api.PFJob) error {
	if jobPod == nil || job == nil {
		return fmt.Errorf("jobSpec or PFJob is nil")
	}
	podSpec := &jobPod.Spec
	if len(job.Tasks) == 0 {
		log.Debugf("custom singleJob has no tasks")
		return nil
	}

	task := job.Tasks[0]
	if podSpec.Containers == nil || len(podSpec.Containers) == 0 {
		log.Warningf("singleJob without any container")
		podSpec.Containers = []v1.Container{{}}
	}
	container := podSpec.Containers[0]
	// fill image
	if task.Image != "" {
		container.Image = task.Image
	}
	// set resource
	var err error
	container.Resources, err = kuberuntime.GenerateResourceRequirements(task.Flavour, task.LimitFlavour)
	if err != nil {
		log.Errorf("generate resource requirements failed, err: %v", err)
		return err
	}
	podSpec.Containers[0] = container
	return nil

}

func (sp *KubeSingleJob) builtinSingleJob(jobPod *v1.Pod, job *api.PFJob) error {
	jobName := job.NamespacedName()
	if len(job.Tasks) != 1 {
		return fmt.Errorf("create builtin %s failed, job member is nil", sp.String(jobName))
	}
	if job.Tasks[0].Name == "" {
		job.Tasks[0].Name = job.ID
	}
	return kuberuntime.BuildPod(jobPod, job.Tasks[0])
}

func (sp *KubeSingleJob) Stop(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Infof("begin to stop %s", sp.String(jobName))
	if err := sp.runtimeClient.Delete(job.Namespace, job.ID, sp.frameworkVersion); err != nil {
		log.Errorf("stop %s failed, err: %v", sp.String(jobName), err)
		return err
	}
	return nil
}

func (sp *KubeSingleJob) Update(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Infof("begin to update %s", sp.String(jobName))
	if err := kuberuntime.UpdateKubeJob(job, sp.runtimeClient, sp.frameworkVersion); err != nil {
		log.Errorf("update %s failed, err: %v", sp.String(jobName), err)
		return err
	}
	return nil
}

func (sp *KubeSingleJob) Delete(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Infof("begin to delete %s ", sp.String(jobName))
	if err := sp.runtimeClient.Delete(job.Namespace, job.ID, sp.frameworkVersion); err != nil {
		log.Errorf("delete %s failed, err %v", sp.String(jobName), err)
		return err
	}
	return nil
}

func (sp *KubeSingleJob) GetLog(ctx context.Context, jobLogRequest pfschema.JobLogRequest) (pfschema.JobLogInfo, error) {
	// TODO: add get log logic
	return pfschema.JobLogInfo{}, nil
}

func (sp *KubeSingleJob) AddEventListener(ctx context.Context, listenerType string, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeJob:
		err = sp.addJobEventListener(ctx, jobQueue, listener)
	case pfschema.ListenerTypeTask:
		err = sp.addTaskEventListener(ctx, jobQueue, listener)
	default:
		err = fmt.Errorf("listenerType %s is not supported", listenerType)
	}
	return err
}

func (sp *KubeSingleJob) addJobEventListener(ctx context.Context, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	if jobQueue == nil || listener == nil {
		return fmt.Errorf("add job event listener failed, err: listener is nil")
	}
	sp.jobQueue = jobQueue
	informer := listener.(cache.SharedIndexInformer)
	informer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			job := obj.(*unstructured.Unstructured)
			labels := job.GetLabels()
			jobName := job.GetLabels()
			if labels != nil && labels[pfschema.JobOwnerLabel] == pfschema.JobOwnerValue {
				if labels[pfschema.JobLabelFramework] != string(pfschema.FrameworkStandalone) {
					log.Debugf("job %s is not single job", jobName)
					return false
				}
				log.Debugf("responsible for handle job. jobName:[%s]", jobName)
				return true
			}
			return false
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    sp.addJob,
			UpdateFunc: sp.updateJob,
			DeleteFunc: sp.deleteJob,
		},
	})
	return nil
}

func (sp *KubeSingleJob) addJob(obj interface{}) {
	jobSyncInfo, err := kuberuntime.JobAddFunc(obj, sp.JobStatus)
	if err != nil {
		return
	}
	sp.jobQueue.Add(jobSyncInfo)
}

func (sp *KubeSingleJob) updateJob(old, new interface{}) {
	jobSyncInfo, err := kuberuntime.JobUpdateFunc(old, new, sp.JobStatus)
	if err != nil {
		return
	}
	sp.jobQueue.Add(jobSyncInfo)
}

func (sp *KubeSingleJob) deleteJob(obj interface{}) {
	jobSyncInfo, err := kuberuntime.JobDeleteFunc(obj, sp.JobStatus)
	if err != nil {
		return
	}
	sp.jobQueue.Add(jobSyncInfo)
}

// JobStatus get single job status, message from interface{}, and covert to JobStatus
func (sp *KubeSingleJob) JobStatus(obj interface{}) (api.StatusInfo, error) {
	unObj := obj.(*unstructured.Unstructured)
	// convert to Pod struct
	job := &v1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, job); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s pod failed. error: %s", obj, sp.GVK.String(), err.Error())
		return api.StatusInfo{}, err
	}
	// convert job status
	state, msg, err := sp.getJobStatus(&job.Status)
	if err != nil {
		log.Errorf("get single job status failed, err: %v", err)
		return api.StatusInfo{}, err
	}
	log.Infof("Single job status: %s", state)
	return api.StatusInfo{
		OriginStatus: string(job.Status.Phase),
		Status:       state,
		Message:      msg,
	}, nil
}

func (sp *KubeSingleJob) getJobStatus(jobStatus *v1.PodStatus) (pfschema.JobStatus, string, error) {
	status := pfschema.JobStatus("")
	msg := ""
	switch jobStatus.Phase {
	case v1.PodPending:
		status = pfschema.StatusJobPending
		msg = "job is pending"
	case v1.PodRunning:
		status = pfschema.StatusJobRunning
		msg = "job is running"
	case v1.PodSucceeded:
		status = pfschema.StatusJobSucceeded
		msg = "job is succeeded"
	case v1.PodFailed, v1.PodUnknown:
		status = pfschema.StatusJobFailed
		msg = getJobMessage(jobStatus)
	default:
		return status, msg, fmt.Errorf("unexpected single job status: %s", jobStatus.Phase)
	}
	return status, msg, nil
}

func getJobMessage(jobStatus *v1.PodStatus) string {
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

func (sp *KubeSingleJob) addTaskEventListener(ctx context.Context, taskQueue workqueue.RateLimitingInterface, listener interface{}) error {
	if taskQueue == nil || listener == nil {
		return fmt.Errorf("add task event listener failed, err: listener is nil")
	}
	sp.taskQueue = taskQueue
	podInformer := listener.(cache.SharedIndexInformer)
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sp.addTask,
		UpdateFunc: sp.updateTask,
		DeleteFunc: sp.deleteTask,
	},
	)
	return nil
}

func (sp *KubeSingleJob) addTask(obj interface{}) {
	kuberuntime.TaskUpdateFunc(obj, pfschema.Create, sp.taskQueue)
}

func (sp *KubeSingleJob) updateTask(old, new interface{}) {
	kuberuntime.TaskUpdate(old, new, sp.taskQueue, sp.jobQueue)
}

func (sp *KubeSingleJob) deleteTask(obj interface{}) {
	kuberuntime.TaskUpdateFunc(obj, pfschema.Delete, sp.taskQueue)
}
