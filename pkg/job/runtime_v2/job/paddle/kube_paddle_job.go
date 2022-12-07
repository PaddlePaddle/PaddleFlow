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

package paddle

import (
	"context"
	"fmt"

	paddlejobv1 "github.com/paddleflow/paddle-operator/api/v1"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/util/kuberuntime"
)

var (
	JobGVK              = k8s.PaddleJobGVK
	KubePaddleFwVersion = client.KubeFrameworkVersion(JobGVK)
)

// KubePaddleJob is an executor struct that runs a paddle job
type KubePaddleJob struct {
	GVK              schema.GroupVersionKind
	frameworkVersion pfschema.FrameworkVersion
	runtimeClient    framework.RuntimeClientInterface
	jobQueue         workqueue.RateLimitingInterface
}

func New(kubeClient framework.RuntimeClientInterface) framework.JobInterface {
	return &KubePaddleJob{
		runtimeClient:    kubeClient,
		GVK:              JobGVK,
		frameworkVersion: KubePaddleFwVersion,
	}
}

func (pj *KubePaddleJob) String(name string) string {
	return fmt.Sprintf("%s job %s on %s", pj.GVK.String(), name, pj.runtimeClient.Cluster())
}

func (pj *KubePaddleJob) Submit(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Debugf("begin to create %s", pj.String(jobName))

	pdj := &paddlejobv1.PaddleJob{}
	err := kuberuntime.CreateKubeJobFromYaml(pdj, pj.GVK, job)
	if err != nil {
		log.Errorf("create %s failed, err %v", pj.String(jobName), err)
		return err
	}
	if err := pj.validatePaddleContainers(pdj, job); err != nil {
		log.Errorf("validate paddlejob %s failed, err: %v", pj.String(jobName), err)
		return err
	}

	// set metadata field
	kuberuntime.BuildJobMetadata(&pdj.ObjectMeta, job)
	// set scheduling policy for paddle job
	if err := pj.buildSchedulingPolicy(&pdj.Spec, &job.Conf); err != nil {
		log.Errorf("build scheduling policy for %s failed, err: %v", pj.String(jobName), err)
		return err
	}

	// build job spec field
	if job.IsCustomYaml {
		// set custom PaddleJob Spec from user
		err = pj.customPaddleJob(pdj, job)
	} else {
		// set builtin PaddleJob Spec
		err = pj.builtinPaddleJob(pdj, job)
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", pj.String(jobName), err)
		return err
	}
	log.Debugf("begin to create %s, paddle job info: %v", pj.String(jobName), pdj)
	err = pj.runtimeClient.Create(pdj, pj.frameworkVersion)
	if err != nil {
		log.Errorf("create %s failed, err %v", pj.String(jobName), err)
		return err
	}
	return nil
}

func (pj *KubePaddleJob) customPaddleJob(pdj *paddlejobv1.PaddleJob, job *api.PFJob) error {
	log.Infof("customPaddleJob fill resource")
	if pdj == nil || job == nil {
		return fmt.Errorf("jobSpec or PFJob is nil")
	}
	if err := pj.validateCustomYaml(pdj, job.ID); err != nil {
		log.Errorf("validate custom yaml for %s failed, err: %v", pj.String(job.ID), err)
		return err
	}
	if err := pj.patchResource(pdj, job); err != nil {
		log.Errorf("patch resource for paddlejob %s failed, err: %v", pj.String(job.ID), err)
		return err
	}
	return nil
}

func (pj *KubePaddleJob) validateCustomYaml(pdj *paddlejobv1.PaddleJob, jobID string) error {
	log.Infof("validate custom yaml for %s, and yaml content: %v", pj.String(jobID), pdj)
	resourceSpecs := []*paddlejobv1.ResourceSpec{pdj.Spec.PS, pdj.Spec.Worker}
	for _, resourceSpec := range resourceSpecs {
		if resourceSpec == nil {
			continue
		}
		kuberuntime.BuildTaskMetadata(&resourceSpec.Template.ObjectMeta, jobID, &pfschema.Conf{})
		if err := kuberuntime.ValidatePodResources(&resourceSpec.Template.Spec); err != nil {
			err = fmt.Errorf("validate resource in extensionTemplate.Worker failed, err %v", err)
			log.Errorf("%v", err)
			return err
		}
	}
	return nil
}

func (pj *KubePaddleJob) buildSchedulingPolicy(pdjSpec *paddlejobv1.PaddleJobSpec, jobConf pfschema.PFJobConf) error {
	if pdjSpec == nil {
		return fmt.Errorf("build scheduling policy failed, err: pdjSpec is nil")
	}
	// .Spec.SchedulingPolicy
	if pdjSpec.SchedulingPolicy == nil {
		pdjSpec.SchedulingPolicy = &paddlejobv1.SchedulingPolicy{}
	}
	if len(pdjSpec.SchedulingPolicy.Queue) == 0 {
		pdjSpec.SchedulingPolicy.Queue = jobConf.GetQueueName()
	}
	if len(pdjSpec.SchedulingPolicy.PriorityClass) == 0 {
		pdjSpec.SchedulingPolicy.PriorityClass = kuberuntime.KubePriorityClass(jobConf.GetPriority())
	}
	if pdjSpec.SchedulingPolicy.MinAvailable == nil {
		// TODO: study for paddlejob's minAvailable
		var minAvailable int32 = 1
		pdjSpec.SchedulingPolicy.MinAvailable = &minAvailable
	}

	// WithGloo indicate whether enable gloo, 0/1/2 for disable/enable for worker/enable for server, default 1
	var withGloo int = 1
	if pdjSpec.WithGloo == nil {
		pdjSpec.WithGloo = &withGloo
	}
	// policy will clean pods only on job completed
	if len(pdjSpec.CleanPodPolicy) == 0 {
		pdjSpec.CleanPodPolicy = paddlejobv1.CleanOnCompletion
	}
	// way of communicate between pods, choose one in Service/PodIP
	if len(pdjSpec.Intranet) == 0 {
		pdjSpec.Intranet = paddlejobv1.PodIP
	}
	return nil
}

func (pj *KubePaddleJob) builtinPaddleJob(pdj *paddlejobv1.PaddleJob, job *api.PFJob) error {
	if pdj == nil || job == nil {
		return fmt.Errorf("PaddleJob or PFJob is nil")
	}
	jobName := job.NamespacedName()
	// build job tasks
	var minAvailable int32
	minResources := resources.EmptyResource()
	for _, task := range job.Tasks {
		var err error
		switch task.Role {
		case pfschema.RolePServer:
			// patch parameter server
			err = pj.patchPaddleTask(pdj.Spec.PS, task, job.ID)
		case pfschema.RolePWorker, pfschema.RoleWorker:
			// patch worker
			err = pj.patchPaddleTask(pdj.Spec.Worker, task, job.ID)
		default:
			err = fmt.Errorf("role %s is not supported", task.Role)
		}
		if err != nil {
			log.Errorf("build task for paddle job with role %s failed, err: %v", task.Role, err)
			return err
		}
		// calculate min resources
		taskResources, err := resources.NewResourceFromMap(task.Flavour.ToMap())
		if err != nil {
			log.Errorf("parse resources for %s task failed, err: %v", pj.String(jobName), err)
			return err
		}
		taskResources.Multi(task.Replicas)
		minResources.Add(taskResources)
		// calculate min available
		minAvailable += int32(task.Replicas)
	}
	// set minAvailable and minResources for paddle job
	if pdj.Spec.SchedulingPolicy != nil {
		pdj.Spec.SchedulingPolicy.MinAvailable = &minAvailable
		pdj.Spec.SchedulingPolicy.MinResources = k8s.NewResourceList(minResources)
	}
	return nil
}

// patchPaddleTask patch info into task of paddle job
func (pj *KubePaddleJob) patchPaddleTask(resourceSpec *paddlejobv1.ResourceSpec, task pfschema.Member, jobID string) error {
	log.Infof("patch paddle task %s, task: %#v", pj.String(""), task)
	if resourceSpec == nil {
		resourceSpec = &paddlejobv1.ResourceSpec{}
	}
	if task.Replicas > 0 {
		resourceSpec.Replicas = task.Replicas
	}
	if resourceSpec.Replicas <= 0 {
		resourceSpec.Replicas = kuberuntime.DefaultReplicas
	}
	// set metadata
	if task.Name == "" {
		task.Name = uuid.GenerateIDWithLength(jobID, 3)
	}
	kuberuntime.BuildTaskMetadata(&resourceSpec.Template.ObjectMeta, jobID, &task.Conf)
	// build pod spec
	return kuberuntime.BuildPodSpec(&resourceSpec.Template.Spec, task)
}

func (pj *KubePaddleJob) Stop(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Infof("begin to stop %s", pj.String(jobName))
	if err := pj.runtimeClient.Delete(job.Namespace, job.ID, pj.frameworkVersion); err != nil {
		log.Errorf("stop %s failed, err: %v", pj.String(jobName), err)
		return err
	}
	return nil
}

func (pj *KubePaddleJob) Update(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Infof("begin to update %s", pj.String(jobName))
	if err := kuberuntime.UpdateKubeJob(job, pj.runtimeClient, pj.frameworkVersion); err != nil {
		log.Errorf("update %s failed, err: %v", pj.String(jobName), err)
		return err
	}
	return nil
}

func (pj *KubePaddleJob) Delete(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Infof("begin to delete %s ", pj.String(jobName))
	if err := pj.runtimeClient.Delete(job.Namespace, job.ID, pj.frameworkVersion); err != nil {
		log.Errorf("delete %s failed, err %v", pj.String(jobName), err)
		return err
	}
	return nil
}

func (pj *KubePaddleJob) GetLog(ctx context.Context, jobLogRequest pfschema.JobLogRequest) (pfschema.JobLogInfo, error) {
	// TODO: add get log logic
	return pfschema.JobLogInfo{}, nil
}

func (pj *KubePaddleJob) AddEventListener(ctx context.Context, listenerType string, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeJob:
		err = pj.addJobEventListener(ctx, jobQueue, listener)
	default:
		err = fmt.Errorf("listenerType %s is not supported", listenerType)
	}
	return err
}

func (pj *KubePaddleJob) addJobEventListener(ctx context.Context, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	if jobQueue == nil || listener == nil {
		return fmt.Errorf("add job event listener failed, err: listener is nil")
	}
	pj.jobQueue = jobQueue
	informer := listener.(cache.SharedIndexInformer)
	informer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kuberuntime.ResponsibleForJob,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    pj.addJob,
			UpdateFunc: pj.updateJob,
			DeleteFunc: pj.deleteJob,
		},
	})
	return nil
}

func (pj *KubePaddleJob) addJob(obj interface{}) {
	jobSyncInfo, err := kuberuntime.JobAddFunc(obj, pj.JobStatus)
	if err != nil {
		return
	}
	pj.jobQueue.Add(jobSyncInfo)
}

func (pj *KubePaddleJob) updateJob(old, new interface{}) {
	jobSyncInfo, err := kuberuntime.JobUpdateFunc(old, new, pj.JobStatus)
	if err != nil {
		return
	}
	pj.jobQueue.Add(jobSyncInfo)
}

func (pj *KubePaddleJob) deleteJob(obj interface{}) {
	jobSyncInfo, err := kuberuntime.JobDeleteFunc(obj, pj.JobStatus)
	if err != nil {
		return
	}
	pj.jobQueue.Add(jobSyncInfo)
}

// JobStatus get the statusInfo of paddle job, including origin status, pf status and message
func (pj *KubePaddleJob) JobStatus(obj interface{}) (api.StatusInfo, error) {
	unObj := obj.(*unstructured.Unstructured)
	// convert to PaddleJob struct
	job := &paddlejobv1.PaddleJob{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, job); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s job failed. error: %s", obj, pj.GVK.String(), err)
		return api.StatusInfo{}, err
	}
	// convert job status
	state, msg, err := pj.getJobStatus(&job.Status)
	if err != nil {
		log.Errorf("get PaddleJob status failed, err: %v", err)
		return api.StatusInfo{}, err
	}
	log.Infof("Paddle job status: %s", state)
	return api.StatusInfo{
		OriginStatus: string(job.Status.Phase),
		Status:       state,
		Message:      msg,
	}, nil
}

func (pj *KubePaddleJob) getJobStatus(jobStatus *paddlejobv1.PaddleJobStatus) (pfschema.JobStatus, string, error) {
	status := pfschema.JobStatus("")
	msg := ""
	if jobStatus == nil {
		return status, msg, fmt.Errorf("job status is nil")
	}
	switch jobStatus.Phase {
	case paddlejobv1.Starting, paddlejobv1.Pending:
		status = pfschema.StatusJobPending
		msg = "paddle job is pending"
	case paddlejobv1.Running, paddlejobv1.Restarting, paddlejobv1.Completing, paddlejobv1.Scaling:
		status = pfschema.StatusJobRunning
		msg = "paddle job is running"
	case paddlejobv1.Terminating, paddlejobv1.Aborting:
		status = pfschema.StatusJobTerminating
		msg = "paddle job is terminating"
	case paddlejobv1.Completed, paddlejobv1.Succeed:
		status = pfschema.StatusJobSucceeded
		msg = "paddle job is succeeded"
	case paddlejobv1.Aborted:
		status = pfschema.StatusJobTerminated
		msg = "paddle job is terminated"
	case paddlejobv1.Failed, paddlejobv1.Terminated, paddlejobv1.Unknown:
		status = pfschema.StatusJobFailed
		msg = "paddle job is failed"
	default:
		return status, msg, fmt.Errorf("unexpected paddlejob status: %s", jobStatus.Phase)
	}
	return status, msg, nil
}

func (pj *KubePaddleJob) patchResource(pdj *paddlejobv1.PaddleJob, job *api.PFJob) error {
	log.Infof("patch resource in paddlejob")
	if len(job.Tasks) == 0 {
		log.Debugf("no resources to be configured")
		return nil
	}

	// fill resource
	var minAvailable int32
	minResources := resources.EmptyResource()
	for _, task := range job.Tasks {
		if pfschema.IsEmptyResource(task.Flavour.ResourceInfo) {
			continue
		}
		resourceRequirements, err := kuberuntime.GenerateResourceRequirements(task.Flavour, task.LimitFlavour)
		if err != nil {
			log.Errorf("generate resource requirements failed, err: %v", err)
			return err
		}
		switch task.Role {
		case pfschema.RolePServer:
			pdj.Spec.PS.Template.Spec.Containers[0].Resources = resourceRequirements
		case pfschema.RolePWorker, pfschema.RoleWorker:
			pdj.Spec.Worker.Template.Spec.Containers[0].Resources = resourceRequirements
		default:
			err = fmt.Errorf("role %s is not supported", task.Role)
		}
		if err != nil {
			log.Errorf("build task for paddle job with role %s failed, err: %v", task.Role, err)
			return err
		}
		// calculate min resources
		taskResources, err := resources.NewResourceFromMap(task.Flavour.ToMap())
		if err != nil {
			log.Errorf("parse resources for %s task failed, err: %v", pj.String(job.ID), err)
			return err
		}
		taskResources.Multi(task.Replicas)
		minResources.Add(taskResources)
		// calculate min available
		minAvailable += int32(task.Replicas)
	}
	// set minAvailable and minResources for paddle job
	if pdj.Spec.SchedulingPolicy != nil {
		pdj.Spec.SchedulingPolicy.MinAvailable = &minAvailable
		pdj.Spec.SchedulingPolicy.MinResources = k8s.NewResourceList(minResources)
	}
	return nil
}

func (pj *KubePaddleJob) validatePaddleContainers(pdj *paddlejobv1.PaddleJob, job *api.PFJob) error {
	nilContainerErr := fmt.Errorf("container is required in paddleJob")
	if pdj.Spec.PS == nil && job.JobMode == pfschema.EnvJobModePS {
		err := fmt.Errorf("PS mode required spec.PS")
		log.Errorln(err)
		return err
	}
	if pdj.Spec.PS != nil {
		psContainers := pdj.Spec.PS.Template.Spec.Containers
		if len(psContainers) == 0 {
			log.Errorln(nilContainerErr)
			return nilContainerErr
		}
	}
	if pdj.Spec.Worker == nil {
		err := fmt.Errorf("worker is required in paddleJob")
		log.Errorln(err)
		return err
	}
	if len(pdj.Spec.Worker.Template.Spec.Containers) == 0 {
		log.Errorln(nilContainerErr)
		return nilContainerErr
	}
	return nil
}
