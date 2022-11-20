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

package mpi

import (
	"context"
	"fmt"

	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	mpiv1 "github.com/kubeflow/training-operator/pkg/apis/mpi/v1"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/util/kuberuntime"
)

var (
	JobGVK           = k8s.MPIJobGVK
	KubeMPIFwVersion = client.KubeFrameworkVersion(JobGVK)
)

// KubeMPIJob is a struct that runs a mpi job
type KubeMPIJob struct {
	GVK              schema.GroupVersionKind
	frameworkVersion pfschema.FrameworkVersion
	runtimeClient    framework.RuntimeClientInterface
	jobQueue         workqueue.RateLimitingInterface
}

func New(kubeClient framework.RuntimeClientInterface) framework.JobInterface {
	return &KubeMPIJob{
		runtimeClient:    kubeClient,
		GVK:              JobGVK,
		frameworkVersion: KubeMPIFwVersion,
	}
}

func (mj *KubeMPIJob) String(name string) string {
	return fmt.Sprintf("%s job %s on %s", mj.GVK.String(), name, mj.runtimeClient.Cluster())
}

func (mj *KubeMPIJob) Submit(ctx context.Context, job *api.PFJob) error {
	jobName := job.NamespacedName()
	mpiJob := &mpiv1.MPIJob{}
	if err := kuberuntime.CreateKubeJobFromYaml(mpiJob, mj.GVK, job); err != nil {
		log.Errorf("create %s failed, err %v", mj.String(jobName), err)
		return err
	}

	var err error
	// set metadata field
	kuberuntime.BuildJobMetadata(&mpiJob.ObjectMeta, job)
	// set spec field
	if job.IsCustomYaml {
		// set custom MPIJob Spec from user
		err = mj.customMPIJobSpec(&mpiJob.Spec, job)
	} else {
		// set builtin MPIJob Spec
		err = mj.builtinMPIJobSpec(&mpiJob.Spec, job)
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", mj.String(jobName), err)
		return err
	}
	log.Debugf("begin to create %s, job info: %v", mj.String(jobName), mpiJob)
	err = mj.runtimeClient.Create(mpiJob, mj.frameworkVersion)
	if err != nil {
		log.Errorf("create %s failed, err %v", mj.String(jobName), err)
		return err
	}
	return nil
}

// builtinMPIJobSpec set build-in MPIJob spec
func (mj *KubeMPIJob) builtinMPIJobSpec(mpiJobSpec *mpiv1.MPIJobSpec, job *api.PFJob) error {
	jobName := job.NamespacedName()
	log.Debugf("patch %s spec:%#v", mj.String(jobName), mpiJobSpec)
	// TODO: set ElasticPolicy for MPIJob
	// set MPIReplicaSpecs
	minResources := resources.EmptyResource()
	for _, task := range job.Tasks {
		replicaType := mpiv1.MPIReplicaTypeLauncher
		if task.Role == pfschema.RoleWorker || task.Role == pfschema.RolePWorker {
			// mpi worker
			replicaType = mpiv1.MPIReplicaTypeWorker
		}
		replicaSpec := mpiJobSpec.MPIReplicaSpecs[replicaType]
		if err := kuberuntime.KubeflowReplicaSpec(replicaSpec, job.ID, &task); err != nil {
			log.Errorf("build %s RepilcaSpec for %s failed, err: %v", replicaType, mj.String(jobName), err)
			return err
		}
		// calculate job minResources
		taskResources, _ := resources.NewResourceFromMap(task.Flavour.ToMap())
		taskResources.Multi(task.Replicas)
		minResources.Add(taskResources)
	}
	// set RunPolicy
	resourceList := k8s.NewResourceList(minResources)
	return kuberuntime.KubeflowRunPolicy(&mpiJobSpec.RunPolicy, &resourceList, job.Conf.GetQueueName(), job.Conf.GetPriority())
}

// customMPIJobSpec set custom MPIJob Spec
func (mj *KubeMPIJob) customMPIJobSpec(mpiJobSpec *mpiv1.MPIJobSpec, job *api.PFJob) error {
	jobName := job.NamespacedName()
	log.Debugf("patch %s spec:%#v", mj.String(jobName), mpiJobSpec)
	// patch metadata
	ps, find := mpiJobSpec.MPIReplicaSpecs[mpiv1.MPIReplicaTypeLauncher]
	if find && ps != nil {
		kuberuntime.BuildTaskMetadata(&ps.Template.ObjectMeta, job.ID, &pfschema.Conf{})
	}
	worker, find := mpiJobSpec.MPIReplicaSpecs[mpiv1.MPIReplicaTypeWorker]
	if find && worker != nil {
		kuberuntime.BuildTaskMetadata(&worker.Template.ObjectMeta, job.ID, &pfschema.Conf{})
	}
	// TODO: patch mpi job from user
	// check RunPolicy
	return kuberuntime.KubeflowRunPolicy(&mpiJobSpec.RunPolicy, nil, job.Conf.GetQueueName(), job.Conf.GetPriority())
}

func (mj *KubeMPIJob) Stop(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Infof("begin to stop %s", mj.String(jobName))
	if err := mj.runtimeClient.Delete(job.Namespace, job.ID, mj.frameworkVersion); err != nil {
		log.Errorf("stop %s failed, err: %v", mj.String(jobName), err)
		return err
	}
	return nil
}

func (mj *KubeMPIJob) Update(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Infof("begin to update %s", mj.String(jobName))
	if err := kuberuntime.UpdateKubeJob(job, mj.runtimeClient, mj.frameworkVersion); err != nil {
		log.Errorf("update %s failed, err: %v", mj.String(jobName), err)
		return err
	}
	return nil
}

func (mj *KubeMPIJob) Delete(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Infof("begin to delete %s ", mj.String(jobName))
	if err := mj.runtimeClient.Delete(job.Namespace, job.ID, mj.frameworkVersion); err != nil {
		log.Errorf("delete %s failed, err %v", mj.String(jobName), err)
		return err
	}
	return nil
}

func (mj *KubeMPIJob) GetLog(ctx context.Context, jobLogRequest pfschema.JobLogRequest) (pfschema.JobLogInfo, error) {
	// TODO: add get log logic
	return pfschema.JobLogInfo{}, nil
}

func (mj *KubeMPIJob) AddEventListener(ctx context.Context, listenerType string, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeJob:
		err = mj.addJobEventListener(ctx, jobQueue, listener)
	default:
		err = fmt.Errorf("listenerType %s is not supported", listenerType)
	}
	return err
}

func (mj *KubeMPIJob) addJobEventListener(ctx context.Context, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	if jobQueue == nil || listener == nil {
		return fmt.Errorf("add job event listener failed, err: listener is nil")
	}
	mj.jobQueue = jobQueue
	informer := listener.(cache.SharedIndexInformer)
	informer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kuberuntime.ResponsibleForJob,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    mj.addJob,
			UpdateFunc: mj.updateJob,
			DeleteFunc: mj.deleteJob,
		},
	})
	return nil
}

func (mj *KubeMPIJob) addJob(obj interface{}) {
	jobSyncInfo, err := kuberuntime.JobAddFunc(obj, mj.JobStatus)
	if err != nil {
		log.Errorf("add job failed, err: %v", err)
		return
	}
	mj.jobQueue.Add(jobSyncInfo)
}

func (mj *KubeMPIJob) updateJob(old, new interface{}) {
	jobSyncInfo, err := kuberuntime.JobUpdateFunc(old, new, mj.JobStatus)
	if err != nil {
		log.Errorf("update job failed, err: %v", err)
		return
	}
	mj.jobQueue.Add(jobSyncInfo)
}

func (mj *KubeMPIJob) deleteJob(obj interface{}) {
	jobSyncInfo, err := kuberuntime.JobDeleteFunc(obj, mj.JobStatus)
	if err != nil {
		log.Errorf("delete job failed, err: %v", err)
		return
	}
	mj.jobQueue.Add(jobSyncInfo)
}

// JobStatus get the statusInfo of mpi job, including origin status, pf status and message
func (mj *KubeMPIJob) JobStatus(obj interface{}) (api.StatusInfo, error) {
	unObj := obj.(*unstructured.Unstructured)
	// convert to MPIJob struct
	job := &mpiv1.MPIJob{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, job); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s job failed. error: %s", obj, mj.GVK.String(), err)
		return api.StatusInfo{}, err
	}
	// convert job status
	condLen := len(job.Status.Conditions)
	var jobCond kubeflowv1.JobCondition
	if condLen >= 1 {
		jobCond = job.Status.Conditions[condLen-1]
	}
	state, msg, err := mj.getJobStatus(jobCond)
	if err != nil {
		log.Errorf("get mpi status failed, err: %v", err)
		return api.StatusInfo{}, err
	}
	log.Infof("mpi job status: %s", state)
	return api.StatusInfo{
		OriginStatus: string(jobCond.Type),
		Status:       state,
		Message:      msg,
	}, nil
}

func (mj *KubeMPIJob) getJobStatus(jobCond kubeflowv1.JobCondition) (pfschema.JobStatus, string, error) {
	return kuberuntime.GetKubeflowJobStatus(jobCond)
}
