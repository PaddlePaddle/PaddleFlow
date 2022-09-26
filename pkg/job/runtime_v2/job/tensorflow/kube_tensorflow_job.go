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

package tensorflow

import (
	"context"
	"fmt"

	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	tfv1 "github.com/kubeflow/training-operator/pkg/apis/tensorflow/v1"
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
	JobGVK          = k8s.TFJobGVK
	KubeTFFwVersion = client.KubeFrameworkVersion(JobGVK)
)

// KubeTFJob is a struct that runs a tensorflow job
type KubeTFJob struct {
	GVK              schema.GroupVersionKind
	frameworkVersion pfschema.FrameworkVersion
	runtimeClient    framework.RuntimeClientInterface
	jobQueue         workqueue.RateLimitingInterface
}

func New(kubeClient framework.RuntimeClientInterface) framework.JobInterface {
	return &KubeTFJob{
		runtimeClient:    kubeClient,
		GVK:              JobGVK,
		frameworkVersion: KubeTFFwVersion,
	}
}

func (pj *KubeTFJob) String(name string) string {
	return fmt.Sprintf("%s job %s on %s", pj.GVK.String(), name, pj.runtimeClient.Cluster())
}

func (pj *KubeTFJob) Submit(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	tfjob := &tfv1.TFJob{}
	if err := kuberuntime.CreateKubeJobFromYaml(tfjob, pj.GVK, job); err != nil {
		log.Errorf("create %s failed, err %v", pj.String(jobName), err)
		return err
	}

	var err error
	// set metadata field
	kuberuntime.BuildJobMetadata(&tfjob.ObjectMeta, job)
	// set spec field
	if job.IsCustomYaml {
		// set custom TFJob Spec from user
		err = pj.customTFJobSpec(&tfjob.Spec, job)
	} else {
		// set builtin TFJob Spec
		err = pj.builtinTFJobSpec(&tfjob.Spec, job)
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", pj.String(jobName), err)
		return err
	}
	log.Debugf("begin to create %s, job info: %v", pj.String(jobName), tfjob)
	err = pj.runtimeClient.Create(tfjob, pj.frameworkVersion)
	if err != nil {
		log.Errorf("create %s failed, err %v", pj.String(jobName), err)
		return err
	}
	return nil
}

// builtinTFJobSpec set build-in TFJob spec
func (pj *KubeTFJob) builtinTFJobSpec(tfJobSpec *tfv1.TFJobSpec, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Debugf("patch %s spec:%#v", pj.String(jobName), tfJobSpec)
	// TODO: set ElasticPolicy for TFJob
	// set TFReplicaSpecs
	minResources := resources.EmptyResource()
	for _, task := range job.Tasks {
		// tf parameter server for distributed training
		replicaType := tfv1.TFReplicaTypePS
		// if role is worker, set it to tf worker
		if task.Role == pfschema.RoleWorker || task.Role == pfschema.RolePWorker {
			// tf worker for distributed training
			replicaType = tfv1.TFReplicaTypeWorker
		}
		replicaSpec, ok := tfJobSpec.TFReplicaSpecs[replicaType]
		if !ok {
			return fmt.Errorf("replica type %s for %s is not supported", replicaType, pj.String(jobName))
		}
		if err := kuberuntime.KubeflowReplicaSpec(replicaSpec, &task); err != nil {
			log.Errorf("build %s RepilcaSpec for %s failed, err: %v", replicaType, pj.String(jobName), err)
			return err
		}
		// calculate job minResources
		taskResources, err := resources.NewResourceFromMap(task.Flavour.ToMap())
		if err != nil {
			log.Errorf("parse resources for %s task failed, err: %v", pj.String(jobName), err)
			return err
		}
		taskResources.Multi(task.Replicas)
		minResources.Add(taskResources)
	}
	// set RunPolicy
	resourceList := k8s.NewResourceList(minResources)
	return kuberuntime.KubeflowRunPolicy(&tfJobSpec.RunPolicy, &resourceList, job.Conf.GetQueueName(), job.Conf.GetPriority())
}

// customTFJobSpec set custom TFJob Spec
func (pj *KubeTFJob) customTFJobSpec(tfJobSpec *tfv1.TFJobSpec, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Debugf("patch %s spec:%#v", pj.String(jobName), tfJobSpec)
	// TODO: patch pytorch job from user
	// check RunPolicy
	return kuberuntime.KubeflowRunPolicy(&tfJobSpec.RunPolicy, nil, job.Conf.GetQueueName(), job.Conf.GetPriority())
}

func (pj *KubeTFJob) Stop(ctx context.Context, job *api.PFJob) error {
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

func (pj *KubeTFJob) Update(ctx context.Context, job *api.PFJob) error {
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

func (pj *KubeTFJob) Delete(ctx context.Context, job *api.PFJob) error {
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

func (pj *KubeTFJob) GetLog(ctx context.Context, jobLogRequest pfschema.JobLogRequest) (pfschema.JobLogInfo, error) {
	// TODO: add get log logic
	return pfschema.JobLogInfo{}, nil
}

func (pj *KubeTFJob) AddEventListener(ctx context.Context, listenerType string, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeJob:
		err = pj.addJobEventListener(ctx, jobQueue, listener)
	default:
		err = fmt.Errorf("listenerType %s is not supported", listenerType)
	}
	return err
}

func (pj *KubeTFJob) addJobEventListener(ctx context.Context, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
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

func (pj *KubeTFJob) addJob(obj interface{}) {
	jobSyncInfo, err := kuberuntime.JobAddFunc(obj, pj.JobStatus)
	if err != nil {
		return
	}
	pj.jobQueue.Add(jobSyncInfo)
}

func (pj *KubeTFJob) updateJob(old, new interface{}) {
	jobSyncInfo, err := kuberuntime.JobUpdateFunc(old, new, pj.JobStatus)
	if err != nil {
		return
	}
	pj.jobQueue.Add(jobSyncInfo)
}

func (pj *KubeTFJob) deleteJob(obj interface{}) {
	jobSyncInfo, err := kuberuntime.JobDeleteFunc(obj, pj.JobStatus)
	if err != nil {
		return
	}
	pj.jobQueue.Add(jobSyncInfo)
}

// JobStatus get the statusInfo of TF job, including origin status, pf status and message
func (pj *KubeTFJob) JobStatus(obj interface{}) (api.StatusInfo, error) {
	unObj := obj.(*unstructured.Unstructured)
	// convert to TFJob struct
	job := &tfv1.TFJob{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, job); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s job failed. error: %s", obj, pj.GVK.String(), err)
		return api.StatusInfo{}, err
	}
	// convert job status
	condLen := len(job.Status.Conditions)
	var jobCond kubeflowv1.JobCondition
	if condLen >= 1 {
		jobCond = job.Status.Conditions[condLen-1]
	}
	state, msg, err := pj.getJobStatus(jobCond)
	if err != nil {
		log.Errorf("get tensorflow job status failed, err: %v", err)
		return api.StatusInfo{}, err
	}
	log.Infof("tensorflow job status: %s", state)
	return api.StatusInfo{
		OriginStatus: string(jobCond.Type),
		Status:       state,
		Message:      msg,
	}, nil
}

func (pj *KubeTFJob) getJobStatus(jobCond kubeflowv1.JobCondition) (pfschema.JobStatus, string, error) {
	return kuberuntime.GetKubeflowJobStatus(jobCond)
}
