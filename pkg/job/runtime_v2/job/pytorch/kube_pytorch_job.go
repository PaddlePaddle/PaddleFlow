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

package pytorch

import (
	"context"
	"fmt"

	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	pytorchv1 "github.com/kubeflow/training-operator/pkg/apis/pytorch/v1"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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
	JobGVK               = k8s.PyTorchJobGVK
	KubePyTorchFwVersion = client.KubeFrameworkVersion(JobGVK)
)

// KubePyTorchJob is a struct that runs a pytorch job
type KubePyTorchJob struct {
	kuberuntime.KubeBaseJob
}

func New(kubeClient framework.RuntimeClientInterface) framework.JobInterface {
	return &KubePyTorchJob{
		KubeBaseJob: kuberuntime.NewKubeBaseJob(JobGVK, KubePyTorchFwVersion, kubeClient),
	}
}

func (pj *KubePyTorchJob) Submit(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	pdj := &pytorchv1.PyTorchJob{}
	if err := kuberuntime.CreateKubeJobFromYaml(pdj, pj.GVK, job); err != nil {
		log.Errorf("create %s failed, err %v", pj.String(jobName), err)
		return err
	}

	var err error
	// set metadata field
	kuberuntime.BuildJobMetadata(&pdj.ObjectMeta, job)
	// set spec field
	if job.IsCustomYaml {
		// set custom PyTorchJob Spec from user
		err = pj.customPyTorchJobSpec(&pdj.Spec, job)
	} else {
		// set builtin PyTorchJob Spec
		err = pj.builtinPyTorchJobSpec(&pdj.Spec, job)
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", pj.String(jobName), err)
		return err
	}
	log.Debugf("begin to create %s, job info: %v", pj.String(jobName), pdj)
	err = pj.RuntimeClient.Create(pdj, pj.FrameworkVersion)
	if err != nil {
		log.Errorf("create %s failed, err %v", pj.String(jobName), err)
		return err
	}
	return nil
}

// builtinPyTorchJobSpec set build-in PyTorchJob spec
func (pj *KubePyTorchJob) builtinPyTorchJobSpec(torchJobSpec *pytorchv1.PyTorchJobSpec, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Debugf("patch %s spec:%#v", pj.String(jobName), torchJobSpec)
	// TODO: set ElasticPolicy for PyTorchJob
	// set PyTorchReplicaSpecs
	minResources := resources.EmptyResource()
	for _, task := range job.Tasks {
		replicaType := pytorchv1.PyTorchReplicaTypeMaster
		if task.Role == pfschema.RoleWorker || task.Role == pfschema.RolePWorker {
			// pytorch worker
			replicaType = pytorchv1.PyTorchReplicaTypeWorker
		}
		replicaSpec, ok := torchJobSpec.PyTorchReplicaSpecs[replicaType]
		if !ok {
			return fmt.Errorf("replica type %s for %s is not supported", replicaType, pj.String(jobName))
		}
		if err := kuberuntime.KubeflowReplicaSpec(replicaSpec, job.ID, &task); err != nil {
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
	return kuberuntime.KubeflowRunPolicy(&torchJobSpec.RunPolicy, &resourceList, job.Conf.GetQueueName(), job.Conf.GetPriority())
}

// customPyTorchJobSpec set custom PyTorchJob Spec
func (pj *KubePyTorchJob) customPyTorchJobSpec(torchJobSpec *pytorchv1.PyTorchJobSpec, job *api.PFJob) error {
	if job == nil || torchJobSpec == nil {
		return fmt.Errorf("job or torchJobSpec is nil")
	}
	jobName := job.NamespacedName()
	log.Debugf("patch %s spec:%#v", pj.String(jobName), torchJobSpec)
	// patch metadata
	ps, find := torchJobSpec.PyTorchReplicaSpecs[pytorchv1.PyTorchReplicaTypeMaster]
	if find && ps != nil {
		kuberuntime.BuildTaskMetadata(&ps.Template.ObjectMeta, job.ID, &pfschema.Conf{})
	}
	worker, find := torchJobSpec.PyTorchReplicaSpecs[pytorchv1.PyTorchReplicaTypeWorker]
	if find && worker != nil {
		kuberuntime.BuildTaskMetadata(&worker.Template.ObjectMeta, job.ID, &pfschema.Conf{})
	}
	// TODO: patch pytorch job from user
	// check RunPolicy
	return kuberuntime.KubeflowRunPolicy(&torchJobSpec.RunPolicy, nil, job.Conf.GetQueueName(), job.Conf.GetPriority())
}

func (pj *KubePyTorchJob) AddEventListener(ctx context.Context, listenerType string, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeJob:
		err = pj.AddJobEventListener(ctx, jobQueue, listener, pj.JobStatus, nil)
	default:
		err = fmt.Errorf("listenerType %s is not supported", listenerType)
	}
	return err
}

// JobStatus get the statusInfo of PyTorch job, including origin status, pf status and message
func (pj *KubePyTorchJob) JobStatus(obj interface{}) (api.StatusInfo, error) {
	unObj := obj.(*unstructured.Unstructured)
	// convert to PyTorchJob struct
	job := &pytorchv1.PyTorchJob{}
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
		log.Errorf("get pytorch status failed, err: %v", err)
		return api.StatusInfo{}, err
	}
	log.Infof("pytorch job status: %s", state)
	return api.StatusInfo{
		OriginStatus: string(jobCond.Type),
		Status:       state,
		Message:      msg,
	}, nil
}

func (pj *KubePyTorchJob) getJobStatus(jobCond kubeflowv1.JobCondition) (pfschema.JobStatus, string, error) {
	return kuberuntime.GetKubeflowJobStatus(jobCond)
}
