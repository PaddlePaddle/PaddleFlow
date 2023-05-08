/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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

	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"

	paddlev1 "github.com/PaddlePaddle/PaddleFlow/pkg/apis/training-operator/kubeflow.org/v1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/util/kuberuntime"
)

// KubeKFPaddleJob is an executor struct that runs a kubeflow paddle job
type KubeKFPaddleJob struct {
	kuberuntime.KubeBaseJob
}

func KFNew(kubeClient framework.RuntimeClientInterface) framework.JobInterface {
	return &KubeKFPaddleJob{
		KubeBaseJob: kuberuntime.NewKubeBaseJob(pfschema.KFPaddleKindGroupVersion, kubeClient),
	}
}

func (pj *KubeKFPaddleJob) Submit(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	pdj := &paddlev1.PaddleJob{}
	if err := kuberuntime.CreateKubeJobFromYaml(pdj, pj.KindGroupVersion, job); err != nil {
		log.Errorf("create %s failed, err %v", pj.String(jobName), err)
		return err
	}

	var err error
	// set metadata field
	kuberuntime.BuildJobMetadata(&pdj.ObjectMeta, job)
	// set spec field
	if job.IsCustomYaml {
		// set custom kubeflow PaddleJob Spec from user
		err = pj.customPaddleJobSpec(&pdj.Spec, job)
	} else {
		// set builtin kubeflow PaddleJob Spec
		err = pj.builtinPaddleJobSpec(&pdj.Spec, job)
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", pj.String(jobName), err)
		return err
	}
	log.Debugf("begin to create %s, job info: %v", pj.String(jobName), pdj)
	err = pj.RuntimeClient.Create(pdj, pj.KindGroupVersion)
	if err != nil {
		log.Errorf("create %s failed, err %v", pj.String(jobName), err)
		return err
	}
	return nil
}

func (pj *KubeKFPaddleJob) builtinPaddleJobSpec(jobSpec *paddlev1.PaddleJobSpec, job *api.PFJob) error {
	if job == nil || jobSpec == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Debugf("patch %s spec:%#v", pj.String(jobName), jobSpec)
	// TODO: set ElasticPolicy for PaddleJob
	// set PyTorchReplicaSpecs
	minResources := resources.EmptyResource()
	for _, task := range job.Tasks {
		replicaType := paddlev1.PaddleJobReplicaTypeMaster
		if task.Role == pfschema.RoleWorker || task.Role == pfschema.RolePWorker {
			// paddle worker
			replicaType = paddlev1.PaddleJobReplicaTypeWorker
		}
		replicaSpec, ok := jobSpec.PaddleReplicaSpecs[replicaType]
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
	return kuberuntime.KubeflowRunPolicy(&jobSpec.RunPolicy, &resourceList, job.Conf.GetQueueName(), job.Conf.GetPriority())
}

func (pj *KubeKFPaddleJob) customPaddleJobSpec(jobSpec *paddlev1.PaddleJobSpec, job *api.PFJob) error {
	if job == nil || jobSpec == nil {
		return fmt.Errorf("job or jobSpec is nil")
	}
	jobName := job.NamespacedName()
	log.Debugf("patch %s spec:%#v", pj.String(jobName), jobSpec)
	// patch metadata
	ps, find := jobSpec.PaddleReplicaSpecs[paddlev1.PaddleJobReplicaTypeMaster]
	if find && ps != nil {
		kuberuntime.BuildTaskMetadata(&ps.Template.ObjectMeta, job.ID, &pfschema.Conf{})
	}
	worker, find := jobSpec.PaddleReplicaSpecs[paddlev1.PaddleJobReplicaTypeWorker]
	if find && worker != nil {
		kuberuntime.BuildTaskMetadata(&worker.Template.ObjectMeta, job.ID, &pfschema.Conf{})
	}
	// TODO: patch paddle job from user
	// check RunPolicy
	return kuberuntime.KubeflowRunPolicy(&jobSpec.RunPolicy, nil, job.Conf.GetQueueName(), job.Conf.GetPriority())
}

func (pj *KubeKFPaddleJob) AddEventListener(ctx context.Context, listenerType string, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeJob:
		err = pj.AddJobEventListener(ctx, jobQueue, listener, pj.JobStatus, nil)
	default:
		err = fmt.Errorf("listenerType %s is not supported", listenerType)
	}
	return err
}

// JobStatus get the statusInfo of kubeflow paddle job, including origin status, pf status and message
func (pj *KubeKFPaddleJob) JobStatus(obj interface{}) (api.StatusInfo, error) {
	unObj := obj.(*unstructured.Unstructured)
	// convert to kubeflow PaddleJob struct
	job := &paddlev1.PaddleJob{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, job); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s job failed. error: %s", obj, pj.KindGroupVersion, err)
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
		log.Errorf("get kubeflow paddlejob status failed, err: %v", err)
		return api.StatusInfo{}, err
	}
	log.Infof("kubeflow paddlejob status: %s", state)
	return api.StatusInfo{
		OriginStatus: string(jobCond.Type),
		Status:       state,
		Message:      msg,
	}, nil
}

func (pj *KubeKFPaddleJob) getJobStatus(jobCond kubeflowv1.JobCondition) (pfschema.JobStatus, string, error) {
	return kuberuntime.GetKubeflowJobStatus(jobCond)
}
