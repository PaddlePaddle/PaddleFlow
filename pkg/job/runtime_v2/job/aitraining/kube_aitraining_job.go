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

package aitraining

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apis/aitj-operator/v1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/util/kuberuntime"
)

// KubeAITrainingJob is an executor struct that runs a aitj job
type KubeAITrainingJob struct {
	kuberuntime.KubeBaseJob
}

func New(kubeClient framework.RuntimeClientInterface) framework.JobInterface {
	return &KubeAITrainingJob{
		KubeBaseJob: kuberuntime.NewKubeBaseJob(pfschema.AITrainingKindGroupVersion, kubeClient),
	}
}

func (pj *KubeAITrainingJob) Submit(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Debugf("begin to create %s", pj.String(jobName))

	pdj := &v1.AITrainingJob{}
	err := kuberuntime.CreateKubeJobFromYaml(pdj, pj.KindGroupVersion, job)
	if err != nil {
		log.Errorf("create %s failed, err %v", pj.String(jobName), err)
		return err
	}

	// set metadata field
	kuberuntime.BuildJobMetadata(&pdj.ObjectMeta, job)
	// set scheduling policy for AITraining job
	if err := pj.buildSchedulingPolicy(&pdj.Spec, job); err != nil {
		log.Errorf("build scheduling policy for %s failed, err: %v", pj.String(jobName), err)
		return err
	}

	// build job spec field
	if job.IsCustomYaml {
		// set custom AITrainingJob Spec from user
		err = pj.customAITrainingJob(&pdj.Spec, job)
	} else {
		// set builtin AITrainingJob Spec
		err = pj.builtinAITrainingJob(&pdj.Spec, job)
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", pj.String(jobName), err)
		return err
	}
	log.Debugf("begin to create %s, ai training job info: %v", pj.String(jobName), pdj)
	err = pj.RuntimeClient.Create(pdj, pj.KindGroupVersion)
	if err != nil {
		log.Errorf("create %s failed, err %v", pj.String(jobName), err)
		return err
	}
	return nil
}

func (pj *KubeAITrainingJob) buildSchedulingPolicy(pdj *v1.TrainingJobSpec, job *api.PFJob) error {
	if pdj == nil || job == nil {
		return fmt.Errorf("AITrainingJob is nil")
	}
	log.Infof("build scheduling policy for AITrainingJob")
	// patch priority
	if job.Conf.GetPriority() != "" {
		pdj.Priority = kuberuntime.KubePriorityClass(job.Conf.GetPriority())
	}
	// patch scheduler
	pdj.SchedulerName = config.GlobalServerConfig.Job.SchedulerName
	return nil
}

func (pj *KubeAITrainingJob) customAITrainingJob(pdj *v1.TrainingJobSpec, job *api.PFJob) error {
	jobName := job.NamespacedName()
	log.Infof("custom AITrainingJob %s fill resource", pj.String(jobName))
	// patch resources and fs
	for _, task := range job.Tasks {
		// patch worker
		var err error
		switch task.Role {
		case pfschema.RoleWorker:
			// patch worker
			rs := pdj.ReplicaSpecs[v1.ReplicaName("trainer")]
			if rs == nil || rs.ReplicaType != v1.ReplicaWorker {
				continue
			}
			err = pj.patchReplicaSpec(rs, task, job.ID)
		default:
			err = fmt.Errorf("role %s is not supported", task.Role)
		}
		if err != nil {
			log.Errorf("build task for paddle job with role %s failed, err: %v", task.Role, err)
			return err
		}
	}
	return nil
}

func (pj *KubeAITrainingJob) patchReplicaSpec(rs *v1.ReplicaSpec, task pfschema.Member, jobID string) error {
	log.Debugf("patch replica spec for %s task", pj.String(task.Name))
	// patch replica
	if rs.Replicas == nil || *rs.Replicas < kuberuntime.DefaultReplicas {
		var replicas int32 = kuberuntime.DefaultReplicas
		if task.Replicas > 0 {
			replicas = int32(task.Replicas)
		}
		rs.Replicas = &replicas
	}
	// patch fs
	kuberuntime.NewPodTemplateSpecBuilder(&rs.Template, jobID).Build(task)
	return nil
}

func (pj *KubeAITrainingJob) builtinAITrainingJob(pdj *v1.TrainingJobSpec, job *api.PFJob) error {
	return pj.customAITrainingJob(pdj, job)
}

func (pj *KubeAITrainingJob) AddEventListener(ctx context.Context, listenerType string, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeJob:
		err = pj.AddJobEventListener(ctx, jobQueue, listener, pj.JobStatus, nil)
	default:
		err = fmt.Errorf("listenerType %s is not supported", listenerType)
	}
	return err
}

// JobStatus get the statusInfo of paddle job, including origin status, pf status and message
func (pj *KubeAITrainingJob) JobStatus(obj interface{}) (api.StatusInfo, error) {
	unObj := obj.(*unstructured.Unstructured)
	log.Debugf(`get unstructured of job [%#v]`, unObj.Object)
	// convert to AITrainingJob struct
	job := &v1.AITrainingJob{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, job); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s job failed. error: %s", obj, pj.KindGroupVersion, err)
		return api.StatusInfo{}, err
	}
	// convert job status
	state, msg, err := pj.getJobStatus(&job.Status)
	if err != nil {
		log.Errorf("get AITrainingJob status failed, err: %v", err)
		return api.StatusInfo{}, err
	}
	log.Infof("AITraining job status: %s", state)
	return api.StatusInfo{
		OriginStatus: string(job.Status.Phase),
		Status:       state,
		Message:      msg,
		StartTime:    kuberuntime.GetKubeTime(job.Status.StartRunningTime),
		FinishedTime: kuberuntime.GetKubeTime(job.Status.EndTime),
	}, nil
}

func (pj *KubeAITrainingJob) getJobStatus(jobStatus *v1.TrainingJobStatus) (pfschema.JobStatus, string, error) {
	status := pfschema.JobStatus("")
	msg := ""
	if jobStatus == nil {
		return status, msg, fmt.Errorf("job status is nil")
	}
	switch jobStatus.Phase {
	case v1.TrainingJobPhaseNone, v1.TrainingJobPhasePending, v1.TrainingJobPhaseCreating:
		status = pfschema.StatusJobPending
		msg = "ai training job is pending"
	case v1.TrainingJobPhaseRestarting, v1.TrainingJobPhaseRunning, v1.TrainingJobPhaseScaling, v1.TrainingJobPhaseTerminating:
		status = pfschema.StatusJobRunning
		msg = "ai training job is running"
	case v1.TrainingJobPhaseSucceeded:
		status = pfschema.StatusJobSucceeded
		msg = "ai training job is succeeded"
	case v1.TrainingJobPhaseFailed, v1.TrainingJobPhaseNodeFail, v1.TrainingJobPhasePreempted, v1.TrainingJobPhaseTimeout:
		status = pfschema.StatusJobFailed
		msg = "ai training job is failed"
	default:
		return status, msg, fmt.Errorf("unexpected ai training job status: %s", jobStatus.Phase)
	}
	return status, msg, nil
}
