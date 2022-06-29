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

package executor

import (
	"fmt"

	paddlev1 "github.com/paddleflow/paddle-operator/api/v1"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
)

type PaddleJob struct {
	KubeJob
	JobModeParams
}

func (pj *PaddleJob) validateJob() error {
	if err := pj.KubeJob.validateJob(); err != nil {
		return err
	}
	if len(pj.JobMode) == 0 && !pj.IsCustomYaml {
		// patch default value
		pj.JobMode = schema.EnvJobModeCollective
	}

	return nil
}

func (pj *PaddleJob) CreateJob() (string, error) {
	pdj := &paddlev1.PaddleJob{}
	if err := pj.createJobFromYaml(pdj); err != nil {
		log.Errorf("create job[%s] failed, err %v", pj.ID, err)
		return "", err
	}
	if err := pj.validateJob(); err != nil {
		log.Errorf("validate [%s]type job[%s] failed, err %v", pj.JobType, pj.ID, err)
		return "", err
	}
	if err := pj.validateJob(); err != nil {
		log.Errorf("validate %s job failed, err %v", pj.JobType, err)
		return "", err
	}

	var err error
	// patch .metadata field
	pj.patchMetadata(&pdj.ObjectMeta, pj.ID)
	// patch .spec field
	if err = pj.patchPaddleJobSpec(&pdj.Spec); err != nil {
		log.Errorf("build job spec failed, err %v", err)
		return "", err
	}

	// create job on cluster
	log.Infof("create %s job %s/%s on cluster", pj.JobType, pj.Namespace, pj.Name)
	if err = Create(pdj, pj.GroupVersionKind, pj.DynamicClientOption); err != nil {
		log.Errorf("create %s job %s/%s on cluster failed, err %v", pj.JobType, pj.Namespace, pj.Name, err)
		return "", err
	}
	return pj.ID, err
}

// patchPaddleJobSpec
//  - schedulerName: volcano will be specified in worker when gan-scheduling is required
func (pj *PaddleJob) patchPaddleJobSpec(pdjSpec *paddlev1.PaddleJobSpec) error {
	log.Debugf("patch %s job %s/%s spec:%#v", pj.JobType, pj.Namespace, pj.Name, pdjSpec)
	// .Spec.SchedulingPolicy
	if pdjSpec.SchedulingPolicy == nil {
		pdjSpec.SchedulingPolicy = &paddlev1.SchedulingPolicy{}
	}
	if len(pdjSpec.SchedulingPolicy.Queue) == 0 {
		pdjSpec.SchedulingPolicy.Queue = pj.QueueName
	}
	if len(pdjSpec.SchedulingPolicy.PriorityClass) == 0 {
		pdjSpec.SchedulingPolicy.PriorityClass = pj.getPriorityClass()
	}
	if pdjSpec.SchedulingPolicy.MinAvailable == nil {
		// todo study for paddlejob's minAvailable
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
		pdjSpec.CleanPodPolicy = paddlev1.CleanOnCompletion
	}
	// way of communicate between pods, choose one in Service/PodIP
	if len(pdjSpec.Intranet) == 0 {
		pdjSpec.Intranet = paddlev1.PodIP
	}

	if pj.IsCustomYaml {
		log.Infof("%s job %s/%s using custom yaml, pass the patch from tasks", pj.JobType, pj.Namespace, pj.Name)
		return nil
	}

	var err error
	switch pj.JobMode {
	case schema.EnvJobModePS:
		err = pj.patchPdjPsSpec(pdjSpec)
	case schema.EnvJobModeCollective:
		err = pj.patchPdjCollectiveSpec(pdjSpec)
	default:
		err = errors.InvalidJobModeError(pj.JobMode)
	}
	if err != nil {
		log.Errorf("patch paddleJobVariable failed, err=[%v]", err)
		return err
	}
	return nil
}

func (pj *PaddleJob) StopJobByID(jobID string) error {
	job, err := models.GetJobByID(jobID)
	if err != nil {
		return err
	}
	namespace := job.Config.GetNamespace()

	if err = Delete(namespace, job.ID, k8s.PaddleJobGVK, pj.DynamicClientOption); err != nil {
		log.Errorf("stop paddleJob %s in namespace %s failed, err %v", job.ID, namespace, err)
		return err
	}
	return nil
}

// patchPdjPsSpec fill paddleJob spec in ps mode
func (pj *PaddleJob) patchPdjPsSpec(pdjSpec *paddlev1.PaddleJobSpec) error {
	// ps and worker
	if pdjSpec.PS == nil || pdjSpec.Worker == nil {
		return fmt.Errorf("paddlejob[%s] must be contain ps and worker, actually exist null", pj.Name)
	}
	for _, task := range pj.Tasks {
		if task.Role != schema.RoleWorker && task.Role != schema.RolePWorker {
			// ps master
			pdjSpec.PS.Template.Spec.SchedulerName = config.GlobalServerConfig.Job.SchedulerName
			if err := pj.patchPdjTask(pdjSpec.PS, task); err != nil {
				log.Errorf("fill Task[%s] in PS-Mode failed, err=[%v]", pj.Name, err)
				return err
			}
		} else {
			// worker
			if err := pj.patchPdjTask(pdjSpec.Worker, task); err != nil {
				log.Errorf("fill Task[%s] in PS-Mode failed, err=[%v]", pj.Name, err)
				return err
			}
		}
	}

	// MinAvailable
	if pdjSpec.SchedulingPolicy.MinAvailable == nil {
		minAvailable := int32(pdjSpec.PS.Replicas + pdjSpec.Worker.Replicas)
		pdjSpec.SchedulingPolicy.MinAvailable = &minAvailable
	}

	pj.patchMinResource(pdjSpec)
	return nil
}

// patchPdjCollectiveSpec fill paddleJob spec in collective mode
func (pj *PaddleJob) patchPdjCollectiveSpec(pdjSpec *paddlev1.PaddleJobSpec) error {
	// set ps nil at first
	pdjSpec.PS = nil

	worker := pdjSpec.Worker
	for _, task := range pj.Tasks {
		if task.Role != schema.RoleWorker && task.Role != schema.RolePWorker {
			return fmt.Errorf("paddlejob[%s] must be contain worker, task: %#v", pj.ID, task)
		}
		// patch collective worker
		if err := pj.patchPdjTask(worker, task); err != nil {
			log.Errorf("fill Task[%s] in PS-Mode failed, err=[%v]", pj.ID, err)
			return err
		}
	}
	log.Infof("worker=%#v, pj.Tasks=%#v", worker, pj.Tasks)
	// MinAvailable
	// minAvailable := int32(defaultCollectiveReplicas)
	if pdjSpec.SchedulingPolicy.MinAvailable == nil {
		minAvailable := int32(worker.Replicas)
		pdjSpec.SchedulingPolicy.MinAvailable = &minAvailable
	}

	pj.patchMinResource(pdjSpec)
	return nil
}

// patchPdjTask patches info into task of paddleJob
func (pj *PaddleJob) patchPdjTask(resourceSpec *paddlev1.ResourceSpec, task models.Member) error {
	log.Infof("patchPdjTask, resourceSpec=%#v, task=%#v", resourceSpec, task)
	if !pj.IsCustomYaml && task.Replicas > 0 {
		resourceSpec.Replicas = task.Replicas
	}
	if resourceSpec.Replicas <= 0 {
		resourceSpec.Replicas = defaultPSReplicas
	}

	// set metadata
	taskName := task.Name
	if taskName == "" {
		taskName = uuid.GenerateIDWithLength(pj.ID, 3)
	}
	pj.patchMetadata(&resourceSpec.Template.ObjectMeta, taskName)
	// set pod template
	pj.fillPodSpec(&resourceSpec.Template.Spec, &task)

	// patch Task.Template.Spec.Containers[0]
	if len(resourceSpec.Template.Spec.Containers) != 1 {
		resourceSpec.Template.Spec.Containers = []v1.Container{{}}
	}
	pj.fillContainerInTasks(&resourceSpec.Template.Spec.Containers[0], task)
	// append into container.VolumeMounts
	taskFs := task.Conf.GetAllFileSystem()
	resourceSpec.Template.Spec.Volumes = appendVolumesIfAbsent(resourceSpec.Template.Spec.Volumes, generateVolumes(taskFs))
	return nil
}

// patchMinResource calculate and update minResource of paddleJob
// - when minResource is not Null, passed
// - when ps mode for job, calculate resource for both ps and worker
// - when collective mode for job, calculate resource for worker
func (pj *PaddleJob) patchMinResource(pdjSpec *paddlev1.PaddleJobSpec) {
	log.Infof("patchMinResource for paddlejob[%s], pdjSpec=[%v]", pj.Name, pdjSpec)
	if len(pdjSpec.SchedulingPolicy.MinResources) != 0 {
		return
	}
	workerRequests := pdjSpec.Worker.Template.Spec.Containers[0].Resources.Requests
	workerReplicas := pdjSpec.Worker.Replicas
	workerResource := k8s.NewResource(workerRequests)
	workerResource.Multi(float64(workerReplicas))

	if pdjSpec.PS != nil {
		psRequests := pdjSpec.PS.Template.Spec.Containers[0].Resources.Requests
		psReplicas := pdjSpec.PS.Replicas
		psResource := k8s.NewResource(psRequests)
		psResource.Multi(float64(psReplicas))
		workerResource.Add(psResource)
	}

	minResourceList := k8s.NewResourceList(workerResource)
	pdjSpec.SchedulingPolicy.MinResources = minResourceList
	log.Infof("paddleJob MinResource=%s", workerResource.String())
}
