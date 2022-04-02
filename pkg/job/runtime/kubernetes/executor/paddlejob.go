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
	"strconv"

	paddlev1 "github.com/paddleflow/paddle-operator/api/v1"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/errors"
	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/common/schema"
)

type PaddleJob struct {
	KubeJob
	JobModeParams
}

func (pj *PaddleJob) validateJob() error {
	if err := pj.KubeJob.validateJob(); err != nil {
		return err
	}
	if len(pj.JobMode) == 0 {
		// patch default value
		pj.JobMode = schema.EnvJobModeCollective
	}
	var err error
	switch pj.JobMode {
	case schema.EnvJobModePS:
		err = pj.validatePSMode()
	case schema.EnvJobModeCollective:
		err = pj.validateCollectiveMode()
	default:
		return errors.InvalidJobModeError(pj.JobMode)
	}
	return err
}

func (pj *PaddleJob) CreateJob() (string, error) {
	if err := pj.validateJob(); err != nil {
		log.Errorf("validate %s job failed, err %v", pj.JobType, err)
		return "", err
	}
	pdj := &paddlev1.PaddleJob{}
	if err := pj.createJobFromYaml(pdj); err != nil {
		log.Errorf("create job failed, err %v", err)
		return "", err
	}

	// patch .metadata field
	pj.patchMetadata(&pdj.ObjectMeta)
	// patch .spec field
	err := pj.patchPaddleJobSpec(&pdj.Spec)
	if err != nil {
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
		log.Errorf("patchVCJobVariable failed, err=[%v]", err)
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
	// ps master
	ps := pdjSpec.PS
	if err := pj.patchPdjTask(ps, true); err != nil {
		log.Errorf("fill Task[%s] in PS-Mode failed, err=[%v]", ps.Template.Name, err)
		return err
	}
	// worker
	worker := pdjSpec.Worker
	if err := pj.patchPdjTask(worker, false); err != nil {
		log.Errorf("fill Task[%s] in PS-Mode failed, err=[%v]", pj.Name, err)
		return err
	}
	// MinAvailable
	if pdjSpec.SchedulingPolicy.MinAvailable == nil {
		minAvailable := int32(ps.Replicas + worker.Replicas)
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
	if err := pj.patchPdjTask(worker, false); err != nil {
		log.Errorf("fill Task[%s] in collective-Mode failed, err=[%v]", pj.Name, err)
		return err
	}
	// MinAvailable
	//minAvailable := int32(defaultCollectiveReplicas)
	if pdjSpec.SchedulingPolicy.MinAvailable == nil {
		minAvailable := int32(worker.Replicas)
		pdjSpec.SchedulingPolicy.MinAvailable = &minAvailable
	}

	pj.patchMinResource(pdjSpec)
	return nil
}

// patchPdjTask patches info into task of paddleJob
func (pj *PaddleJob) patchPdjTask(resourceSpec *paddlev1.ResourceSpec, isMaster bool) error {
	replicaStr, commandEnv, flavourStr := pj.patchTaskParams(isMaster)

	if !isMaster {
		// pj worker should be specified schedulerName
		resourceSpec.Template.Spec.SchedulerName = config.GlobalServerConfig.Job.SchedulerName
	}

	if commandEnv == "" {
		return fmt.Errorf("the env [command] is required")
	}

	if replicaStr != "" {
		replicasInt, _ := strconv.Atoi(replicaStr)
		resourceSpec.Replicas = replicasInt
	}
	if resourceSpec.Replicas <= 0 {
		resourceSpec.Replicas = defaultPSReplicas
	}

	// patch resourceSpec.Template.Labels
	if resourceSpec.Template.Labels == nil {
		resourceSpec.Template.Labels = map[string]string{}
	}
	resourceSpec.Template.Labels[schema.JobIDLabel] = pj.Name

	// patch Task.Template.Spec.Containers[0]
	if len(resourceSpec.Template.Spec.Containers) != 1 {
		resourceSpec.Template.Spec.Containers = []v1.Container{{}}
	}
	pj.fillContainerInTask(&resourceSpec.Template.Spec.Containers[0], flavourStr, commandEnv)

	// patch resourceSpec.Template.Spec.Volumes
	resourceSpec.Template.Spec.Volumes = pj.appendVolumeIfAbsent(resourceSpec.Template.Spec.Volumes, pj.generateVolume(pj.PVCName))
	return nil
}

// patchMinResource calculate and update minResource of paddleJob
// - when minResource is not Null, passed
// - when ps mode for job, calculate resource for both ps and worker
// - when collective mode for job, calculate resource for worker
func (pj *PaddleJob) patchMinResource(pdjSpec *paddlev1.PaddleJobSpec) {
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
