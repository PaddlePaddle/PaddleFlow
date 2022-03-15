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

func (paddleJob *PaddleJob) validateJob() error {
	if err := paddleJob.KubeJob.validateJob(); err != nil {
		return err
	}
	if len(paddleJob.JobMode) == 0 {
		// patch default value
		paddleJob.JobMode = schema.EnvJobModeCollective
	}
	var err error
	switch paddleJob.JobMode {
	case schema.EnvJobModePS:
		err = paddleJob.validatePSMode()
	case schema.EnvJobModeCollective:
		err = paddleJob.validateCollectiveMode()
	default:
		return errors.InvalidJobModeError(paddleJob.JobMode)
	}
	return err
}

func (paddleJob *PaddleJob) CreateJob() (string, error) {
	if err := paddleJob.validateJob(); err != nil {
		log.Errorf("validate job fs failed, err %v", err)
		return "", err
	}
	jobID := paddleJob.GetID()
	log.Debugf("begin create job jobID:[%s]", jobID)

	pdj := &paddlev1.PaddleJob{}
	if err := paddleJob.createJobFromYaml(pdj); err != nil {
		log.Errorf("create job failed, err %v", err)
		return "", err
	}

	if err := paddleJob.patchPaddleJobVariable(pdj, jobID); err != nil {
		log.Errorf("patch for paddleJob[%s] failed, err %v", pdj.Name, err)
		return "", err
	}
	log.Debugf("begin submit job jobID:[%s], jobApp:[%v]", jobID, pdj)
	err := Create(pdj, k8s.PaddleJobGVK, paddleJob.DynamicClientOption)
	if err != nil {
		log.Errorf("create job %v failed, err %v", jobID, err)
		return "", err
	}
	return jobID, nil
}

// patchPaddleJobVariable
//  - schedulerName: volcano will be specified in worker when gan-scheduling is required
func (paddleJob *PaddleJob) patchPaddleJobVariable(pdj *paddlev1.PaddleJob, jobID string) error {
	// metadata.Name
	pdj.Name = jobID
	// metadata.Namespace
	pdj.Namespace = paddleJob.Namespace
	// metadata.Labels
	if pdj.Labels == nil {
		pdj.Labels = map[string]string{}
	}
	pdj.Labels[schema.JobOwnerLabel] = schema.JobOwnerValue
	pdj.Labels[schema.JobIDLabel] = jobID

	// .Spec
	// .Spec.SchedulingPolicy
	if pdj.Spec.SchedulingPolicy == nil {
		pdj.Spec.SchedulingPolicy = &paddlev1.SchedulingPolicy{}
	}
	if len(pdj.Spec.SchedulingPolicy.Queue) == 0 {
		pdj.Spec.SchedulingPolicy.Queue = paddleJob.QueueName
	}
	if len(pdj.Spec.SchedulingPolicy.PriorityClass) == 0 {
		pdj.Spec.SchedulingPolicy.PriorityClass = paddleJob.getPriorityClass()
	}
	if pdj.Spec.SchedulingPolicy.MinAvailable == nil {
		// todo study for paddlejob's minAvailable
		var minAvailable int32 = 1
		pdj.Spec.SchedulingPolicy.MinAvailable = &minAvailable
	}
	// WithGloo indicate whether enable gloo, 0/1/2 for disable/enable for worker/enable for server, default 1
	var withGloo int = 1
	if pdj.Spec.WithGloo == nil {
		pdj.Spec.WithGloo = &withGloo
	}
	// policy will clean pods only on job completed
	if len(pdj.Spec.CleanPodPolicy) == 0 {
		pdj.Spec.CleanPodPolicy = paddlev1.CleanOnCompletion
	}
	// way of communicate between pods, choose one in Service/PodIP
	if len(pdj.Spec.Intranet) == 0 {
		pdj.Spec.Intranet = paddlev1.PodIP
	}

	var err error
	switch paddleJob.JobMode {
	case schema.EnvJobModePS:
		err = paddleJob.patchPdjPsSpec(pdj)
	case schema.EnvJobModeCollective:
		err = paddleJob.patchPdjCollectiveSpec(pdj)
	default:
		err = errors.InvalidJobModeError(paddleJob.JobMode)
	}
	if err != nil {
		log.Errorf("patchVCJobVariable failed, err=[%v]", err)
		return err
	}

	return nil
}

func (paddleJob *PaddleJob) StopJobByID(jobID string) error {
	job, err := models.GetJobByID(jobID)
	if err != nil {
		return err
	}
	namespace := job.Config.GetNamespace()

	if err = Delete(namespace, job.ID, k8s.PaddleJobGVK, paddleJob.DynamicClientOption); err != nil {
		log.Errorf("stop paddleJob %s in namespace %s failed, err %v", job.ID, namespace, err)
		return err
	}
	return nil
}

func (paddleJob *PaddleJob) ValidateConf() error {
	var err error
	if len(paddleJob.JobMode) == 0 {
		paddleJob.JobMode = schema.EnvJobModePS
	}
	switch paddleJob.JobMode {
	case schema.EnvJobModePS:
		err = paddleJob.validatePSMode()
	case schema.EnvJobModeCollective:
		err = paddleJob.validateCollectiveMode()
	default:
		return errors.InvalidJobModeError(paddleJob.JobMode)
	}
	return err
}

// patchPdjPsSpec fill paddleJob spec in ps mode
func (paddleJob *PaddleJob) patchPdjPsSpec(pdj *paddlev1.PaddleJob) error {
	// ps and worker
	if pdj.Spec.PS == nil || pdj.Spec.Worker == nil {
		return fmt.Errorf("paddlejob[%s] must be contain ps and worker, actually exist null", pdj.Name)
	}
	// ps master
	ps := pdj.Spec.PS
	if err := paddleJob.patchPdjTask(ps, true, pdj.Name); err != nil {
		log.Errorf("fill Task[%s] in PS-Mode failed, err=[%v]", ps.Template.Name, err)
		return err
	}
	// worker
	worker := pdj.Spec.Worker
	if err := paddleJob.patchPdjTask(worker, false, pdj.Name); err != nil {
		log.Errorf("fill Task[%s] in PS-Mode failed, err=[%v]", pdj.Name, err)
		return err
	}
	// MinAvailable
	if pdj.Spec.SchedulingPolicy.MinAvailable == nil {
		minAvailable := int32(ps.Replicas + worker.Replicas)
		pdj.Spec.SchedulingPolicy.MinAvailable = &minAvailable
	}

	paddleJob.patchMinResource(pdj)

	return nil
}

// patchPdjCollectiveSpec fill paddleJob spec in collective mode
func (paddleJob *PaddleJob) patchPdjCollectiveSpec(pdj *paddlev1.PaddleJob) error {
	 // set ps nil at first
	pdj.Spec.PS = nil

	worker := pdj.Spec.Worker
	if err := paddleJob.patchPdjTask(worker, false, pdj.Name); err != nil {
		log.Errorf("fill Task[%s] in collective-Mode failed, err=[%v]", pdj.Name, err)
		return err
	}
	// MinAvailable
	//minAvailable := int32(defaultCollectiveReplicas)
	if pdj.Spec.SchedulingPolicy.MinAvailable == nil {
		minAvailable := int32(worker.Replicas)
		pdj.Spec.SchedulingPolicy.MinAvailable = &minAvailable
	}

	paddleJob.patchMinResource(pdj)

	return nil
}

// patchPdjTask patches info into task of paddleJob
func (paddleJob *PaddleJob) patchPdjTask(resourceSpec *paddlev1.ResourceSpec, isMaster bool, jobName string) error {
	replicaStr, commandEnv, flavourStr := paddleJob.patchTaskParams(isMaster)

	if !isMaster {
		// paddleJob worker should be specified schedulerName
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
	resourceSpec.Template.Labels[schema.JobIDLabel] = jobName

	// patch Task.Template.Spec.Containers[0]
	if len(resourceSpec.Template.Spec.Containers) != 1 {
		resourceSpec.Template.Spec.Containers = []v1.Container{{}}
	}
	paddleJob.fillContainerInTask(&resourceSpec.Template.Spec.Containers[0], flavourStr, commandEnv)

	// patch resourceSpec.Template.Spec.Volumes
	resourceSpec.Template.Spec.Volumes = paddleJob.appendVolumeIfAbsent(resourceSpec.Template.Spec.Volumes, paddleJob.generateVolume(paddleJob.PVCName))

	return nil
}

// patchMinResource calculate and update minResource of paddleJob
// - when minResource is not Null, passed
// - when ps mode for job, calculate resource for both ps and worker
// - when collective mode for job, calculate resource for worker
func (paddleJob *PaddleJob) patchMinResource(pdj *paddlev1.PaddleJob) {
	if len(pdj.Spec.SchedulingPolicy.MinResources) != 0 {
		return
	}
	workerRequests := pdj.Spec.Worker.Template.Spec.Containers[0].Resources.Requests
	workerReplicas := pdj.Spec.Worker.Replicas
	workerResource := k8s.NewResource(workerRequests)
	workerResource.Multi(float64(workerReplicas))

	if pdj.Spec.PS != nil {
		psRequests := pdj.Spec.PS.Template.Spec.Containers[0].Resources.Requests
		psReplicas := pdj.Spec.PS.Replicas
		psResource := k8s.NewResource(psRequests)
		psResource.Multi(float64(psReplicas))
		workerResource.Add(psResource)
	}

	minResourceList := k8s.NewResourceList(workerResource)
	pdj.Spec.SchedulingPolicy.MinResources = minResourceList
	log.Infof("paddleJob MinResource=%s", workerResource.String())
}
