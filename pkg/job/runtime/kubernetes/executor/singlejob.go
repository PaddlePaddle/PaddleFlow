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

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	v1 "k8s.io/api/core/v1"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/common/schema"
)

// SingleJob is a executor struct that runs a single pod
type SingleJob struct {
	KubeJob
	Flavour schema.Flavour
}

func (sp *SingleJob) validateJob() error {
	if err := sp.KubeJob.validateJob(); err != nil {
		return err
	}
	if sp.KubeJob.Namespace == "" {
		return fmt.Errorf("namespace is empty")
	}
	if sp.KubeJob.Image == "" {
		return fmt.Errorf("image is empty")
	}
	if sp.KubeJob.Command == "" {
		return fmt.Errorf("command is empty")
	}
	if sp.Flavour.Name == "" {
		return fmt.Errorf("flavour name is empty")
	}

	return nil
}

// patchSinglePodVariable patch env variable to vcJob, the order of patches following vcJob crd
func (sp *SingleJob) patchSinglePodVariable(pod *v1.Pod, jobID string) error {
	// if pod's name exist, sp.Name should be overwritten
	// metadata
	sp.patchMetadata(&pod.ObjectMeta)

	if len(sp.QueueName) > 0 {
		pod.Labels[schema.QueueLabelKey] = sp.QueueName
		priorityClass := sp.getPriorityClass()
		pod.Spec.PriorityClassName = priorityClass
	}
	// fill SchedulerName
	pod.Spec.SchedulerName = config.GlobalServerConfig.Job.SchedulerName
	// fill volumes
	pod.Spec.Volumes = sp.appendVolumeIfAbsent(pod.Spec.Volumes, sp.generateVolume())
	// file container
	if err := sp.fillContainersInPod(pod); err != nil {
		log.Errorf("failed to fill containers, err=%v", err)
		return err
	}
	return nil
}

// CreateJob creates a job
func (sp *SingleJob) CreateJob() (string, error) {
	if err := sp.validateJob(); err != nil {
		log.Errorf("validate job failed, err: %v", err)
		return "", err
	}
	jobID := sp.GetID()
	log.Debugf("begin create job jobID:[%s]", jobID)

	singlePod := &v1.Pod{}
	if sp.YamlTemplateContent != nil && len(sp.YamlTemplateContent) != 0 {
		if err := sp.createJobFromYaml(singlePod); err != nil {
			log.Errorf("create job failed, err %v", err)
			return "", err
		}
	}

	if err := sp.patchSinglePodVariable(singlePod, jobID); err != nil {
		log.Errorf("failed to patch single pod variable, err=%v", err)
		return "", err
	}

	log.Debugf("begin submit job jobID:[%s], singlePod:[%v]", jobID, singlePod)
	err := Create(singlePod, k8s.PodGVK, sp.DynamicClientOption)
	if err != nil {
		log.Errorf("create job %v failed, err %v", jobID, err)
		return "", err
	}
	return jobID, nil
}

// StopJobByID stops a job by jobID
func (sp *SingleJob) StopJobByID(jobID string) error {
	job, err := models.GetJobByID(jobID)
	if err != nil {
		return err
	}
	namespace := job.Config.GetNamespace()
	if err = Delete(namespace, job.ID, k8s.PodGVK, sp.DynamicClientOption); err != nil {
		log.Errorf("stop vcjob %s in namespace %s failed, err %v", job.ID, namespace, err)
		return err
	}
	return nil
}

//fillContainersInPod fill containers in pod
func (sp *SingleJob) fillContainersInPod(pod *v1.Pod) error {
	log.Debugf("fillContainersInPod for job[%s]", pod.Name)
	if pod.Spec.Containers == nil || len(pod.Spec.Containers) == 0 {
		pod.Spec.Containers = []v1.Container{{}}
	}
	// only fill the first container
	index := 0
	if err := sp.fillContainer(&pod.Spec.Containers[index], pod.Name); err != nil {
		log.Errorf("fillContainer occur a err[%v]", err)
		return err
	}
	log.Debugf("job[%s].Spec.Tasks=[%+v]", pod.Name, pod.Spec.Containers)
	return nil
}

//fill container for pod, and return err if exist error
func (sp *SingleJob) fillContainer(container *v1.Container, podName string) error {
	log.Debugf("fillContainer for job[%s]", podName)
	// fill name
	if container.Name == "" {
		container.Name = podName
	}
	// fill image
	if container.Image == "" {
		container.Image = sp.Image
	}
	// fill command
	if container.Command == nil || len(container.Command) == 0 {
		container.Command = []string{"bash", "-c", sp.fixContainerCommand(sp.Command)}
	}
	// container.Args would be passed
	// fill resource
	flavour, err := sp.getFlavour()
	container.Resources = sp.generateResourceRequirements(flavour)
	if err != nil {
		log.Errorf("getFlavour occur a err[%v]", err)
		return err
	}
	// fill env
	container.Env = sp.appendEnvIfAbsent(container.Env, sp.generateEnvVars())
	// fill volumeMount
	container.VolumeMounts = sp.appendMountIfAbsent(container.VolumeMounts, sp.generateVolumeMount())

	log.Debugf("fillContainer completed: pod[%s]-container[%s]", podName, container.Name)
	return nil
}

// getFlavour get flavour by name or create a new one
func (sp *SingleJob) getFlavour() (schema.Flavour, error) {
	res := schema.Flavour{}
	// get flavour by name
	log.Debugf("pod[%s].conf.Flavour=%v", sp.Name, sp.Flavour)
	flavour, err := models.GetFlavour(sp.Flavour.Name)
	if err == nil {
		return schema.Flavour{
			Name: flavour.Name,
			ResourceInfo: schema.ResourceInfo{
				CPU:             flavour.CPU,
				Mem:             flavour.Mem,
				ScalarResources: flavour.ScalarResources,
			},
		}, nil
	}
	// flavour not found from db, create a new one by singlePod.config
	if err != gorm.ErrRecordNotFound {
		return res, err
	} else if sp.Flavour.CPU == "" && sp.Flavour.Mem == "" {
		return res, fmt.Errorf("flavour[%v] not found, meanwhile cpu or mem is empty", sp.Flavour)
	} else {
		// If run here, it means flavour is not found by name, construct temp flavour.
		return sp.Flavour, nil
	}
}
