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
	v1 "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
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
	if !sp.IsCustomYaml {
		if sp.Namespace == "" {
			return fmt.Errorf("namespace is empty")
		}
		if sp.Image == "" {
			return fmt.Errorf("spark image is not defined")
		}
		if sp.Command == "" {
			return fmt.Errorf("command is empty")
		}
	}
	return nil
}

// patchSinglePodVariable patch env variable to vcJob, the order of patches following vcJob crd
func (sp *SingleJob) patchSinglePodVariable(pod *v1.Pod, jobID string) error {
	// if pod's name exist, sp.Name should be overwritten
	// metadata
	sp.patchMetadata(&pod.ObjectMeta, sp.ID)

	if len(sp.QueueName) > 0 {
		pod.Annotations[schema.QueueLabelKey] = sp.QueueName
		pod.Labels[schema.QueueLabelKey] = sp.QueueName
		priorityClass := sp.getPriorityClass()
		pod.Spec.PriorityClassName = priorityClass
	}
	sp.fillPodSpec(&pod.Spec, nil)

	// file container
	if err := sp.fillContainersInPod(pod); err != nil {
		log.Errorf("failed to fill containers, err=%v", err)
		return err
	}
	return nil
}

// CreateJob creates a job
func (sp *SingleJob) CreateJob() (string, error) {
	jobID := sp.GetID()
	log.Debugf("begin create job jobID:[%s]", jobID)

	singlePod := &v1.Pod{}
	if sp.YamlTemplateContent != nil && len(sp.YamlTemplateContent) != 0 {
		if err := sp.createJobFromYaml(singlePod); err != nil {
			log.Errorf("create job failed, err %v", err)
			return "", err
		}
	}
	if err := sp.validateJob(); err != nil {
		log.Errorf("validate job failed, err: %v", err)
		return "", err
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

// fillContainersInPod fill containers in pod
func (sp *SingleJob) fillContainersInPod(pod *v1.Pod) error {
	log.Debugf("fillContainersInPod for job[%s]", pod.Name)
	if pod.Spec.Containers == nil || len(pod.Spec.Containers) == 0 {
		pod.Spec.Containers = []v1.Container{{}}
	}

	// patch config for Paddle Para
	_, find := sp.Env[schema.EnvPaddleParaJob]
	if find {
		if err := sp.patchPaddlePara(pod, pod.Name); err != nil {
			log.Errorf("patch parameters for paddle para job failed, err: %v", err)
			return err
		}
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

// fill container for pod, and return err if exist error
func (sp *SingleJob) fillContainer(container *v1.Container, podName string) error {
	log.Debugf("fillContainer for job[%s]", podName)
	// fill name
	if sp.isNeedPatch(container.Name) {
		container.Name = podName
	}
	// fill image
	if sp.isNeedPatch(container.Image) {
		container.Image = sp.Image
	}
	// fill command
	if !sp.IsCustomYaml {
		if sp.Command == "" {
			return errors.EmptyJobCommandError()
		}
		container.Command = []string{"sh", "-c", sp.fixContainerCommand(sp.Command)}
	}

	// container.Args would be passed
	// fill resource
	container.Resources = sp.generateResourceRequirements(sp.Flavour)

	// fill env
	container.Env = sp.appendEnvIfAbsent(container.Env, sp.generateEnvVars())
	// fill volumeMount
	container.VolumeMounts = appendMountsIfAbsent(container.VolumeMounts, generateVolumeMounts(sp.FileSystems))

	log.Debugf("fillContainer completed: pod[%s]-container[%s]", podName, container.Name)
	return nil
}
