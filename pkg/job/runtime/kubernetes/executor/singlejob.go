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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

// SingleJob is a executor struct that runs a single pod
type SingleJob struct {
	KubeJob
	Flavour schema.Flavour
}

func (sp *SingleJob) validateJob(singlePod *v1.Pod) error {
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
		if schema.IsEmptyResource(sp.Flavour.ResourceInfo) {
			return fmt.Errorf("flavour resource is empty")
		}
	} else if err := sp.validateCustomYaml(singlePod); err != nil {
		log.Errorf("validate custom yaml failed, err %v", err)
		return err
	}
	return nil
}

func (sp *SingleJob) validateCustomYaml(singlePod *v1.Pod) error {
	log.Infof("validate custom yaml for single pod: %v, pod from yaml: %v", sp, singlePod)
	if singlePod.Spec.Containers == nil || len(singlePod.Spec.Containers) == 0 {
		return fmt.Errorf("single pod has no containers")
	}
	if err := validateTemplateResources(&singlePod.Spec); err != nil {
		log.Errorf("validate resources in extensionTemplate failed, err %v", err)
		return err
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
	// set single job mark
	pod.Labels[schema.JobLabelFramework] = string(schema.FrameworkStandalone)
	if err := sp.fillPodSpec(&pod.Spec, nil); err != nil {
		log.Errorf("single job fillPodSpec failed, err: %v", err)
		return err
	}

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
	if err := sp.createJobFromYaml(singlePod); err != nil {
		log.Errorf("create job failed, err %v", err)
		return "", err
	}

	if err := sp.validateJob(singlePod); err != nil {
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
	if sp.IsCustomYaml {
		log.Debugf("fillContainer passed for job[%s] with custom yaml", podName)
		return nil
	}
	// fill name
	container.Name = podName
	// fill image
	container.Image = sp.Image
	// fill command
	sp.fillCMDInContainer(container, nil)

	// container.Args would be passed
	// fill resource
	var err error
	container.Resources, err = sp.generateResourceRequirements(sp.Flavour)
	if err != nil {
		log.Errorf("generate resource requirements failed, err: %v", err)
		return err
	}
	// fill env
	container.Env = sp.appendEnvIfAbsent(container.Env, sp.generateEnvVars())
	// fill volumeMount
	container.VolumeMounts = appendMountsIfAbsent(container.VolumeMounts, generateVolumeMounts(sp.FileSystems))

	log.Debugf("fillContainer completed: pod[%s]-container[%s]", podName, container.Name)
	return nil
}
