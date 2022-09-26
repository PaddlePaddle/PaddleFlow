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

	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

// kubeflowRunPolicy build RunPolicy for kubeflow job, such as PyTorchJob, TFJob and so on.
func (j *KubeJob) kubeflowRunPolicy(runPolicy *kubeflowv1.RunPolicy, minResources *corev1.ResourceList) error {
	if runPolicy == nil {
		return fmt.Errorf("build run policy for %s faield, err: runPolicy is nil", j.String())
	}
	// TODO set cleanPolicy
	// set SchedulingPolicy
	if runPolicy.SchedulingPolicy == nil {
		runPolicy.SchedulingPolicy = &kubeflowv1.SchedulingPolicy{}
	}
	runPolicy.SchedulingPolicy.Queue = j.QueueName
	runPolicy.SchedulingPolicy.PriorityClass = j.getPriorityClass()
	runPolicy.SchedulingPolicy.MinResources = minResources
	return nil
}

// kubeflowReplicaSpec build ReplicaSpec for kubeflow job, such as PyTorchJob, TFJob and so on.
func (j *KubeJob) kubeflowReplicaSpec(replicaSpec *kubeflowv1.ReplicaSpec, task *schema.Member) error {
	if replicaSpec == nil || task == nil {
		return fmt.Errorf("build %s failed, err: replicaSpec or task is nil", j.String())
	}
	// set Replicas for job
	replicas := int32(task.Replicas)
	replicaSpec.Replicas = &replicas
	// set RestartPolicy
	// TODO: make RestartPolicy configurable
	replicaSpec.RestartPolicy = kubeflowv1.RestartPolicyNever
	// set PodTemplate
	return j.setPodTemplateSpec(&replicaSpec.Template, task)
}

// setPodTemplateSpec build PodTemplateSpec for built-in distributed job, such as PaddleJob, PyTorchJob, TFJob and so on
func (j *KubeJob) setPodTemplateSpec(podSpec *corev1.PodTemplateSpec, task *schema.Member) error {
	if podSpec == nil || task == nil {
		return fmt.Errorf("podTemplateSpec or task is nil")
	}
	// TODO: set pod metadata
	// set SchedulerName
	podSpec.Spec.SchedulerName = config.GlobalServerConfig.Job.SchedulerName
	// TODO: remove hard coded schedulerName when upstream package is fixed
	// HARD CODE schedulerName to default scheduler, fix KubeFlow training operator bug at volcano scheduler TEMPERATELY
	// see issue https://github.com/kubeflow/training-operator/issues/1630
	podSpec.Spec.SchedulerName = "default-scheduler"
	// set Priority
	podSpec.Spec.PriorityClassName = KubePriorityClass(task.Priority)
	// set RestartPolicy with framework
	if j.Framework == schema.FrameworkPaddle {
		podSpec.Spec.RestartPolicy = corev1.RestartPolicyNever
	}
	// set Affinity
	if err := j.setAffinity(&podSpec.Spec); err != nil {
		log.Errorf("setAffinity for %s failed, err: %v", j.String(), err)
		return err
	}
	// set Volumes
	podSpec.Spec.Volumes = appendVolumesIfAbsent(podSpec.Spec.Volumes, generateVolumes(j.FileSystems))
	// set Containers[0]
	if len(podSpec.Spec.Containers) != 1 {
		podSpec.Spec.Containers = []corev1.Container{{}}
	}
	if err := j.setPodContainer(&podSpec.Spec.Containers[0], task); err != nil {
		log.Errorf("build container for %s failed, err: %v", j.String(), err)
		return err
	}
	return nil
}

// setPodContainer build container in pod
func (j *KubeJob) setPodContainer(container *corev1.Container, task *schema.Member) error {
	if container == nil || task == nil {
		return fmt.Errorf("contaienr or task is nil")
	}
	container.Image = task.Image
	// set container Env
	container.Env = j.appendEnvIfAbsent(container.Env, j.generateTaskEnvVars(task.Env))
	// set container Command and Args
	j.fillCMDInContainer(container, task)

	if len(task.Args) > 0 {
		container.Args = task.Args
	}
	// set container Resources
	var err error
	container.Resources, err = j.generateResourceRequirements(task.Flavour)
	if err != nil {
		log.Errorf("fillContainerInTasks failed when generateResourceRequirements, err: %v", err)
		return err
	}
	// set container VolumeMounts
	taskFs := task.Conf.GetAllFileSystem()
	if len(taskFs) != 0 {
		container.VolumeMounts = appendMountsIfAbsent(container.VolumeMounts, generateVolumeMounts(taskFs))
	}
	// TODO: set others field
	return nil
}
