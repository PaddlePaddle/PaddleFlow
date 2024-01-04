/*
Copyright (c) 2024 PaddlePaddle Authors. All Rights Reserve.

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

package kuberuntime

import (
	"strings"

	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

// PodSpecBuilder is used to build pod spec
type PodSpecBuilder struct {
	podSpec *corev1.PodSpec
	jobID   string
}

// NewPodSpecBuilder create a new PodSpecBuilder
func NewPodSpecBuilder(podSpec *corev1.PodSpec, jobID string) *PodSpecBuilder {
	return &PodSpecBuilder{
		podSpec: podSpec,
		jobID:   jobID,
	}
}

// Scheduler set scheduler name for pod
func (p *PodSpecBuilder) Scheduler() *PodSpecBuilder {
	// fill SchedulerName
	p.podSpec.SchedulerName = config.GlobalServerConfig.Job.SchedulerName
	return p
}

// PriorityClassName set priority class name for pod
func (p *PodSpecBuilder) PriorityClassName(priorityName string) *PodSpecBuilder {
	p.podSpec.PriorityClassName = KubePriorityClass(priorityName)
	return p
}

// RestartPolicy set restart policy for pod
func (p *PodSpecBuilder) RestartPolicy(restartPolicy string) *PodSpecBuilder {
	// fill restartPolicy
	if restartPolicy == string(corev1.RestartPolicyAlways) ||
		restartPolicy == string(corev1.RestartPolicyOnFailure) {
		p.podSpec.RestartPolicy = corev1.RestartPolicy(restartPolicy)
	} else {
		p.podSpec.RestartPolicy = corev1.RestartPolicyNever
	}
	return p
}

// PFS set volumes and volumeMounts for pod
func (p *PodSpecBuilder) PFS(fileSystems []schema.FileSystem) *PodSpecBuilder {
	if len(fileSystems) == 0 {
		return p
	}
	// fill volumes
	p.podSpec.Volumes = BuildVolumes(p.podSpec.Volumes, fileSystems)
	// fill volumeMount
	for idx, container := range p.podSpec.Containers {
		p.podSpec.Containers[idx].VolumeMounts = BuildVolumeMounts(container.VolumeMounts, fileSystems)
	}
	// fill affinity
	var fsIDs []string
	for _, fs := range fileSystems {
		fsIDs = append(fsIDs, fs.ID)
	}
	var err error
	p.podSpec.Affinity, err = generateAffinity(p.podSpec.Affinity, fsIDs)
	if err != nil {
		log.Infof("mergeNodeAffinity new affinity is nil")
	}
	return p
}

// containerResources set resources for container
func (p *PodSpecBuilder) containerResources(container *corev1.Container, requestFlavour, limitFlavour schema.Flavour) {
	// fill request resources
	if !schema.IsEmptyResource(requestFlavour.ResourceInfo) {
		flavourResource, err := resources.NewResourceFromMap(requestFlavour.ToMap())
		if err != nil {
			log.Errorf("GenerateResourceRequirements by request:[%+v] error:%v", requestFlavour, err)
			return
		}
		container.Resources.Requests = k8s.NewResourceList(flavourResource)
	}
	// fill limit resources
	if !schema.IsEmptyResource(limitFlavour.ResourceInfo) {
		limitFlavourResource, err := resources.NewResourceFromMap(limitFlavour.ToMap())
		if err != nil {
			log.Errorf("GenerateResourceRequirements by limitFlavour:[%+v] error:%v", limitFlavourResource, err)
			return
		}
		if strings.ToUpper(limitFlavour.Name) == schema.EnvJobLimitFlavourNone {
			container.Resources.Limits = nil
		} else {
			// limit set specified value
			container.Resources.Limits = k8s.NewResourceList(limitFlavourResource)
		}
	}

}

// Containers set containers for pod
func (p *PodSpecBuilder) Containers(task schema.Member) *PodSpecBuilder {
	log.Debugf("fill containers for job[%s]", p.jobID)
	for idx, container := range p.podSpec.Containers {
		// fill container[*].name
		if task.Name != "" {
			p.podSpec.Containers[idx].Name = task.Name
		}
		// fill container[*].Image
		if task.Image != "" {
			p.podSpec.Containers[idx].Image = task.Image
		}
		// fill container[*].Command
		if task.Command != "" {
			workDir := getWorkDir(&task, nil, nil)
			container.Command = generateContainerCommand(task.Command, workDir)
		}
		// fill container[*].Args
		if len(task.Args) > 0 {
			container.Args = task.Args
		}
		// fill container[*].Resources
		p.containerResources(&p.podSpec.Containers[idx], task.Flavour, task.LimitFlavour)
		// fill env
		p.podSpec.Containers[idx].Env = BuildEnvVars(container.Env, task.Env)
	}
	log.Debugf("fill containers completed: %v", p.podSpec.Containers)
	return p
}

// Build create pod spec
func (p *PodSpecBuilder) Build(task schema.Member) {
	if p.podSpec == nil {
		return
	}
	p.Scheduler().
		RestartPolicy(task.GetRestartPolicy()).
		PriorityClassName(task.Priority).
		PFS(task.Conf.GetAllFileSystem()).
		Containers(task)
}

// PodTemplateSpecBuilder build pod template spec
type PodTemplateSpecBuilder struct {
	podTemplateSpec *corev1.PodTemplateSpec
	jobID           string
}

// NewPodTemplateSpecBuilder create pod template spec builder
func NewPodTemplateSpecBuilder(podTempSpec *corev1.PodTemplateSpec, jobID string) *PodTemplateSpecBuilder {
	return &PodTemplateSpecBuilder{
		podTemplateSpec: podTempSpec,
		jobID:           jobID,
	}
}

// Metadata set metadata for pod template spec
func (k *PodTemplateSpecBuilder) Metadata(name, namespace string, labels, annotations map[string]string) *PodTemplateSpecBuilder {
	if name != "" {
		k.podTemplateSpec.Name = name
	}
	if namespace != "" {
		k.podTemplateSpec.Namespace = namespace
	}
	// set annotations
	k.podTemplateSpec.Annotations = appendMapsIfAbsent(k.podTemplateSpec.Annotations, annotations)
	// set labels
	k.podTemplateSpec.Labels = appendMapsIfAbsent(k.podTemplateSpec.Labels, labels)
	k.podTemplateSpec.Labels[schema.JobIDLabel] = k.jobID
	k.podTemplateSpec.Labels[schema.JobOwnerLabel] = schema.JobOwnerValue
	return k
}

// Build set pod spec for pod template spec
func (k *PodTemplateSpecBuilder) Build(task schema.Member) {
	if k.podTemplateSpec == nil {
		return
	}
	// build metadata
	k.Metadata("", "", task.GetLabels(), task.GetAnnotations())
	// set pod spec
	NewPodSpecBuilder(&k.podTemplateSpec.Spec, k.jobID).Build(task)
}

// KubeflowJobBuilder build kubeflow job
type KubeflowJobBuilder struct {
	runPolicy   *kubeflowv1.RunPolicy
	replicaSpec *kubeflowv1.ReplicaSpec
	jobID       string
}

// NewKubeflowJobBuilder create kubeflow job builder
func NewKubeflowJobBuilder(jobID string, runPolicy *kubeflowv1.RunPolicy,
	replicaSpec *kubeflowv1.ReplicaSpec) *KubeflowJobBuilder {
	return &KubeflowJobBuilder{
		jobID:       jobID,
		runPolicy:   runPolicy,
		replicaSpec: replicaSpec,
	}
}

// ReplicaSpec set replica spec for kubeflow job
func (k *KubeflowJobBuilder) ReplicaSpec(task schema.Member) {
	if k.replicaSpec == nil {
		return
	}
	// set Replicas for job
	var replicas int32 = 1
	if task.Replicas > 0 {
		replicas = int32(task.Replicas)
		k.replicaSpec.Replicas = &replicas
	}
	// set RestartPolicy
	k.replicaSpec.RestartPolicy = kubeflowv1.RestartPolicyNever
	// set PodTemplate
	NewPodTemplateSpecBuilder(&k.replicaSpec.Template, k.jobID).Build(task)
}

// RunPolicy set run policy for kubeflow job
func (k *KubeflowJobBuilder) RunPolicy(minResources *corev1.ResourceList, queueName, priority string) {
	if k.runPolicy == nil {
		return
	}
	// set SchedulingPolicy
	if k.runPolicy.SchedulingPolicy == nil {
		k.runPolicy.SchedulingPolicy = &kubeflowv1.SchedulingPolicy{}
	}
	if queueName != "" {
		k.runPolicy.SchedulingPolicy.Queue = queueName
	}
	if priority != "" {
		k.runPolicy.SchedulingPolicy.PriorityClass = KubePriorityClass(priority)
	}
	if minResources != nil {
		k.runPolicy.SchedulingPolicy.MinResources = minResources
	}
}
