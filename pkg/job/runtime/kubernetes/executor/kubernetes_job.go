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
	"reflect"
	"strings"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kubeschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"

	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/errors"
	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/job/api"
)

const (
	defaultPodReplicas        = 1 // default replicas for pod mode
	defaultPSReplicas         = 1 // default replicas for PS mode, including ps-server and ps-worker
	defaultCollectiveReplicas = 2 // default replicas for collective mode
)

// KubeJobInterface define methods for create kubernetes job
type KubeJobInterface interface {
	createJobFromYaml(jobEntity interface{}) error
	getPriorityClass() string

	generateVolume(pvcName string) corev1.Volume
	generateVolumeMount() corev1.VolumeMount
	generateEnvVars() []corev1.EnvVar
	fixContainerCommand(string) string
	generateResourceRequirements(flavour schema.Flavour) corev1.ResourceRequirements
	appendVolumeIfAbsent(vSlice []corev1.Volume, element corev1.Volume) []corev1.Volume
	appendMountIfAbsent(vmSlice []corev1.VolumeMount, element corev1.VolumeMount) []corev1.VolumeMount

	validateJob() error
}

type KubeJob struct {
	ID string
	// Name the name of job on kubernetes
	Name       string
	Namespace  string
	JobType    schema.JobType
	JobMode    string
	Image      string
	Command    string
	Env        map[string]string
	VolumeName string
	PVCName    string
	Priority   string
	QueueName  string
	// YamlTemplateContent indicate template content of job
	YamlTemplateContent []byte
	GroupVersionKind    kubeschema.GroupVersionKind

	DynamicClientOption *k8s.DynamicClientOption
}

func NewKubeJob(job *api.PFJob, dynamicClientOpt *k8s.DynamicClientOption) (api.PFJobInterface, error) {
	pvcName := fmt.Sprintf("pfs-%s-pvc", job.Conf.GetFS())
	kubeJob := KubeJob{
		ID:                  job.ID,
		Name:                job.Name,
		Namespace:           job.Namespace,
		JobType:             job.JobType,
		JobMode:             job.JobMode,
		Image:               job.Conf.GetImage(),
		Command:             job.Conf.GetCommand(),
		Env:                 job.Conf.GetEnv(),
		VolumeName:          job.Conf.GetFS(),
		PVCName:             pvcName,
		YamlTemplateContent: job.ExtRuntimeConf,
		Priority:            job.Conf.GetPriority(),
		QueueName:           job.Conf.GetQueueName(),
		DynamicClientOption: dynamicClientOpt,
	}

	switch job.JobType {
	case schema.TypeSparkJob:
		kubeJob.GroupVersionKind = k8s.SparkAppGVK
		return &SparkJob{
			KubeJob:          kubeJob,
			SparkMainFile:    job.Conf.Env[schema.EnvJobSparkMainFile],
			SparkMainClass:   job.Conf.Env[schema.EnvJobSparkMainClass],
			SparkArguments:   job.Conf.Env[schema.EnvJobSparkArguments],
			DriverFlavour:    job.Conf.Env[schema.EnvJobDriverFlavour],
			ExecutorFlavour:  job.Conf.Env[schema.EnvJobExecutorFlavour],
			ExecutorReplicas: job.Conf.Env[schema.EnvJobExecutorReplicas],
		}, nil
	case schema.TypeVcJob:
		kubeJob.GroupVersionKind = k8s.VCJobGVK
		return &VCJob{
			KubeJob:       kubeJob,
			JobModeParams: newJobModeParams(job.Conf),
		}, nil
	case schema.TypePaddleJob:
		kubeJob.GroupVersionKind = k8s.PaddleJobGVK
		return &PaddleJob{
			KubeJob:       kubeJob,
			JobModeParams: newJobModeParams(job.Conf),
		}, nil

	default:
		return nil, fmt.Errorf("kubernetes job type[%s] is not supported", job.Conf.Type())
	}
}

func (j *KubeJob) generateVolume(pvcName string) corev1.Volume {
	volume := corev1.Volume{
		Name: j.VolumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
		},
	}
	return volume
}

func (j *KubeJob) generateVolumeMount() corev1.VolumeMount {
	volumeMount := corev1.VolumeMount{
		Name:      j.VolumeName,
		ReadOnly:  false,
		MountPath: schema.DefaultFSMountPath,
	}
	return volumeMount
}

func (j *KubeJob) generateEnvVars() []corev1.EnvVar {
	envs := make([]corev1.EnvVar, 0)
	for key, value := range j.Env {
		env := corev1.EnvVar{
			Name:  key,
			Value: value,
		}
		envs = append(envs, env)
	}
	return envs
}

func (j *KubeJob) validateJob() error {
	// TODO: add validate job
	return nil
}

func (j *KubeJob) getPriorityClass() string {
	switch j.Priority {
	case schema.EnvJobVeryLowPriority:
		return schema.PriorityClassVeryLow
	case schema.EnvJobLowPriority:
		return schema.PriorityClassLow
	case schema.EnvJobNormalPriority:
		return schema.PriorityClassNormal
	case schema.EnvJobHighPriority:
		return schema.PriorityClassHigh
	case schema.EnvJobVeryHighPriority:
		return schema.PriorityClassVeryHigh
	}
	return schema.PriorityClassNormal
}

// createJobFromYaml parse the object of job from specified yaml file path
func (j *KubeJob) createJobFromYaml(jobEntity interface{}) error {
	log.Debugf("createJobFromYaml jobEntity[%+v] %v", jobEntity, reflect.TypeOf(jobEntity))

	// decode []byte into unstructured.Unstructured
	dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	unstructuredObj := &unstructured.Unstructured{}

	if _, _, err := dec.Decode(j.YamlTemplateContent, nil, unstructuredObj); err != nil {
		log.Errorf("Decode from yamlFIle[%s] failed! err:[%v]\n", j.YamlTemplateContent, err)
		return err
	}

	// convert unstructuredObj.Object into entity
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.Object, jobEntity); err != nil {
		log.Errorf("convert map struct object[%+v] to acutal job type failed: %v", unstructuredObj.Object, err)
		return err
	}

	log.Debugf("get jobEntity[%+v] from yamlContent[%s]job", jobEntity, j.YamlTemplateContent)
	return nil
}

// fillContainerInTask fill container in job task
// flavourKey can be {schema.EnvJobFlavour, schema.EnvJobWorkerFlavour, schema.EnvJobPServerFlavour} of Env value
// comm and in conf.Command or {schema.EnvJobPServerCommand, schema.EnvJobWorkerCommand} in Env
func (j *KubeJob) fillContainerInTask(container *corev1.Container, flavourKey, command string) {
	container.Image = j.Image
	container.Command = []string{"bash", "-c", j.fixContainerCommand(command)}
	flavourValue := config.GlobalServerConfig.FlavourMap[flavourKey]
	container.Resources = j.generateResourceRequirements(flavourValue)
	container.VolumeMounts = j.appendMountIfAbsent(container.VolumeMounts, j.generateVolumeMount())
	container.Env = j.generateEnvVars()
}

// appendMountIfAbsent append volumeMount if not exist in volumeMounts
func (j *KubeJob) appendMountIfAbsent(vmSlice []corev1.VolumeMount, element corev1.VolumeMount) []corev1.VolumeMount {
	if vmSlice == nil {
		return []corev1.VolumeMount{element}
	}
	for _, cur := range vmSlice {
		if cur.Name == element.Name {
			return vmSlice
		}
	}
	vmSlice = append(vmSlice, element)
	return vmSlice
}

// appendVolumeIfAbsent append volume if not exist in volumes
func (j *KubeJob) appendVolumeIfAbsent(vSlice []corev1.Volume, element corev1.Volume) []corev1.Volume {
	if vSlice == nil {
		return []corev1.Volume{element}
	}
	for _, cur := range vSlice {
		if cur.Name == element.Name {
			return vSlice
		}
	}
	vSlice = append(vSlice, element)
	return vSlice
}

func (j *KubeJob) fixContainerCommand(command string) string {
	command = strings.TrimPrefix(command, "bash -c")
	command = fmt.Sprintf("%s %s;%s", "cd", schema.DefaultFSMountPath, command)
	return command
}

func (j *KubeJob) generateResourceRequirements(flavour schema.Flavour) corev1.ResourceRequirements {
	log.Infof("generateResourceRequirements by flavour:[%+v]", flavour)
	resources := corev1.ResourceRequirements{
		Requests: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    resource.MustParse(flavour.CPU),
			corev1.ResourceMemory: resource.MustParse(flavour.Mem),
		},
		Limits: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    resource.MustParse(flavour.CPU),
			corev1.ResourceMemory: resource.MustParse(flavour.Mem),
		},
	}

	for key, value := range flavour.ScalarResources {
		resources.Requests[corev1.ResourceName(key)] = resource.MustParse(value)
		resources.Limits[corev1.ResourceName(key)] = resource.MustParse(value)
	}

	return resources
}

func (j *KubeJob) patchMetadata(metadata *metav1.ObjectMeta) {
	metadata.Name = j.Name
	metadata.Namespace = j.Namespace
	if metadata.Labels == nil {
		metadata.Labels = map[string]string{}
	}
	metadata.Labels[schema.JobOwnerLabel] = schema.JobOwnerValue
	metadata.Labels[schema.JobIDLabel] = j.ID
}

func (j *KubeJob) CreateJob() (string, error) {
	return "", nil
}

func (j *KubeJob) StopJobByID(id string) error {
	return nil
}

func (j *KubeJob) UpdateJob() error {
	return nil
}

func (j *KubeJob) DeleteJob() error {
	log.Infof("delete %s job %s/%s from cluster", j.JobType, j.Namespace, j.Name)
	if err := Delete(j.Namespace, j.Name, j.GroupVersionKind, j.DynamicClientOption); err != nil {
		log.Errorf("delete %s job %s/%s from cluster failed, err %v", j.JobType, j.Namespace, j.Name, err)
		return err
	}
	return nil
}

func (j *KubeJob) GetID() string {
	return j.ID
}

// JobModeParams records the parameters related to job mode
type JobModeParams struct {
	JobFlavour string // flavour of job in pod or collective mode

	CollectiveJobReplicas string // parameters for Collective job

	PServerReplicas string // server.replicas or driver.replicas of job
	PServerFlavour  string // server.flavour or driver.flavour of job
	PServerCommand  string // server.command or driver.command of job
	WorkerReplicas  string // worker.replicas or executor.replicas of job
	WorkerFlavour   string // worker.flavour or executor.flavour of job
	WorkerCommand   string // worker.command or executor.command of job
}

// newJobModeParams create a JobModeParams for job with jobMode
func newJobModeParams(conf schema.Conf) JobModeParams {
	return JobModeParams{
		PServerReplicas:       conf.GetPSReplicas(),
		PServerFlavour:        conf.GetPSFlavour(),
		PServerCommand:        conf.GetPSCommand(),
		WorkerReplicas:        conf.GetWorkerReplicas(),
		WorkerFlavour:         conf.GetWorkerFlavour(),
		WorkerCommand:         conf.GetWorkerCommand(),
		CollectiveJobReplicas: conf.GetJobReplicas(),
		JobFlavour:            conf.GetFlavour(),
	}
}

func (j *JobModeParams) validatePodMode() error {
	if len(j.JobFlavour) == 0 {
		return errors.EmptyFlavourError()
	}
	return nil
}

// validatePSMode validate PServerCommand, WorkerCommand
func (j *JobModeParams) validatePSMode() error {
	if len(j.WorkerFlavour) == 0 || len(j.WorkerCommand) == 0 || len(j.PServerFlavour) == 0 || len(j.PServerCommand) == 0 {
		return errors.EmptyFlavourError()
	}

	return nil
}

func (j *JobModeParams) validateCollectiveMode() error {
	if len(j.WorkerFlavour) == 0 || len(j.WorkerCommand) == 0 {
		return errors.EmptyFlavourError()
	}
	return nil
}

// patchTaskParams return
func (j *JobModeParams) patchTaskParams(isMaster bool) (string, string, string) {
	psReplicaStr := ""
	commandEnv := ""
	flavourStr := ""
	if isMaster {
		psReplicaStr = j.PServerReplicas
		flavourStr = j.PServerFlavour
		commandEnv = j.PServerCommand
	} else {
		psReplicaStr = j.WorkerReplicas
		flavourStr = j.WorkerFlavour
		commandEnv = j.WorkerCommand
	}
	return psReplicaStr, commandEnv, flavourStr
}
