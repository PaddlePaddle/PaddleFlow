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
	"io/ioutil"
	"path/filepath"
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
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	locationAwareness "github.com/PaddlePaddle/PaddleFlow/pkg/fs/location-awareness"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
)

const (
	defaultPodReplicas        = 1 // default replicas for pod mode
	defaultPSReplicas         = 1 // default replicas for PS mode, including ps-server and ps-worker
	defaultCollectiveReplicas = 2 // default replicas for collective mode

	fsLocationAwarenessKey    = "kubernetes.io/hostname"
	fsLocationAwarenessWeight = 100
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
	Name        string
	Namespace   string
	JobType     schema.JobType
	JobMode     string
	Image       string
	Command     string
	Env         map[string]string
	VolumeName  string // deprecated
	PVCName     string // deprecated
	Priority    string
	QueueName   string
	Labels      map[string]string
	Annotations map[string]string
	// 存储资源
	FileSystems []schema.FileSystem

	// job framework
	Framework schema.Framework
	// YamlTemplateContent indicate template content of job
	YamlTemplateContent []byte
	IsCustomYaml        bool
	Tasks               []models.Member
	GroupVersionKind    kubeschema.GroupVersionKind
	DynamicClientOption *k8s.DynamicClientOption
}

func NewKubeJob(job *api.PFJob, dynamicClientOpt *k8s.DynamicClientOption) (api.PFJobInterface, error) {
	log.Debugf("create kubernetes job: %#v", job)
	pvcName := ""
	if job.FSID != "" {
		pvcName = fmt.Sprintf("pfs-%s-pvc", job.FSID)
	}

	kubeJob := KubeJob{
		ID:                  job.ID,
		Name:                job.ID,
		Namespace:           job.Namespace,
		JobType:             job.JobType,
		JobMode:             job.JobMode,
		Image:               job.Conf.GetImage(),
		Command:             job.Conf.GetCommand(),
		Env:                 job.Conf.GetEnv(),
		VolumeName:          job.FSID,
		PVCName:             pvcName,
		Labels:              job.Conf.Labels,
		Annotations:         job.Conf.Annotations,
		FileSystems:         job.Conf.GetAllFileSystem(),
		Framework:           job.Framework,
		Tasks:               job.Tasks,
		Priority:            job.Conf.GetPriority(),
		QueueName:           job.Conf.GetQueueName(),
		DynamicClientOption: dynamicClientOpt,
		YamlTemplateContent: []byte(job.ExtensionTemplate),
	}

	switch job.JobType {
	case schema.TypeVcJob:
		// todo(zhongzichao): to be removed
		kubeJob.GroupVersionKind = k8s.VCJobGVK
		if len(job.Tasks) == 0 {
			kubeJob.Tasks = []models.Member{
				{
					Conf: schema.Conf{
						Flavour: job.Conf.Flavour,
					},
				},
			}
		}
		return &VCJob{
			KubeJob:       kubeJob,
			JobModeParams: newJobModeParams(job.Conf),
		}, nil
	case schema.TypeWorkflow:
		kubeJob.GroupVersionKind = k8s.ArgoWorkflowGVK
		return &WorkflowJob{
			KubeJob: kubeJob,
		}, nil
	case schema.TypeSingle:
		kubeJob.GroupVersionKind = k8s.PodGVK
		if kubeJob.Name == "" {
			kubeJob.Name = kubeJob.ID
		}
		return &SingleJob{
			KubeJob: kubeJob,
			Flavour: job.Conf.Flavour,
		}, nil
	case schema.TypeDistributed:
		return newFrameWorkJob(kubeJob, job)
	default:
		return nil, fmt.Errorf("kubernetes job type[%s] is not supported", job.Conf.Type())
	}
}

func newFrameWorkJob(kubeJob KubeJob, job *api.PFJob) (api.PFJobInterface, error) {
	switch job.Framework {
	case schema.FrameworkSpark:
		kubeJob.GroupVersionKind = k8s.SparkAppGVK
		sparkJob := &SparkJob{
			KubeJob: kubeJob,
		}
		if kubeJob.Tasks != nil && kubeJob.Tasks[0].Env != nil {
			sparkJob.SparkMainClass = kubeJob.Tasks[0].Env[schema.EnvJobSparkMainClass]
			sparkJob.SparkMainFile = kubeJob.Tasks[0].Env[schema.EnvJobSparkMainFile]
			sparkJob.SparkArguments = kubeJob.Tasks[0].Env[schema.EnvJobSparkArguments]
		}
		log.Debugf("newFrameWorkJob: create spark job: %#v", sparkJob)
		return sparkJob, nil
	case schema.FrameworkMPI:
		kubeJob.GroupVersionKind = k8s.VCJobGVK
		return &VCJob{
			KubeJob:       kubeJob,
			JobModeParams: newJobModeParams(job.Conf),
		}, nil
	case schema.FrameworkPaddle:
		kubeJob.GroupVersionKind = k8s.PaddleJobGVK
		return &PaddleJob{
			KubeJob:       kubeJob,
			JobModeParams: newJobModeParams(job.Conf),
		}, nil
	default:
		return nil, fmt.Errorf("kubernetes job framework[%s] is not supported", job.Framework)
	}
}

func (j *KubeJob) generateAffinity(affinity *corev1.Affinity, fsIDs []string) *corev1.Affinity {
	nodes, err := locationAwareness.ListFsCacheLocation(fsIDs)
	if err != nil || len(nodes) == 0 {
		log.Warningf("get location awareness for PaddleFlow filesystem %s failed or cache location is empty, err: %v", fsIDs, err)
		return affinity
	}
	log.Infof("nodes for PaddleFlow filesystem %s location awareness: %v", fsIDs, nodes)
	fsCacheAffinity := j.fsCacheAffinity(nodes)
	if affinity == nil {
		return fsCacheAffinity
	}
	// merge filesystem location awareness affinity to pod affinity
	if affinity.NodeAffinity == nil {
		affinity.NodeAffinity = fsCacheAffinity.NodeAffinity
	} else {
		affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
			fsCacheAffinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
			affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution...)
	}
	return affinity
}

func (j *KubeJob) fsCacheAffinity(nodes []string) *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{
				{
					Weight: fsLocationAwarenessWeight,
					Preference: corev1.NodeSelectorTerm{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      fsLocationAwarenessKey,
								Operator: corev1.NodeSelectorOpIn,
								Values:   nodes,
							},
						},
					},
				},
			},
		},
	}
}

// deprecated
func (j *KubeJob) generateVolume() corev1.Volume {
	if j.PVCName == "" || j.VolumeName == "" {
		return corev1.Volume{}
	}
	volume := corev1.Volume{
		Name: j.VolumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: j.PVCName,
			},
		},
	}
	return volume
}

// deprecated
func (j *KubeJob) generateVolumeMount() corev1.VolumeMount {
	if j.VolumeName == "" {
		return corev1.VolumeMount{}
	}
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
	return KubePriorityClass(j.Priority)
}

func KubePriorityClass(priority string) string {
	switch priority {
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
	default:
		return schema.PriorityClassNormal
	}
}

// createJobFromYaml parse the object of job from specified yaml file path
func (j *KubeJob) createJobFromYaml(jobEntity interface{}) error {
	log.Debugf("createJobFromYaml jobEntity[%+v] %v", jobEntity, reflect.TypeOf(jobEntity))
	// get extensionTemplate
	if len(j.YamlTemplateContent) == 0 {
		var err error
		j.IsCustomYaml = false
		j.YamlTemplateContent, err = j.getDefaultTemplate(j.Framework)
		if err != nil {
			return fmt.Errorf("get default template failed, err: %v", err)
		}
	} else {
		// get template from user
		j.IsCustomYaml = true
	}

	// decode []byte into unstructured.Unstructured
	dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	unstructuredObj := &unstructured.Unstructured{}

	if _, _, err := dec.Decode(j.YamlTemplateContent, nil, unstructuredObj); err != nil {
		log.Errorf("Decode from yamlFile[%s] failed! err:[%v]\n", string(j.YamlTemplateContent), err)
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

// fill PodSpec
func (j *KubeJob) fillPodSpec(podSpec *corev1.PodSpec, task *models.Member) {
	if task != nil {
		j.Priority = task.Priority
	}
	podSpec.PriorityClassName = j.getPriorityClass()
	// fill SchedulerName
	podSpec.SchedulerName = config.GlobalServerConfig.Job.SchedulerName
	// fill volumes
	podSpec.Volumes = appendVolumesIfAbsent(podSpec.Volumes, generateVolumes(j.FileSystems))
	if j.isNeedPatch(string(podSpec.RestartPolicy)) {
		podSpec.RestartPolicy = corev1.RestartPolicyNever
	}
	// fill affinity
	if len(j.FileSystems) != 0 {
		var fsIDs []string
		for _, fs := range j.FileSystems {
			fsIDs = append(fsIDs, fs.ID)
		}
		podSpec.Affinity = j.generateAffinity(podSpec.Affinity, fsIDs)
	}
}

// todo: to be removed
// fillContainerInVcJob fill container in job task, only called by vcjob
func (j *KubeJob) fillContainerInVcJob(container *corev1.Container, flavour schema.Flavour, command string) {
	container.Image = j.Image
	workDir := j.getWorkDir(nil)
	container.Command = j.generateContainerCommand(j.Command, workDir)
	container.Resources = j.generateResourceRequirements(flavour)
	container.VolumeMounts = j.appendMountIfAbsent(container.VolumeMounts, j.generateVolumeMount())
	container.Env = j.generateEnvVars()
}

// fillContainerInTasks fill container in job task
func (j *KubeJob) fillContainerInTasks(container *corev1.Container, task models.Member) {
	if j.isNeedPatch(container.Image) {
		container.Image = task.Image
	}
	if j.isNeedPatch(task.Command) {
		workDir := j.getWorkDir(&task)
		container.Command = j.generateContainerCommand(task.Command, workDir)
	}
	if !j.IsCustomYaml && len(task.Args) > 0 {
		container.Args = task.Args
	}
	container.Resources = j.generateResourceRequirements(task.Flavour)

	taskFs := task.Conf.GetAllFileSystem()
	if len(taskFs) != 0 {
		container.VolumeMounts = appendMountsIfAbsent(container.VolumeMounts, generateVolumeMounts(taskFs))
	}

	container.Env = j.appendEnvIfAbsent(container.Env, j.generateEnvVars())
}

// appendLabelsIfAbsent append labels if absent
func (j *KubeJob) appendLabelsIfAbsent(labels map[string]string, addLabels map[string]string) map[string]string {
	return appendMapsIfAbsent(labels, addLabels)
}

// appendAnnotationsIfAbsent append Annotations if absent
func (j *KubeJob) appendAnnotationsIfAbsent(Annotations map[string]string, addAnnotations map[string]string) map[string]string {
	return appendMapsIfAbsent(Annotations, addAnnotations)
}

// appendMapsIfAbsent append Maps if absent, only support string type
func appendMapsIfAbsent(Maps map[string]string, addMaps map[string]string) map[string]string {
	if Maps == nil {
		Maps = make(map[string]string)
	}
	for key, value := range addMaps {
		if _, ok := Maps[key]; !ok {
			Maps[key] = value
		}
	}
	return Maps
}

// appendEnvIfAbsent append new env if not exist in baseEnvs
func (j *KubeJob) appendEnvIfAbsent(baseEnvs []corev1.EnvVar, addEnvs []corev1.EnvVar) []corev1.EnvVar {
	if baseEnvs == nil {
		return addEnvs
	}
	keySet := make(map[string]bool)
	for _, cur := range baseEnvs {
		keySet[cur.Name] = true
	}
	for _, cur := range addEnvs {
		if _, ok := keySet[cur.Name]; !ok {
			baseEnvs = append(baseEnvs, cur)
		}
	}
	return baseEnvs
}

// deprecated
// appendMountIfAbsent append volumeMount if not exist in volumeMounts
func (j *KubeJob) appendMountIfAbsent(vmSlice []corev1.VolumeMount, element corev1.VolumeMount) []corev1.VolumeMount {
	if element.Name == "" {
		return vmSlice
	}
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

// deprecated
// appendVolumeIfAbsent append volume if not exist in volumes
func (j *KubeJob) appendVolumeIfAbsent(vSlice []corev1.Volume, element corev1.Volume) []corev1.Volume {
	if element.Name == "" {
		return vSlice
	}
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

// generateContainerCommand if task is not nil, prefer to using info in task, otherwise using job's
func (j *KubeJob) generateContainerCommand(command string, workdir string) []string {
	command = strings.TrimPrefix(command, "bash -c")
	command = strings.TrimPrefix(command, "sh -c")

	if workdir != "" {
		command = fmt.Sprintf("%s %s;%s", "cd", workdir, command)
	}

	commands := []string{"sh", "-c", command}
	return commands
}

func (j *KubeJob) getWorkDir(task *models.Member) string {
	// prepare fs and envs
	fileSystems := j.FileSystems
	envs := j.Env
	if task != nil {
		fileSystems = task.Conf.GetAllFileSystem()
		envs = task.Env
	}
	if len(envs) == 0 {
		envs = make(map[string]string)
	}
	// check workdir, which exist only if there is more than one file system and env.'EnvMountPath' is not NONE
	hasWorkDir := len(fileSystems) != 0 && strings.ToUpper(envs[schema.EnvMountPath]) != "NONE"
	if !hasWorkDir {
		return ""
	}

	workdir := ""
	mountPath := filepath.Clean(fileSystems[0].MountPath)
	log.Infof("getWorkDir by hasWorkDir: true,mountPath: %s, task: %v", mountPath, task)
	if mountPath != "." {
		workdir = mountPath
	} else {
		workdir = filepath.Join(schema.DefaultFSMountPath, fileSystems[0].ID)
	}
	envs[schema.EnvJobWorkDir] = workdir
	return workdir
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

func (j *KubeJob) patchMetadata(metadata *metav1.ObjectMeta, name string) {
	metadata.Name = name
	metadata.Namespace = j.Namespace
	metadata.Annotations = j.appendAnnotationsIfAbsent(metadata.Annotations, j.Annotations)
	metadata.Labels = j.appendLabelsIfAbsent(metadata.Labels, j.Labels)
	metadata.Labels[schema.JobOwnerLabel] = schema.JobOwnerValue
	metadata.Labels[schema.JobIDLabel] = j.ID
}

func (j *KubeJob) isNeedPatch(v string) bool {
	return !j.IsCustomYaml
}

func (j *KubeJob) CreateJob() (string, error) {
	return "", nil
}

func (j *KubeJob) StopJobByID(id string) error {
	return nil
}

func (j *KubeJob) UpdateJob(data []byte) error {
	log.Infof("update %s job %s/%s on cluster, data: %s", j.JobType, j.Namespace, j.Name, string(data))
	if err := Patch(j.Namespace, j.Name, j.GroupVersionKind, data, j.DynamicClientOption); err != nil {
		log.Errorf("update %s job %s/%s on cluster failed, err %v", j.JobType, j.Namespace, j.Name, err)
		return err
	}
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

func GetPodGroupName(jobID string) string {
	job, err := models.GetJobByID(jobID)
	if err != nil {
		log.Errorf("get job %s failed, err %v", jobID, err)
		return ""
	}

	// TODO: remove job type TypeVcJob
	if job.Type == string(schema.TypeVcJob) {
		return jobID
	}
	pgName := ""
	switch job.Framework {
	case schema.FrameworkPaddle:
		pgName = jobID
	case schema.FrameworkSpark:
		pgName = fmt.Sprintf("spark-%s-pg", jobID)
	case schema.FrameworkStandalone, "":
		runtimeInfo := job.RuntimeInfo.(map[string]interface{})
		jobObj := &unstructured.Unstructured{}
		if err = runtime.DefaultUnstructuredConverter.FromUnstructured(runtimeInfo, jobObj); err != nil {
			log.Errorf("convert obj to unstructed.Unstructed failed, err %v", err)
			return ""
		}
		anno := jobObj.GetAnnotations()
		if anno != nil {
			pgName = anno[schedulingv1beta1.KubeGroupNameAnnotationKey]
		}
	default:
		log.Warningf("the framework[%s] of job is not supported", job.Framework)
		pgName = jobID
	}
	return pgName
}

func (j *KubeJob) GetID() string {
	return j.ID
}

// patchPaddlePara patch some parameters for paddle para job, and must be work with a shared gpu device plugin
// environments for paddle para job:
//   PF_PADDLE_PARA_JOB: defines the job is a paddle para job
//   PF_PADDLE_PARA_PRIORITY: defines the priority of paddle para job, 0 is high, and 1 is low.
//   PF_PADDLE_PARA_CONFIG_FILE: defines the config of paddle para job
func (j *KubeJob) patchPaddlePara(podTemplate *corev1.Pod, jobName string) error {
	// get parameters from user's job config
	var paddleParaPriority string
	p := j.Env[schema.EnvPaddleParaPriority]
	switch strings.ToLower(p) {
	case schema.PriorityClassHigh:
		paddleParaPriority = "0"
	case schema.PriorityClassLow, "":
		paddleParaPriority = "1"
	default:
		return fmt.Errorf("priority %s for paddle para job is invalid", p)
	}
	// the config path of paddle para gpu job on host os, which will be mounted to job
	gpuConfigFile := schema.PaddleParaGPUConfigFilePath
	value, find := j.Env[schema.EnvPaddleParaConfigHostFile]
	if find {
		gpuConfigFile = value
	}
	gpuConfigDirPath := filepath.Dir(gpuConfigFile)
	if gpuConfigDirPath == "/" {
		return fmt.Errorf("the directory of gpu config file %s cannot be mounted", gpuConfigFile)
	}

	// 1. patch jobName and priority in Annotations
	if podTemplate.ObjectMeta.Annotations == nil {
		podTemplate.ObjectMeta.Annotations = make(map[string]string)
	}
	podTemplate.ObjectMeta.Annotations[schema.PaddleParaAnnotationKeyJobName] = jobName
	podTemplate.ObjectMeta.Annotations[schema.PaddleParaAnnotationKeyPriority] = paddleParaPriority
	// 2. patch env, including config file and job name
	env := podTemplate.Spec.Containers[0].Env
	podTemplate.Spec.Containers[0].Env = append([]corev1.EnvVar{
		{
			Name:  schema.PaddleParaEnvJobName,
			Value: jobName,
		},
		{
			Name:  schema.PaddleParaEnvGPUConfigFile,
			Value: gpuConfigFile,
		},
	}, env...)
	// 3. patch volumes and volumeMounts
	dirType := corev1.HostPathDirectory
	volumes := podTemplate.Spec.Volumes
	podTemplate.Spec.Volumes = append([]corev1.Volume{
		{
			Name: schema.PaddleParaVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: gpuConfigDirPath,
					Type: &dirType,
				},
			},
		},
	}, volumes...)
	volumeMounts := podTemplate.Spec.Containers[0].VolumeMounts
	podTemplate.Spec.Containers[0].VolumeMounts = append([]corev1.VolumeMount{
		{
			Name:      schema.PaddleParaVolumeName,
			MountPath: gpuConfigDirPath,
		},
	}, volumeMounts...)
	return nil
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
	// todo(zhongzichao) validate JobFlavour
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

// getDefaultPath get extra runtime conf default path
func getDefaultPath(jobType schema.JobType, framework schema.Framework, jobMode string) string {
	log.Debugf("get default path, jobType=%s, jobMode=%s", jobType, jobMode)
	baseDir := config.GlobalServerConfig.Job.DefaultJobYamlDir
	suffix := ".yaml"
	if len(jobMode) != 0 && framework != schema.FrameworkSpark {
		suffix = fmt.Sprintf("_%s.yaml", strings.ToLower(jobMode))
	}

	switch jobType {
	case schema.TypeSingle:
		return fmt.Sprintf("%s/%s%s", baseDir, jobType, suffix)
	case schema.TypeDistributed:
		// e.g. basedir/spark.yaml, basedir/paddle_ps.yaml
		return fmt.Sprintf("%s/%s%s", baseDir, framework, suffix)
	default:
		// todo(zhongzichao) remove vcjob type
		return fmt.Sprintf("%s/vcjob%s", baseDir, suffix)
	}
}

// getDefaultTemplate get default template from file
func (j *KubeJob) getDefaultTemplate(framework schema.Framework) ([]byte, error) {
	// get template from default path
	filePath := getDefaultPath(j.JobType, framework, j.JobMode)
	// check file exist
	if exist, err := config.PathExists(filePath); !exist || err != nil {
		log.Errorf("get job from path[%s] failed, file.exsit=[%v], err=[%v]", filePath, exist, err)
		return nil, errors.JobFileNotFound(filePath)
	}

	// read file as []byte
	extConf, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Errorf("read file [%s] failed! err:[%v]\n", filePath, err)
		return nil, err
	}
	return extConf, nil
}

func generateVolumes(fileSystem []schema.FileSystem) []corev1.Volume {
	log.Debugf("generateVolumes FileSystems[%+v]", fileSystem)
	var vs []corev1.Volume
	if len(fileSystem) == 0 {
		log.Debugf("found len(fileSystem) is 0 when calling generateVolumes(fs), fs: %+v", fileSystem)
		return vs
	}

	for _, fs := range fileSystem {
		volume := corev1.Volume{
			Name: fs.Name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: schema.ConcatenatePVCName(fs.ID),
				},
			},
		}
		vs = append(vs, volume)
	}

	return vs
}

func generateVolumeMounts(fileSystems []schema.FileSystem) []corev1.VolumeMount {
	log.Infof("generateVolumeMounts fileSystems:%+v", fileSystems)
	var vms []corev1.VolumeMount
	if len(fileSystems) == 0 {
		log.Debug("generateVolumeMounts fileSystems len is 0")
		return vms
	}
	for _, fs := range fileSystems {
		log.Debugf("generateVolumeMounts walking fileSystem %+v", fs)
		mountPath := filepath.Clean(fs.MountPath)
		if mountPath == "." {
			mountPath = filepath.Join(schema.DefaultFSMountPath, fs.ID)
		}
		volumeMount := corev1.VolumeMount{
			Name:      fs.Name,
			ReadOnly:  fs.ReadOnly,
			MountPath: mountPath,
			SubPath:   fs.SubPath,
		}
		vms = append(vms, volumeMount)
	}
	return vms
}

// appendVolumesIfAbsent append newElements if not exist in volumes
// if job with tasks, it should be like
// `Volumes = appendVolumesIfAbsent(Volumes, generateVolumes(taskFs))`
// otherwise,
// `Volumes = appendVolumesIfAbsent(Volumes, generateVolumes(kubeJob.FileSystems))`
func appendVolumesIfAbsent(volumes []corev1.Volume, newElements []corev1.Volume) []corev1.Volume {
	log.Infof("appendVolumesIfAbsent volumes=%+v, newElements=%+v", volumes, newElements)
	if len(newElements) == 0 {
		return volumes
	}
	if len(volumes) == 0 {
		volumes = []corev1.Volume{}
	}
	volumesDict := make(map[string]bool)
	for _, v := range volumes {
		volumesDict[v.Name] = true
	}
	for _, cur := range newElements {
		if volumesDict[cur.Name] {
			log.Debugf("volume %s has been created in jobTemplate", cur.Name)
			continue
		}
		volumesDict[cur.Name] = true
		volumes = append(volumes, cur)
	}
	return volumes
}

// appendMountsIfAbsent append volumeMount if not exist in volumeMounts
// if job with tasks, it should be like
// `VolumeMounts = appendMountsIfAbsent(VolumeMounts, generateVolumeMounts(taskFs))`
// otherwise,
// `VolumeMounts = appendMountsIfAbsent(VolumeMounts, generateVolumeMounts(kubeJob.FileSystems))`
func appendMountsIfAbsent(volumeMounts []corev1.VolumeMount, newElements []corev1.VolumeMount) []corev1.VolumeMount {
	log.Infof("appendMountsIfAbsent volumeMounts=%+v, newElements=%+v", volumeMounts, newElements)
	if volumeMounts == nil {
		volumeMounts = []corev1.VolumeMount{}
	}
	if len(newElements) == 0 {
		return volumeMounts
	}
	// deduplication
	volumeMountsDict := make(map[string]string)
	for _, cur := range volumeMounts {
		mountPath := filepath.Clean(cur.MountPath)
		volumeMountsDict[mountPath] = cur.Name
	}

	for _, cur := range newElements {
		mountPath := filepath.Clean(cur.MountPath)
		if _, exist := volumeMountsDict[mountPath]; exist {
			log.Debugf("moutPath %s in volumeMount %s has been created in jobTemplate", cur.MountPath, cur.Name)
			continue
		}
		volumeMountsDict[mountPath] = cur.Name
		volumeMounts = append(volumeMounts, cur)
	}
	return volumeMounts
}
