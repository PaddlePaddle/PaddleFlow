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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kubeschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/utils"
	locationAwareness "github.com/PaddlePaddle/PaddleFlow/pkg/fs/location-awareness"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
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
	Tasks               []schema.Member
	GroupVersionKind    kubeschema.GroupVersionKind
	DynamicClientOption *k8s.DynamicClientOption
}

func NewKubeJob(job *api.PFJob, dynamicClientOpt *k8s.DynamicClientOption) (api.PFJobInterface, error) {
	log.Debugf("create kubernetes job: %#v", job)
	kubeJob := KubeJob{
		ID:                  job.ID,
		Name:                job.ID,
		Namespace:           job.Namespace,
		JobType:             job.JobType,
		JobMode:             job.JobMode,
		Image:               job.Conf.GetImage(),
		Command:             job.Conf.GetCommand(),
		Env:                 job.Conf.GetEnv(),
		Labels:              job.Conf.Labels,
		Annotations:         job.Conf.Annotations,
		FileSystems:         job.Conf.GetAllFileSystem(),
		Framework:           job.Framework,
		Tasks:               job.Tasks,
		Priority:            job.Conf.GetPriority(),
		QueueName:           job.Conf.GetQueueName(),
		DynamicClientOption: dynamicClientOpt,
		YamlTemplateContent: job.ExtensionTemplate,
	}

	switch job.JobType {
	case schema.TypeVcJob:
		// todo(zhongzichao): to be removed
		kubeJob.GroupVersionKind = k8s.VCJobGVK
		if len(job.Tasks) == 0 {
			kubeJob.Tasks = []schema.Member{
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
		// TODO: use k8s.MPIJobGVK
		kubeJob.GroupVersionKind = k8s.VCJobGVK
		return &VCJob{
			KubeJob: kubeJob,
		}, nil
	case schema.FrameworkPaddle:
		kubeJob.GroupVersionKind = k8s.PaddleJobGVK
		return &PaddleJob{
			KubeJob: kubeJob,
		}, nil
	case schema.FrameworkPytorch:
		kubeJob.GroupVersionKind = k8s.PyTorchJobGVK
		return &PyTorchJob{
			KubeJob: kubeJob,
		}, nil
	case schema.FrameworkTF:
		kubeJob.GroupVersionKind = k8s.TFJobGVK
		return &TFJob{
			KubeJob: kubeJob,
		}, nil
	case schema.FrameworkRay:
		kubeJob.GroupVersionKind = k8s.RayJobGVK
		return &RayJob{
			KubeJob: kubeJob,
		}, nil
	default:
		return nil, fmt.Errorf("kubernetes job framework[%s] is not supported", job.Framework)
	}
}

func (j *KubeJob) String() string {
	return fmt.Sprintf("%s job %s/%s", j.GroupVersionKind.String(), j.Namespace, j.Name)
}

func (j *KubeJob) Cluster() string {
	clusterName := ""
	if j.DynamicClientOption != nil && j.DynamicClientOption.ClusterInfo != nil {
		clusterName = j.DynamicClientOption.ClusterInfo.Name
	}
	return clusterName
}

func (j *KubeJob) setAffinity(podSpec *corev1.PodSpec) error {
	if len(j.FileSystems) != 0 {
		var fsIDs []string
		for _, fs := range j.FileSystems {
			fsIDs = append(fsIDs, fs.ID)
		}
		var err error
		podSpec.Affinity, err = j.generateAffinity(podSpec.Affinity, fsIDs)
		if err != nil {
			log.Errorf("set affinity for %s failed with fsIDs %v, err: %v", j.String(), fsIDs, err)
			return err
		}
	}
	return nil
}

func (j *KubeJob) generateAffinity(affinity *corev1.Affinity, fsIDs []string) (*corev1.Affinity, error) {
	nodeAffinity, err := locationAwareness.FsNodeAffinity(fsIDs)
	if err != nil {
		err := fmt.Errorf("KubeJob generateAffinity err: %v", err)
		log.Errorf(err.Error())
		return nil, err
	}
	if nodeAffinity == nil {
		log.Warningf("fs %v location awareness has no node affinity", fsIDs)
		return affinity, nil
	}
	log.Infof("KubeJob with fs %v generate node affinity: %v", fsIDs, *nodeAffinity)
	// merge filesystem location awareness affinity to pod affinity
	if affinity == nil {
		return nodeAffinity, nil
	}
	if affinity.NodeAffinity == nil {
		affinity.NodeAffinity = nodeAffinity.NodeAffinity
	} else {
		affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
			nodeAffinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
			affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution...)
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(
			nodeAffinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
			affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms...)
	}
	return affinity, nil
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

func (j *KubeJob) generateTaskEnvVars(taskEnvs map[string]string) []corev1.EnvVar {
	envs := make([]corev1.EnvVar, 0)
	for key, value := range taskEnvs {
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
	parsedGVK := unstructuredObj.GroupVersionKind()
	log.Debugf("unstructuredObj=%v, GroupVersionKind=[%v]", unstructuredObj, parsedGVK)
	if parsedGVK.String() != j.GroupVersionKind.String() {
		err := fmt.Errorf("expect GroupVersionKind is %s, but got %s", j.GroupVersionKind.String(), parsedGVK.String())
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
func (j *KubeJob) fillPodSpec(podSpec *corev1.PodSpec, task *schema.Member) error {
	if task != nil {
		j.Priority = task.Priority
		podSpec.Volumes = appendVolumesIfAbsent(podSpec.Volumes, generateVolumes(task.GetAllFileSystem()))
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
	if err := j.setAffinity(podSpec); err != nil {
		log.Errorf("setAffinity for %s failed, err: %v", j.String(), err)
		return err
	}
	return nil
}

// fillContainerInTasks fill container in job task
func (j *KubeJob) fillContainerInTasks(container *corev1.Container, task schema.Member) error {
	if j.isNeedPatch(container.Image) {
		container.Image = task.Image
	}
	if task.Name != "" {
		container.Name = task.Name
	}

	j.fillCMDInContainer(container, &task)

	if !j.IsCustomYaml && len(task.Args) > 0 {
		container.Args = task.Args
	}
	var err error
	container.Resources, err = j.generateResourceRequirements(task.Flavour)
	if err != nil {
		log.Errorf("fillContainerInTasks failed when generateResourceRequirements, err: %v", err)
		return err
	}
	taskFs := task.Conf.GetAllFileSystem()
	if len(taskFs) != 0 {
		container.VolumeMounts = appendMountsIfAbsent(container.VolumeMounts, generateVolumeMounts(taskFs))
	}

	container.Env = j.appendEnvIfAbsent(container.Env, j.generateTaskEnvVars(task.Env))
	return nil
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

// fillCMDInContainer fill command in container by task.Command or job.Command
func (j *KubeJob) fillCMDInContainer(container *corev1.Container, task *schema.Member) {
	// get workdir
	workDir := j.getWorkDir(task)
	// get command
	command := j.Command
	if task != nil {
		command = task.Command
	}
	// only command is set, should we add workdir to command
	if j.isNeedPatch(command) && command != "" {
		container.Command = j.generateContainerCommand(command, workDir)
	}
}

// generateContainerCommand if task is not nil, prefer to using info in task, otherwise using job's
func (j *KubeJob) generateContainerCommand(command string, workdir string) []string {
	command = strings.TrimPrefix(command, "bash -c")
	command = strings.TrimPrefix(command, "sh -c")

	if workdir != "" {
		workdir = utils.MountPathClean(workdir)
		command = fmt.Sprintf("%s %s;%s", "cd", workdir, command)
	}

	commands := []string{"sh", "-c", command}
	return commands
}

func (j *KubeJob) getWorkDir(task *schema.Member) string {
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
	mountPath := utils.MountPathClean(fileSystems[0].MountPath)
	log.Infof("getWorkDir by hasWorkDir: true,mountPath: %s, task: %v", mountPath, task)
	if mountPath != "/" {
		workdir = fileSystems[0].MountPath
	} else {
		workdir = filepath.Join(schema.DefaultFSMountPath, fileSystems[0].ID)
	}
	envs[schema.EnvJobWorkDir] = workdir
	return workdir
}

func (j *KubeJob) generateResourceRequirements(flavour schema.Flavour) (corev1.ResourceRequirements, error) {
	log.Infof("generateResourceRequirements by flavour:[%+v]", flavour)

	flavourResource, err := resources.NewResourceFromMap(flavour.ToMap())
	if err != nil {
		log.Errorf("generateResourceRequirements by flavour:[%+v] error:%v", flavour, err)
		return corev1.ResourceRequirements{}, err
	}
	resources := corev1.ResourceRequirements{
		Requests: k8s.NewResourceList(flavourResource),
		Limits:   k8s.NewResourceList(flavourResource),
	}

	return resources, nil
}

func (j *KubeJob) patchMetadata(metadata *metav1.ObjectMeta, name string) {
	if name != "" {
		metadata.Name = name
	}
	metadata.Namespace = j.Namespace
	metadata.Annotations = j.appendAnnotationsIfAbsent(metadata.Annotations, j.Annotations)
	metadata.Labels = j.appendLabelsIfAbsent(metadata.Labels, j.Labels)
	metadata.Labels[schema.JobOwnerLabel] = schema.JobOwnerValue
	metadata.Labels[schema.JobIDLabel] = j.ID
}

func (j *KubeJob) patchTaskMetadata(metadata *metav1.ObjectMeta, member schema.Member) {
	if member.Name != "" {
		metadata.Name = member.Name
	}
	metadata.Namespace = j.Namespace
	metadata.Annotations = j.appendAnnotationsIfAbsent(metadata.Annotations, member.Annotations)
	metadata.Labels = j.appendLabelsIfAbsent(metadata.Labels, member.Labels)
	metadata.Labels[schema.JobOwnerLabel] = schema.JobOwnerValue
	metadata.Labels[schema.JobIDLabel] = j.ID
}

func (j *KubeJob) fillPodTemplateSpec(pod *corev1.PodTemplateSpec, member schema.Member) error {
	// ObjectMeta
	j.patchTaskMetadata(&pod.ObjectMeta, member)
	pod.Labels[schema.QueueLabelKey] = member.QueueName
	// fill volumes
	if err := j.fillPodSpec(&pod.Spec, &member); err != nil {
		err = fmt.Errorf("fill pod.Spec failed, err:%v", err)
		log.Errorln(err)
		return err
	}
	// Template.Containers
	switch len(pod.Spec.Containers) {
	case 0:
		pod.Spec.Containers = []corev1.Container{{}}
	case 1:
		break
	default:
		log.Warningf("support filling only one container in rayJobSpec.RayClusterSpec.HeadGroupSpec now, "+
			"current job got %d containers", len(pod.Spec.Containers))
	}
	if err := j.fillContainerInTasks(&pod.Spec.Containers[0], member); err != nil {
		log.Errorf("fill container in task failed, err=[%v]", err)
		return err
	}

	return nil
}

func (j *KubeJob) isNeedPatch(v string) bool {
	return !j.IsCustomYaml
}

func (j *KubeJob) CreateJob() (string, error) {
	return "", nil
}

func (j *KubeJob) StopJobByID(id string) error {
	log.Infof("stop %s on cluster %s", j.String(), j.Cluster())
	if err := Delete(j.Namespace, id, j.GroupVersionKind, j.DynamicClientOption); err != nil {
		log.Errorf("stop %s on cluster %s failed, err: %v", j.String(), j.Cluster(), err)
		return err
	}
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
	job, err := storage.Job.GetJobByID(jobID)
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
	case schema.FrameworkPaddle, schema.FrameworkPytorch, schema.FrameworkTF, schema.FrameworkMXNet:
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
		// e.g. basedir/spark.yaml, basedir/paddle_ps.yaml, basedir/tensorflow.yaml basedir/pytorch.yaml
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
		mountPath := utils.MountPathClean(fs.MountPath)
		if mountPath == "/" {
			mountPath = filepath.Join(schema.DefaultFSMountPath, fs.ID)
		}
		mp := corev1.MountPropagationHostToContainer
		volumeMount := corev1.VolumeMount{
			Name:             fs.Name,
			ReadOnly:         fs.ReadOnly,
			MountPath:        fs.MountPath,
			SubPath:          fs.SubPath,
			MountPropagation: &mp,
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
		mountPath := utils.MountPathClean(cur.MountPath)
		volumeMountsDict[mountPath] = cur.Name
	}

	for _, cur := range newElements {
		mountPath := utils.MountPathClean(cur.MountPath)
		if _, exist := volumeMountsDict[mountPath]; exist {
			log.Debugf("moutPath %s in volumeMount %s has been created in jobTemplate", cur.MountPath, cur.Name)
			continue
		}
		volumeMountsDict[mountPath] = cur.Name
		volumeMounts = append(volumeMounts, cur)
	}
	return volumeMounts
}

func validateTemplateResources(spec *corev1.PodSpec) error {
	for index, container := range spec.Containers {
		resourcesList := k8s.NewMinResourceList()

		if container.Resources.Requests.Cpu().IsZero() || container.Resources.Requests.Memory().IsZero() {
			spec.Containers[index].Resources.Requests = resourcesList
			spec.Containers[index].Resources.Limits = resourcesList
			log.Warnf("podSpec %v container %d cpu is zero, Resources: %v", spec, index, spec.Containers[index].Resources.Requests)
		}

	}
	return nil
}
