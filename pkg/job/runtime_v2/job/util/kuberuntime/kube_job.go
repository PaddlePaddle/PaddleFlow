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

package kuberuntime

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kubeschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/utils"
	locationAwareness "github.com/PaddlePaddle/PaddleFlow/pkg/fs/location-awareness"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	DefaultReplicas = 1
)

// ResponsibleForJob filter job belong to PaddleFlow
func ResponsibleForJob(obj interface{}) bool {
	job := obj.(*unstructured.Unstructured)
	labels := job.GetLabels()
	if labels != nil && labels[schema.JobOwnerLabel] == schema.JobOwnerValue {
		log.Debugf("responsible for handle job. jobName:[%s]", job.GetName())
		return true
	}
	log.Debugf("responsible for skip job. jobName:[%s]", job.GetName())
	return false
}

// getDefaultTemplate get default template from file
func getDefaultTemplate(framework schema.Framework, jobType schema.JobType, jobMode string) ([]byte, error) {
	// jobTemplateName corresponds to the footer comment of yaml file `config/server/default/job/job_template.yaml`
	jobTemplateName := ""

	//the footer comment of all type job as the follow:
	//  single -> single-job, workflow -> workflow-job,
	//  spark -> spark-job, ray -> ray-job
	//  paddle with ps mode -> paddle-ps-job
	//  paddle with collective mode -> paddle-collective-job
	//  tensorflow with ps mode -> tensorflow-ps-job
	//  pytorch with ps mode -> pytorch-ps-job
	switch jobType {
	case schema.TypeSingle, schema.TypeWorkflow:
		jobTemplateName = fmt.Sprintf("%s-job", jobType)
	case schema.TypeDistributed:
		if framework == schema.FrameworkSpark || framework == schema.FrameworkRay || framework == schema.FrameworkMPI {
			jobTemplateName = fmt.Sprintf("%s-job", framework)
		} else {
			jobTemplateName = fmt.Sprintf("%s-%s-job", framework, strings.ToLower(jobMode))
		}
	default:
		return []byte{}, fmt.Errorf("job type %s is not supported", jobType)
	}

	log.Infof("get default template for job, and template name is %s", jobTemplateName)
	jobTemplate, find := config.DefaultJobTemplate[jobTemplateName]
	if !find {
		return []byte{}, fmt.Errorf("job template %s is not found", jobTemplateName)
	}
	return jobTemplate, nil
}

func CreateKubeJobFromYaml(jobEntity interface{}, groupVersionKind kubeschema.GroupVersionKind, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	log.Debugf("createJobFromYaml jobEntity[%+v] %v", jobEntity, reflect.TypeOf(jobEntity))
	// get extensionTemplate
	if len(job.ExtensionTemplate) == 0 {
		// get builtin template
		var err error
		job.IsCustomYaml = false
		job.ExtensionTemplate, err = getDefaultTemplate(job.Framework, job.JobType, job.JobMode)
		if err != nil {
			return fmt.Errorf("get default template failed, err: %v", err)
		}
	} else {
		// get template from user
		job.IsCustomYaml = true
	}

	// decode []byte into unstructured.Unstructured
	dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	unstructuredObj := &unstructured.Unstructured{}

	if _, _, err := dec.Decode(job.ExtensionTemplate, nil, unstructuredObj); err != nil {
		log.Errorf("Decode from yamlFile[%s] failed! err:[%v]\n", string(job.ExtensionTemplate), err)
		return err
	}
	parsedGVK := unstructuredObj.GroupVersionKind()
	log.Debugf("unstructuredObj=%v, GroupVersionKind=[%v]", unstructuredObj, parsedGVK)
	if parsedGVK.String() != groupVersionKind.String() {
		err := fmt.Errorf("expect GroupVersionKind is %s, but got %s", groupVersionKind.String(), parsedGVK.String())
		log.Errorf("Decode from yamlFile[%s] failed! err:[%v]\n", string(job.ExtensionTemplate), err)
		return err
	}

	// convert unstructuredObj.Object into entity
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.Object, jobEntity); err != nil {
		log.Errorf("convert map struct object[%+v] to acutal job type failed: %v", unstructuredObj.Object, err)
		return err
	}

	log.Debugf("get jobEntity[%+v] from yamlContent[%s]job", jobEntity, job.ExtensionTemplate)
	return nil
}

func ValidatePodResources(spec *corev1.PodSpec) error {
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

// In the bellow, build kubernetes job metadata, spec and so on.

// BuildJobMetadata build metadata for kubernetes job
func BuildJobMetadata(metadata *metav1.ObjectMeta, job *api.PFJob) {
	if metadata == nil || job == nil {
		return
	}
	metadata.Name = job.ID
	metadata.Namespace = job.Namespace
	metadata.Annotations = appendMapsIfAbsent(metadata.Annotations, job.Annotations)
	metadata.Labels = appendMapsIfAbsent(metadata.Labels, job.Labels)
	metadata.Labels[schema.JobOwnerLabel] = schema.JobOwnerValue
	metadata.Labels[schema.JobIDLabel] = job.ID

	if len(job.QueueName) > 0 {
		metadata.Annotations[schema.QueueLabelKey] = job.QueueName
		metadata.Labels[schema.QueueLabelKey] = job.QueueName
	}
}

func BuildTaskMetadata(metadata *metav1.ObjectMeta, jobID string, taskConf *schema.Conf) {
	if metadata == nil || taskConf == nil {
		return
	}
	if taskConf.GetName() != "" {
		metadata.Name = taskConf.GetName()
	}
	if taskConf.GetNamespace() != "" {
		metadata.Namespace = taskConf.GetNamespace()
	}
	metadata.Annotations = appendMapsIfAbsent(metadata.Annotations, taskConf.GetAnnotations())
	metadata.Labels = appendMapsIfAbsent(metadata.Labels, taskConf.GetLabels())
	metadata.Labels[schema.JobIDLabel] = jobID
	metadata.Labels[schema.JobOwnerLabel] = schema.JobOwnerValue
	// TODO: add more metadata for task
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

func BuildPodSpec(podSpec *corev1.PodSpec, task schema.Member) error {
	if podSpec == nil {
		return fmt.Errorf("build pod spec failed, err: podSpec or task is nil")
	}
	// fill priorityClassName and schedulerName
	err := buildPriorityAndScheduler(podSpec, task.Priority)
	if err != nil {
		log.Errorln(err)
		return err
	}
	// fill volumes
	fileSystems := task.Conf.GetAllFileSystem()
	podSpec.Volumes = BuildVolumes(podSpec.Volumes, fileSystems)
	// fill affinity
	if len(fileSystems) != 0 {
		var fsIDs []string
		for _, fs := range fileSystems {
			fsIDs = append(fsIDs, fs.ID)
		}
		podSpec.Affinity, err = generateAffinity(podSpec.Affinity, fsIDs)
		if err != nil {
			return err
		}
	}
	// fill restartPolicy
	patchRestartPolicy(podSpec, task)
	// build containers
	if err = buildPodContainers(podSpec, task); err != nil {
		log.Errorf("failed to fill containers, err=%v", err)
		return err
	}
	return nil
}

func buildPriorityAndScheduler(podSpec *corev1.PodSpec, priorityName string) error {
	if podSpec == nil {
		return fmt.Errorf("build scheduling policy failed, err: podSpec is nil")
	}
	// fill priorityClassName
	podSpec.PriorityClassName = KubePriorityClass(priorityName)
	// fill SchedulerName
	podSpec.SchedulerName = config.GlobalServerConfig.Job.SchedulerName
	return nil
}

func BuildPod(pod *corev1.Pod, task schema.Member) error {
	if pod == nil {
		return fmt.Errorf("build pod failed, err: podSpec is nil")
	}
	// fill priorityClassName and schedulerName
	err := buildPriorityAndScheduler(&pod.Spec, task.Priority)
	if err != nil {
		log.Errorln(err)
		return err
	}
	// fill volumes
	fileSystems := task.Conf.GetAllFileSystem()
	pod.Spec.Volumes = BuildVolumes(pod.Spec.Volumes, fileSystems)
	// fill fs affinity
	if len(fileSystems) != 0 {
		var fsIDs []string
		for _, fs := range fileSystems {
			fsIDs = append(fsIDs, fs.ID)
		}
		pod.Spec.Affinity, err = generateAffinity(pod.Spec.Affinity, fsIDs)
		if err != nil {
			return err
		}
	}
	// fill restartPolicy
	patchRestartPolicy(&pod.Spec, task)

	// patch config for Paddle Para
	_, find := task.Env[schema.EnvPaddleParaJob]
	if find {
		if err = patchPaddlePara(pod, task.Name, task); err != nil {
			log.Errorf("patch parameters for paddle para job failed, err: %v", err)
			return err
		}
	}

	// build containers
	if err = buildPodContainers(&pod.Spec, task); err != nil {
		log.Errorf("failed to fill containers, err=%v", err)
		return err
	}
	return nil
}

func patchRestartPolicy(podSpec *corev1.PodSpec, task schema.Member) {
	if podSpec == nil {
		return
	}
	// fill restartPolicy
	restartPolicy := task.GetRestartPolicy()
	if restartPolicy == string(corev1.RestartPolicyAlways) ||
		restartPolicy == string(corev1.RestartPolicyOnFailure) {
		podSpec.RestartPolicy = corev1.RestartPolicy(restartPolicy)
	} else {
		podSpec.RestartPolicy = corev1.RestartPolicyNever
	}
}

func generateAffinity(affinity *corev1.Affinity, fsIDs []string) (*corev1.Affinity, error) {
	nodeAffinity, err := locationAwareness.FsNodeAffinity(fsIDs)
	if err != nil {
		err = fmt.Errorf("KubeJob generateAffinity err: %v", err)
		log.Errorf(err.Error())
		return nil, err
	}
	return mergeNodeAffinity(affinity, nodeAffinity), nil
}

func mergeNodeAffinity(former, new *corev1.Affinity) *corev1.Affinity {
	if new == nil {
		log.Infof("mergeNodeAffinity new affinity is nil")
		return former
	}
	if former == nil {
		log.Infof("mergeNodeAffinity former affinity is nil")
		return new
	}

	if former.NodeAffinity == nil {
		former.NodeAffinity = new.NodeAffinity
		return former
	}

	// merge required
	newRequired := new.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if newRequired != nil && len(newRequired.NodeSelectorTerms) != 0 {
		formerRequired := former.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if formerRequired == nil || len(formerRequired.NodeSelectorTerms) == 0 {
			formerRequired = newRequired
		} else {
			formerRequired.NodeSelectorTerms = append(formerRequired.NodeSelectorTerms, newRequired.NodeSelectorTerms...)
		}
	}

	// merge preferred
	former.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		former.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
		new.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution...)
	return former
}

func buildPodContainers(podSpec *corev1.PodSpec, task schema.Member) error {
	log.Debugf("fillContainersInPod for job[%s]", task.Name)
	if podSpec.Containers == nil || len(podSpec.Containers) == 0 {
		podSpec.Containers = []corev1.Container{{}}
	}

	// only fill the first container
	index := 0
	if err := fillContainer(&podSpec.Containers[index], task.Name, task); err != nil {
		log.Errorf("fillContainer occur a err[%v]", err)
		return err
	}
	log.Debugf("job[%s].Spec.Tasks=[%+v]", task.Name, podSpec.Containers)
	return nil
}

func fillContainer(container *corev1.Container, podName string, task schema.Member) error {
	log.Debugf("fillContainer for job[%s]", podName)
	// fill name
	if task.Name != "" {
		container.Name = task.Name
	}
	// fill image
	container.Image = task.Image
	// fill command
	filesystems := task.Conf.GetAllFileSystem()
	workDir := getWorkDir(&task, filesystems, task.Env)
	container.Command = generateContainerCommand(task.Command, workDir)

	// container.Args would be passed
	// fill resource
	var err error
	container.Resources, err = GenerateResourceRequirements(task.Flavour, task.LimitFlavour)
	if err != nil {
		log.Errorf("generate resource requirements failed, err: %v", err)
		return err
	}
	// fill env
	container.Env = BuildEnvVars(container.Env, task.Env)
	// fill volumeMount
	container.VolumeMounts = BuildVolumeMounts(container.VolumeMounts, filesystems)

	log.Debugf("fillContainer completed: pod[%s]-container[%s]", podName, container.Name)
	return nil
}

func getWorkDir(task *schema.Member, fileSystems []schema.FileSystem, envs map[string]string) string {
	// prepare fs and envs
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

// generateContainerCommand if task is not nil, prefer to using info in task, otherwise using job's
func generateContainerCommand(command string, workdir string) []string {
	command = strings.TrimPrefix(command, "bash -c")
	command = strings.TrimPrefix(command, "sh -c")

	if workdir != "" {
		// if command is not empty
		if command != "" {
			command = fmt.Sprintf("cd %s; %s", workdir, command)
		} else {
			command = fmt.Sprintf("cd %s", workdir)
		}
	}

	commands := []string{"sh", "-c", command}
	return commands
}

func GenerateResourceRequirements(request, limitFlavour schema.Flavour) (corev1.ResourceRequirements, error) {
	log.Infof("GenerateResourceRequirements by request:[%+v]", request)

	flavourResource, err := resources.NewResourceFromMap(request.ToMap())
	if err != nil {
		log.Errorf("GenerateResourceRequirements by request:[%+v] error:%v", request, err)
		return corev1.ResourceRequirements{}, err
	}

	limitFlavourResource, err := resources.NewResourceFromMap(limitFlavour.ToMap())
	if err != nil {
		log.Errorf("GenerateResourceRequirements by limitFlavour:[%+v] error:%v", request, err)
		return corev1.ResourceRequirements{}, err
	}

	resources := corev1.ResourceRequirements{
		Requests: k8s.NewResourceList(flavourResource),
	}
	if strings.ToUpper(limitFlavour.Name) == schema.EnvJobLimitFlavourNone {
		resources.Limits = nil
	} else if limitFlavourResource.CPU() == 0 || limitFlavourResource.Memory() == 0 {
		// limit set zero, patch the same value as request
		resources.Limits = k8s.NewResourceList(flavourResource)
	} else {
		// limit set specified value
		resources.Limits = k8s.NewResourceList(limitFlavourResource)
	}
	return resources, nil
}

// appendEnvIfAbsent append new env if not exist in baseEnvs
func appendEnvIfAbsent(baseEnvs []corev1.EnvVar, addEnvs []corev1.EnvVar) []corev1.EnvVar {
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

func generateEnvVars(EnvVars map[string]string) []corev1.EnvVar {
	envs := make([]corev1.EnvVar, 0)
	for key, value := range EnvVars {
		env := corev1.EnvVar{
			Name:  key,
			Value: value,
		}
		envs = append(envs, env)
	}
	return envs
}

// BuildEnvVars merge EnvVars
func BuildEnvVars(baseEnvs []corev1.EnvVar, EnvVars map[string]string) []corev1.EnvVar {
	return appendEnvIfAbsent(baseEnvs, generateEnvVars(EnvVars))
}

// BuildVolumes convert PaddleFlow FileSystem to kubernetes volumes
func BuildVolumes(volumes []corev1.Volume, fileSystem []schema.FileSystem) []corev1.Volume {
	return appendVolumesIfAbsent(volumes, generateVolumes(fileSystem))
}

// BuildVolumeMounts covert PaddleFlow FileSystem to kubernetes VolumeMount
func BuildVolumeMounts(volumeMounts []corev1.VolumeMount, fileSystem []schema.FileSystem) []corev1.VolumeMount {
	return appendMountsIfAbsent(volumeMounts, generateVolumeMounts(fileSystem))
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

// TODO: add TransferFS interface on runtime
// generateVolumes generate kubernetes volumes with schema.FileSystem
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
		}
		if fs.Type == schema.PFSTypeLocal {
			// use hostPath
			volume.VolumeSource = corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: fs.HostPath,
				},
			}
		} else {
			// use pvc
			volume.VolumeSource = corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: schema.ConcatenatePVCName(fs.ID),
				},
			}
		}
		vs = append(vs, volume)
	}

	return vs
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

// TODO: add TransferFS interface on runtime
// generateVolumeMounts generate kubernetes volumeMounts with schema.FileSystem
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
			fs.MountPath = filepath.Join(schema.DefaultFSMountPath, fs.ID)
		}
		mp := corev1.MountPropagationHostToContainer
		volumeMount := corev1.VolumeMount{
			Name:      fs.Name,
			ReadOnly:  fs.ReadOnly,
			MountPath: fs.MountPath,
			SubPath:   fs.SubPath,
		}
		if fs.Type != schema.PFSTypeLocal {
			volumeMount.MountPropagation = &mp
		}
		vms = append(vms, volumeMount)
	}
	return vms
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

// patchPaddlePara patch some parameters for paddle para job, and must be work with a shared gpu device plugin
// environments for paddle para job:
//   PF_PADDLE_PARA_JOB: defines the job is a paddle para job
//   PF_PADDLE_PARA_PRIORITY: defines the priority of paddle para job, 0 is high, and 1 is low.
//   PF_PADDLE_PARA_CONFIG_FILE: defines the config of paddle para job
func patchPaddlePara(podTemplate *corev1.Pod, jobName string, task schema.Member) error {
	// get parameters from user's job config
	var paddleParaPriority string
	// TODO: use task.Conf.Getxxx method
	p := task.Env[schema.EnvPaddleParaPriority]
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
	value, find := task.Env[schema.EnvPaddleParaConfigHostFile]
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

// GetKubeflowJobStatus covert job status of kubeflow application to paddleflow job status
func GetKubeflowJobStatus(jobCond kubeflowv1.JobCondition) (schema.JobStatus, string, error) {
	status := schema.JobStatus("")
	msg := jobCond.Message
	switch jobCond.Type {
	case kubeflowv1.JobCreated:
		status = schema.StatusJobPending
	case kubeflowv1.JobRunning, kubeflowv1.JobRestarting:
		status = schema.StatusJobRunning
	case kubeflowv1.JobSucceeded:
		status = schema.StatusJobSucceeded
	case kubeflowv1.JobFailed:
		status = schema.StatusJobFailed
	default:
		return status, msg, fmt.Errorf("unexpected job status: %s", jobCond.Type)
	}
	return status, msg, nil
}

// BuildPodTemplateSpec build PodTemplateSpec for built-in distributed job, such as PaddleJob, PyTorchJob, TFJob and so on
func BuildPodTemplateSpec(podSpec *corev1.PodTemplateSpec, jobID string, task *schema.Member) error {
	if podSpec == nil || task == nil {
		return fmt.Errorf("podTemplateSpec or task is nil")
	}
	// build task metadata
	BuildTaskMetadata(&podSpec.ObjectMeta, jobID, &schema.Conf{})
	// build pod spec
	err := BuildPodSpec(&podSpec.Spec, *task)
	if err != nil {
		log.Errorf("build pod spec failed, err: %v", err)
		return err
	}
	return nil
}

// KubeflowReplicaSpec build ReplicaSpec for kubeflow job, such as PyTorchJob, TFJob and so on.
func KubeflowReplicaSpec(replicaSpec *kubeflowv1.ReplicaSpec, jobID string, task *schema.Member) error {
	if replicaSpec == nil || task == nil {
		return fmt.Errorf("build kubeflow replica spec failed, err: replicaSpec or task is nil")
	}
	// set Replicas for job
	replicas := int32(task.Replicas)
	replicaSpec.Replicas = &replicas
	// set RestartPolicy
	// TODO: make RestartPolicy configurable
	replicaSpec.RestartPolicy = kubeflowv1.RestartPolicyNever
	// TODO: remove hard coded schedulerName when upstream package is fixed
	// HARD CODE schedulerName to default scheduler, fix KubeFlow training operator bug at volcano scheduler TEMPERATELY
	// see issue https://github.com/kubeflow/training-operator/issues/1630
	replicaSpec.Template.Spec.SchedulerName = "default-scheduler"
	// set PodTemplate
	return BuildPodTemplateSpec(&replicaSpec.Template, jobID, task)
}

// KubeflowRunPolicy build RunPolicy for kubeflow job, such as PyTorchJob, TFJob and so on.
func KubeflowRunPolicy(runPolicy *kubeflowv1.RunPolicy, minResources *corev1.ResourceList, queueName, priority string) error {
	if runPolicy == nil {
		return fmt.Errorf("build run policy for kubeflow job faield, err: runPolicy is nil")
	}
	// TODO set cleanPolicy
	// set SchedulingPolicy
	if runPolicy.SchedulingPolicy == nil {
		runPolicy.SchedulingPolicy = &kubeflowv1.SchedulingPolicy{}
	}
	runPolicy.SchedulingPolicy.Queue = queueName
	runPolicy.SchedulingPolicy.PriorityClass = KubePriorityClass(priority)
	if minResources != nil {
		runPolicy.SchedulingPolicy.MinResources = minResources
	}
	return nil
}

// Operations for kubernetes job, including single, paddle, sparkapp, tensorflow, pytorch, mpi jobs and so on.

// getPodGroupName get the name of pod group
func getPodGroupName(jobID string) string {
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
		if pgName == "" {
			pgName = fmt.Sprintf("podgroup-%s", jobObj.GetUID())
		}
	default:
		log.Warningf("the framework[%s] of job is not supported", job.Framework)
		pgName = jobID
	}
	return pgName
}

func updateKubeJobPriority(jobInfo *api.PFJob, runtimeClient framework.RuntimeClientInterface) error {
	// get pod group name for job
	pgName := getPodGroupName(jobInfo.ID)
	if len(pgName) == 0 {
		err := fmt.Errorf("update priority for job %s failed, pod group not found", jobInfo.ID)
		log.Errorln(err)
		return err
	}
	frameworkVersion := schema.NewFrameworkVersion(k8s.PodGroupGVK.Kind, k8s.PodGroupGVK.GroupVersion().String())
	obj, err := runtimeClient.Get(jobInfo.Namespace, pgName, frameworkVersion)
	if err != nil {
		log.Errorf("get pod group for job %s failed, err: %v", jobInfo.ID, err)
		return err
	}
	unObj := obj.(*unstructured.Unstructured)
	oldPG := &schedulingv1beta1.PodGroup{}
	if err = runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, oldPG); err != nil {
		log.Errorf("convert unstructured object [%v] to pod group failed. err: %v", obj, err)
		return err
	}
	if oldPG.Status.Phase != schedulingv1beta1.PodGroupInqueue &&
		oldPG.Status.Phase != schedulingv1beta1.PodGroupPending {
		errmsg := fmt.Errorf("the job %s is already scheduled", jobInfo.ID)
		log.Errorln(errmsg)
		return errmsg
	}

	priorityClassName := KubePriorityClass(jobInfo.PriorityClassName)
	if oldPG.Spec.PriorityClassName != priorityClassName {
		oldPG.Spec.PriorityClassName = priorityClassName
	} else {
		err = fmt.Errorf("the priority of job %s is already %s", jobInfo.ID, oldPG.Spec.PriorityClassName)
		log.Errorln(err)
		return err
	}

	err = runtimeClient.Update(oldPG, frameworkVersion)
	if err != nil {
		log.Errorf("update priority for job %s failed. err: %v", jobInfo.ID, err)
	}
	return err
}

func UpdateKubeJob(job *api.PFJob, runtimeClient framework.RuntimeClientInterface, fv schema.FrameworkVersion) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	jobmsg := fmt.Sprintf("%s job %s on %s", fv.String(), job.NamespacedName(), runtimeClient.Cluster())
	//  1. update job priority
	if len(job.PriorityClassName) != 0 {
		err := updateKubeJobPriority(job, runtimeClient)
		if err != nil {
			log.Errorf("update %s failed, err: %v", jobmsg, err)
			return err
		}
	}
	// 2. update job labels or annotations
	if (job.Labels != nil && len(job.Labels) != 0) ||
		(job.Annotations != nil && len(job.Annotations) != 0) {
		patchJSON := struct {
			metav1.ObjectMeta `json:"metadata,omitempty"`
		}{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      job.Labels,
				Annotations: job.Annotations,
			},
		}
		updateData, err := json.Marshal(patchJSON)
		if err != nil {
			log.Errorf("update kubernetes job[%s] failed, err: %v", job.ID, err)
			return err
		}
		log.Infof("begin to update %s, data: %s", jobmsg, string(updateData))
		if err = runtimeClient.Patch(job.Namespace, job.ID, fv, updateData); err != nil {
			log.Errorf("update %s failed, err: %v", jobmsg, err)
			return err
		}
	}
	return nil
}
