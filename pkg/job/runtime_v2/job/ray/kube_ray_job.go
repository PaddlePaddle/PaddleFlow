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

package ray

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	rayV1alpha1 "github.com/PaddlePaddle/PaddleFlow/pkg/apis/ray-operator/v1alpha1"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/util/kuberuntime"
)

var (
	JobGVK           = k8s.RayJobGVK
	KubeRayFwVersion = client.KubeFrameworkVersion(JobGVK)
)

// KubeRayJob is a struct that runs a ray job
type KubeRayJob struct {
	kuberuntime.KubeBaseJob
}

func New(kubeClient framework.RuntimeClientInterface) framework.JobInterface {
	return &KubeRayJob{
		KubeBaseJob: kuberuntime.NewKubeBaseJob(JobGVK, KubeRayFwVersion, kubeClient),
	}
}

func (rj *KubeRayJob) Submit(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	rayJob := &rayV1alpha1.RayJob{}
	if err := kuberuntime.CreateKubeJobFromYaml(rayJob, rj.GVK, job); err != nil {
		log.Errorf("create %s failed, err %v", rj.String(jobName), err)
		return err
	}

	var err error
	// set metadata field
	kuberuntime.BuildJobMetadata(&rayJob.ObjectMeta, job)
	// set spec field
	if job.IsCustomYaml {
		// set custom ray Spec from user
		err = rj.customRayJobSpec(&rayJob.Spec, job)
	} else {
		// set builtin ray Spec
		err = rj.builtinRayJobSpec(&rayJob.Spec, job)
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", rj.String(jobName), err)
		return err
	}
	log.Debugf("begin to create %s, job info: %v", rj.String(jobName), rayJob)
	err = rj.RuntimeClient.Create(rayJob, rj.FrameworkVersion)
	if err != nil {
		log.Errorf("create %s failed, err %v", rj.String(jobName), err)
		return err
	}
	return nil
}

// builtinRayJobSpec set build-in RayJob spec
func (rj *KubeRayJob) builtinRayJobSpec(rayJobSpec *rayV1alpha1.RayJobSpec, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Debugf("patch %s spec:%#v", rj.String(jobName), rayJobSpec)

	workerIndex := 0
	rayWorkersLength := len(rayJobSpec.RayClusterSpec.WorkerGroupSpecs)
	for _, member := range job.Tasks {
		var err error
		if member.Role == pfschema.RoleMaster {
			// keep the same style with other Framework Job, the command store in master task
			fillRayJobSpec(rayJobSpec, member)
			// head
			err = rj.buildHeadPod(rayJobSpec, job.ID, member)
		} else {
			//worker
			err = rj.buildWorkerPod(rayJobSpec, job.ID, member, workerIndex, rayWorkersLength)
			workerIndex++
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func fillRayJobSpec(rayJobSpec *rayV1alpha1.RayJobSpec, task pfschema.Member) {
	// 	Entrypoint string `json:"entrypoint"`
	rayJobSpec.Entrypoint = task.Command
	//	RuntimeEnv string `json:"runtimeEnv,omitempty"`
	if runtimeEnv, exist := task.Env[pfschema.EnvRayJobRuntimeEnv]; exist {
		rayJobSpec.RuntimeEnv = runtimeEnv
	}
	//	ShutdownAfterJobFinishes bool `json:"shutdownAfterJobFinishes,omitempty"`
	rayJobSpec.ShutdownAfterJobFinishes = true
	//	Todo: Metadata map[string]string `json:"metadata,omitempty"`
	//	Todo: TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
	//	Todo: RayClusterSpec RayClusterSpec `json:"rayClusterSpec,omitempty"`
	//	Todo: ClusterSelector map[string]string `json:"clusterSelector,omitempty"`
}

func (rj *KubeRayJob) buildHeadPod(rayJobSpec *rayV1alpha1.RayJobSpec, jobID string, task pfschema.Member) error {
	// Todo: ServiceType is Kubernetes service type. it will be used by the workers to connect to the head pod
	// Todo: it also need to be validate in validateJob
	// Todo: EnableIngress indicates whether operator should create ingress object for head service or not.

	headGroupSpec := &rayJobSpec.RayClusterSpec.HeadGroupSpec
	// RayStartParams is start command: node-manager-port, object-store-memory, ...
	if headGroupSpec.RayStartParams == nil {
		headGroupSpec.RayStartParams = make(map[string]string)
	}
	for _, argv := range task.Args {
		paramName, paramValue := parseRayArgs(argv)
		headGroupSpec.RayStartParams[paramName] = paramValue
	}
	// remove command args, which is not necessary in ray headGroupSpec
	task.Command = ""
	task.Args = []string{}
	// Template
	if err := kuberuntime.BuildPodTemplateSpec(&headGroupSpec.Template, jobID, &task); err != nil {
		log.Errorf("build head pod spec failed, err:%v", err)
		return err
	}
	// patch queue name
	headGroupSpec.Template.Labels[pfschema.QueueLabelKey] = task.QueueName
	headGroupSpec.Template.Annotations[pfschema.QueueLabelKey] = task.QueueName
	if preStop, exist := task.Env[pfschema.EnvRayJobHeaderPreStop]; exist {
		for index, container := range headGroupSpec.Template.Spec.Containers {
			if container.Name != "autoscaler" {
				if container.Lifecycle == nil {
					container.Lifecycle = &v1.Lifecycle{}
				}
				container.Lifecycle.PreStop = &v1.Handler{
					Exec: &v1.ExecAction{
						// TODO : make it configurable
						Command: []string{"sh", "-c", preStop},
					},
				}
				headGroupSpec.Template.Spec.Containers[index] = container
			}
		}
	}

	return nil
}

func (rj *KubeRayJob) buildWorkerPod(rayJobSpec *rayV1alpha1.RayJobSpec, jobID string, task pfschema.Member,
	workerIndex int, rayWorkersLength int) error {
	if task.Env == nil {
		task.Env = make(map[string]string)
	}
	var worker rayV1alpha1.WorkerGroupSpec
	// use exist workerSpec in template
	if workerIndex < rayWorkersLength {
		worker = rayJobSpec.RayClusterSpec.WorkerGroupSpecs[workerIndex]
	}
	// RayStartParams
	if worker.RayStartParams == nil {
		worker.RayStartParams = make(map[string]string)
	}
	for _, argv := range task.Args {
		paramName, paramValue := parseRayArgs(argv)
		worker.RayStartParams[paramName] = paramValue
	}
	//	Todo: ScaleStrategy defines which pods to remove
	// GroupName
	if groupName, exist := task.Env[pfschema.EnvRayJobWorkerGroupName]; exist {
		worker.GroupName = groupName
	}
	// Replicas
	replicas := int32(task.Replicas)
	worker.Replicas = &replicas
	// minReplicas
	if val, exist, err := getInt32FromEnv(task.Env, pfschema.EnvRayJobWorkerMinReplicas); err != nil {
		err = fmt.Errorf("get minReplicas failed, err: %s", err)
		log.Error(err)
		return err
	} else if exist {
		worker.MinReplicas = &val
	}
	// maxReplicas
	if val, exist, err := getInt32FromEnv(task.Env, pfschema.EnvRayJobWorkerMaxReplicas); err != nil {
		err = fmt.Errorf("get maxReplicas failed, err: %s", err)
		log.Error(err)
		return err
	} else if exist {
		worker.MaxReplicas = &val
	}

	// remove command args, which is not necessary in ray workerGroupSpec
	task.Command = ""
	task.Args = []string{}
	// Template
	if err := kuberuntime.BuildPodTemplateSpec(&worker.Template, jobID, &task); err != nil {
		log.Errorf("build head pod spec failed, err: %v", err)
		return err
	}
	// patch queue name
	worker.Template.Labels[pfschema.QueueLabelKey] = task.QueueName
	worker.Template.Annotations[pfschema.QueueLabelKey] = task.QueueName
	// worker save into WorkerGroupSpecs finally
	if workerIndex < rayWorkersLength {
		rayJobSpec.RayClusterSpec.WorkerGroupSpecs[workerIndex] = worker
	} else {
		rayJobSpec.RayClusterSpec.WorkerGroupSpecs = append(rayJobSpec.RayClusterSpec.WorkerGroupSpecs, worker)
	}
	return nil
}

func getInt32FromEnv(env map[string]string, key string) (int32, bool, error) {
	if valueStr, exist := env[key]; exist {
		valInt, err := strconv.Atoi(valueStr)
		if err != nil {
			err := fmt.Errorf("%s is not valid int type, err: %s", key, err)
			log.Error(err)
			return 0, true, err
		}
		valInt32 := int32(valInt)
		return valInt32, true, nil
	}
	return 0, false, nil
}

func parseRayArgs(argv string) (string, string) {
	args := strings.SplitN(argv, ":", 2)
	if len(args) != 2 {
		return "", ""
	}
	rayStartParam := strings.TrimSpace(args[1])
	rayStartParam = strings.TrimPrefix(rayStartParam, "'")
	rayStartParam = strings.TrimSuffix(rayStartParam, "'")
	return args[0], rayStartParam
}

// customRayJobSpec set custom RayJob Spec
func (rj *KubeRayJob) customRayJobSpec(rayJobSpec *rayV1alpha1.RayJobSpec, job *api.PFJob) error {
	if job == nil || rayJobSpec == nil {
		return fmt.Errorf("job or rayJobSpec is nil")
	}
	jobName := job.NamespacedName()
	log.Debugf("patch %s spec:%#v", rj.String(jobName), rayJobSpec)
	// patch metadata
	kuberuntime.BuildTaskMetadata(&rayJobSpec.RayClusterSpec.HeadGroupSpec.Template.ObjectMeta, job.ID, &pfschema.Conf{})
	for i := range rayJobSpec.RayClusterSpec.WorkerGroupSpecs {
		kuberuntime.BuildTaskMetadata(&rayJobSpec.RayClusterSpec.WorkerGroupSpecs[i].Template.ObjectMeta, job.ID, &pfschema.Conf{})
	}
	// TODO: patch ray job from user
	return nil
}

func (rj *KubeRayJob) AddEventListener(ctx context.Context, listenerType string, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeJob:
		err = rj.AddJobEventListener(ctx, jobQueue, listener, rj.JobStatus, nil)
	default:
		err = fmt.Errorf("listenerType %s is not supported", listenerType)
	}
	return err
}

// JobStatus get the statusInfo of PyTorch job, including origin status, PaddleFlow status and message
func (rj *KubeRayJob) JobStatus(obj interface{}) (api.StatusInfo, error) {
	unObj := obj.(*unstructured.Unstructured)
	jobName := unObj.GetName()
	// convert to RayJob struct
	job := &rayV1alpha1.RayJob{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, job); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s job failed. error: %s", obj, rj.String(jobName), err)
		return api.StatusInfo{}, err
	}
	// convert job status
	state, msg, err := getJobStatus(&job.Status)
	if err != nil {
		log.Errorf("get ray job %s status failed, err: %v", rj.String(jobName), err)
		return api.StatusInfo{}, err
	}
	log.Infof("ray job %s status: %s", rj.String(jobName), state)
	return api.StatusInfo{
		OriginStatus: string(state),
		Status:       state,
		Message:      msg,
	}, nil
}

func getJobStatus(rayJobStatus *rayV1alpha1.RayJobStatus) (pfschema.JobStatus, string, error) {
	status := pfschema.JobStatus("")
	msg := ""
	jobStatus := rayJobStatus.JobStatus
	switch jobStatus {
	// TODO: JobStatusStopped should be processed in another way when the feature "unused status Stopped" is fixed by ray-operator
	case rayV1alpha1.JobStatusPending, rayV1alpha1.JobStatusStopped:
		status, msg = pfschema.StatusJobPending, "ray job is pending"
	case rayV1alpha1.JobStatusRunning:
		status, msg = pfschema.StatusJobRunning, "ray job is running"
	case rayV1alpha1.JobStatusSucceeded:
		status, msg = pfschema.StatusJobSucceeded, "ray job is succeeded"
	case rayV1alpha1.JobStatusFailed:
		status = pfschema.StatusJobFailed
		msg = getRayJobMessage(rayJobStatus)
	default:
		return status, msg, fmt.Errorf("unexpected ray job status: %s", jobStatus)
	}
	return status, msg, nil
}

func getRayJobMessage(rayJobStatus *rayV1alpha1.RayJobStatus) string {
	return fmt.Sprintf("ray job is failed, and status in [jobDeploymentStatus/rayClusterStatus/jobStatus] is %s/%s/%s, message: %s",
		rayJobStatus.JobDeploymentStatus, rayJobStatus.RayClusterStatus.State, rayJobStatus.JobStatus, rayJobStatus.Message)
}
