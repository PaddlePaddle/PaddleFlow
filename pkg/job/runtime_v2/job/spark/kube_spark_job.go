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

package spark

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/util/kuberuntime"
)

var (
	JobGVK             = k8s.SparkAppGVK
	KubeSparkFwVersion = client.KubeFrameworkVersion(JobGVK)
)

// KubeSparkJob is a struct that contains client to operate spark application on cluster
type KubeSparkJob struct {
	kuberuntime.KubeBaseJob
}

func New(kubeClient framework.RuntimeClientInterface) framework.JobInterface {
	return &KubeSparkJob{
		KubeBaseJob: kuberuntime.NewKubeBaseJob(JobGVK, KubeSparkFwVersion, kubeClient),
	}
}

func (sj *KubeSparkJob) Submit(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	sparkJob := &v1beta2.SparkApplication{}
	if err := kuberuntime.CreateKubeJobFromYaml(sparkJob, sj.GVK, job); err != nil {
		log.Errorf("create %s failed, err %v", sj.String(jobName), err)
		return err
	}

	var err error
	// set metadata field
	kuberuntime.BuildJobMetadata(&sparkJob.ObjectMeta, job)
	// build scheduling policy
	sj.buildSchedulingPolicy(sparkJob, job.Conf.GetQueueName(), job.PriorityClassName)
	// set spec field
	if job.IsCustomYaml {
		// set custom SparkApplication Spec from user
		err = sj.customSparkJob(sparkJob, job)
	} else {
		// set builtin SparkApplication Spec
		err = sj.builtinSparkJob(sparkJob, job)
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", sj.String(jobName), err)
		return err
	}
	log.Debugf("begin to create %s, job info: %v", sj.String(jobName), sparkJob)
	err = sj.RuntimeClient.Create(sparkJob, sj.FrameworkVersion)
	if err != nil {
		log.Errorf("create %s failed, err %v", sj.String(jobName), err)
		return err
	}
	return nil
}

func (sj *KubeSparkJob) buildSchedulingPolicy(jobApp *v1beta2.SparkApplication, queueName, priority string) {
	// BatchScheduler && BatchSchedulerOptions
	schedulerName := config.GlobalServerConfig.Job.SchedulerName
	jobApp.Spec.BatchScheduler = &schedulerName
	if jobApp.Spec.BatchSchedulerOptions == nil {
		jobApp.Spec.BatchSchedulerOptions = &v1beta2.BatchSchedulerConfiguration{}
	}
	if len(queueName) > 0 {
		jobApp.Spec.BatchSchedulerOptions.Queue = &queueName
		priorityClass := kuberuntime.KubePriorityClass(priority)
		jobApp.Spec.BatchSchedulerOptions.PriorityClassName = &priorityClass
	}
}

func (sj *KubeSparkJob) builtinSparkJob(jobApp *v1beta2.SparkApplication, job *api.PFJob) error {
	// resource of driver and executor
	var taskFileSystem []pfschema.FileSystem
	var err error
	for _, task := range job.Tasks {
		switch task.Role {
		case pfschema.RoleDriver:
			err = sj.buildSparkDriverSpec(jobApp, task)
		case pfschema.RoleExecutor:
			err = sj.buildSparkExecutorSpec(jobApp, task)
		default:
			err = fmt.Errorf("unknown type[%s] in task[%v]", task.Role, task)
		}
		taskFileSystem = append(taskFileSystem, task.Conf.GetAllFileSystem()...)
	}
	if err != nil {
		log.Errorf("patchSparkSpec failed, err: %v", err)
		return err
	}
	// volumes
	jobApp.Spec.Volumes = kuberuntime.BuildVolumes(jobApp.Spec.Volumes, taskFileSystem)
	return nil
}

func (sj *KubeSparkJob) buildSparkDriverSpec(jobApp *v1beta2.SparkApplication, task pfschema.Member) error {
	if task.Conf.Env == nil {
		return fmt.Errorf("job env for mainAppFile, mainClass, or arguments is nil")
	}
	// mainAppFile, mainClass and arguments
	sparkMainFile, find := task.Conf.Env[pfschema.EnvJobSparkMainFile]
	if find {
		jobApp.Spec.MainApplicationFile = &sparkMainFile
	}
	sparkMainClass, find := task.Conf.Env[pfschema.EnvJobSparkMainClass]
	if find {
		jobApp.Spec.MainClass = &sparkMainClass
	}
	sparkArguments, find := task.Conf.Env[pfschema.EnvJobSparkArguments]
	if find {
		jobApp.Spec.Arguments = []string{sparkArguments}
	}
	err := sj.buildSparkPodSpec(&jobApp.Spec.Driver.SparkPodSpec, task)
	if err != nil {
		log.Errorf("patch SparkPodSpec for driver failed, err: %s", err)
		return err
	}
	// image
	jobApp.Spec.Image = &task.Conf.Image
	if task.Name != "" {
		jobApp.Spec.Driver.PodName = &task.Name
	}
	if jobApp.Spec.Driver.ServiceAccount == nil {
		serviceAccount := string(pfschema.TypeSparkJob)
		jobApp.Spec.Driver.ServiceAccount = &serviceAccount
	}
	return nil
}

func (sj *KubeSparkJob) buildSparkExecutorSpec(jobApp *v1beta2.SparkApplication, task pfschema.Member) error {
	err := sj.buildSparkPodSpec(&jobApp.Spec.Executor.SparkPodSpec, task)
	if err != nil {
		log.Errorf("patch SparkPodSpec for executor failed, err: %s", err)
		return err
	}
	// set executor instances
	instances := int32(kuberuntime.DefaultReplicas)
	if task.Replicas > 0 {
		instances = int32(task.Replicas)
	}
	jobApp.Spec.Executor.Instances = &instances
	return nil
}

// buildSparkPodSpec patch for SparkPodSpec
func (sj *KubeSparkJob) buildSparkPodSpec(podSpec *v1beta2.SparkPodSpec, task pfschema.Member) error {
	if podSpec == nil {
		return fmt.Errorf("build spark pod spec failed, err: podSpec or task is nil")
	}
	flavour := task.Flavour
	err := patchSparkMemory(&flavour.Mem)
	if err != nil {
		return err
	}
	coresInt, _ := strconv.Atoi(task.Flavour.CPU)
	cores := int32(coresInt)
	podSpec.Cores = &cores
	podSpec.CoreLimit = &flavour.CPU
	podSpec.Memory = &flavour.Mem
	// set gpu
	if num, found := flavour.ScalarResources["nvidia.com/gpu"]; found {
		quantity, _ := strconv.Atoi(num)
		// TODO: resource should not fixed here
		podSpec.GPU = &v1beta2.GPUSpec{
			Name:     "nvidia.com/gpu",
			Quantity: int64(quantity),
		}
	}
	// merge Env
	if len(task.Env) != 0 {
		podSpec.Env = kuberuntime.BuildEnvVars(podSpec.Env, task.Env)
	}
	// build VolumeMounts
	taskFileSystems := task.Conf.GetAllFileSystem()
	if len(taskFileSystems) != 0 {
		podSpec.VolumeMounts = kuberuntime.BuildVolumeMounts(podSpec.VolumeMounts, taskFileSystems)
	}
	return nil
}

// patchSparkMemory the spark memory can only accept DecimalSI, so BinarySI would be converted to DecimalSI
func patchSparkMemory(memory *string) error {
	memoryQuantity, err := resource.ParseQuantity(*memory)
	if err != nil {
		log.Errorf("parse spark memory failed, err: %v", err)
		return err
	}
	switch memoryQuantity.Format {
	case resource.BinarySI:
		*memory = strings.TrimSuffix(memoryQuantity.String(), "i")
		log.Infof("convert memory to decimalSI-style: %v", *memory)
	case resource.DecimalSI:
		err = nil
	default:
		err = fmt.Errorf("the %v format of memory %s is not supported", memoryQuantity.Format, *memory)
	}
	return err
}

func (sj *KubeSparkJob) customSparkJob(sparkJob *v1beta2.SparkApplication, job *api.PFJob) error {
	// TODO: add custom SparkJob
	return sj.validateSparkResource(sparkJob)
}

func (sj *KubeSparkJob) validateSparkResource(sparkApp *v1beta2.SparkApplication) error {
	cores := int32(k8s.DefaultCpuRequest)
	coreLimit := strconv.Itoa(k8s.DefaultCpuRequest)
	memoryQuantity := resource.NewQuantity(k8s.DefaultMemRequest, resource.BinarySI)
	memory := memoryQuantity.String()
	// validateTemplateResources for driver
	if sparkApp.Spec.Driver.CoreLimit == nil {
		sparkApp.Spec.Driver.Cores = &cores
		sparkApp.Spec.Driver.CoreLimit = &coreLimit
	}
	if sparkApp.Spec.Driver.Memory == nil {
		sparkApp.Spec.Driver.Memory = &memory
	}
	if err := patchSparkMemory(sparkApp.Spec.Driver.Memory); err != nil {
		err = fmt.Errorf("validate spark.driver memory failed, err: %v", err)
		log.Errorln(err)
		return err
	}

	// validateTemplateResources for executor
	if sparkApp.Spec.Executor.CoreLimit == nil {
		sparkApp.Spec.Executor.Cores = &cores
		sparkApp.Spec.Executor.CoreLimit = &coreLimit
	}
	if sparkApp.Spec.Executor.Memory == nil {
		sparkApp.Spec.Executor.Memory = &memory
	}
	if err := patchSparkMemory(sparkApp.Spec.Executor.Memory); err != nil {
		err = fmt.Errorf("validate spark.Executor memory failed, err: %v", err)
		log.Errorln(err)
		return err
	}
	return nil
}

func (sj *KubeSparkJob) AddEventListener(ctx context.Context, listenerType string, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeJob:
		err = sj.AddJobEventListener(ctx, jobQueue, listener, sj.JobStatus, nil)
	default:
		err = fmt.Errorf("listenerType %s is not supported", listenerType)
	}
	return err
}

// JobStatus get the statusInfo of spark application, including origin status, pf status and message
func (sj *KubeSparkJob) JobStatus(obj interface{}) (api.StatusInfo, error) {
	unObj := obj.(*unstructured.Unstructured)
	// convert to SparkApplication struct
	job := &v1beta2.SparkApplication{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, job); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s job failed. error: %s", obj, sj.GVK.String(), err)
		return api.StatusInfo{}, err
	}
	// convert job status
	state, msg, err := sj.getJobStatus(job.Status.AppState.State)
	if err != nil {
		log.Errorf("get spark job status failed, err: %v", err)
		return api.StatusInfo{}, err
	}
	log.Infof("spark job status: %s", state)
	return api.StatusInfo{
		OriginStatus: string(job.Status.AppState.State),
		Status:       state,
		Message:      msg,
	}, nil
}

func (sj *KubeSparkJob) getJobStatus(state v1beta2.ApplicationStateType) (pfschema.JobStatus, string, error) {
	status := pfschema.JobStatus("")
	msg := ""
	switch state {
	case v1beta2.NewState, v1beta2.SubmittedState:
		status = pfschema.StatusJobPending
		msg = "spark application is pending"
	case v1beta2.RunningState, v1beta2.SucceedingState, v1beta2.FailingState,
		v1beta2.InvalidatingState, v1beta2.PendingRerunState:
		status = pfschema.StatusJobRunning
		msg = "spark application is running"
	case v1beta2.CompletedState:
		status = pfschema.StatusJobSucceeded
		msg = "spark application is succeeded"
	case v1beta2.FailedState, v1beta2.FailedSubmissionState, v1beta2.UnknownState:
		status = pfschema.StatusJobFailed
		msg = "spark application is failed"
	default:
		return status, msg, fmt.Errorf("unexpected spark application status: %s", state)
	}
	return status, msg, nil
}
