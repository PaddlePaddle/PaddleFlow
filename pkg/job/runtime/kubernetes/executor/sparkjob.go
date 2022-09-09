/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	sparkapp "github.com/PaddlePaddle/PaddleFlow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const defaultExecutorInstances int32 = 1

// SparkJob is the executor for spark job
type SparkJob struct {
	KubeJob
	SparkMainFile    string
	SparkMainClass   string
	SparkArguments   string
	DriverFlavour    string
	ExecutorFlavour  string
	ExecutorReplicas string
}

func (sj *SparkJob) validateJob(sparkApp *sparkapp.SparkApplication) error {
	if err := sj.KubeJob.validateJob(); err != nil {
		log.Errorf("validate basic params of spark job failed: %v", err)
		return err
	}
	if !sj.IsCustomYaml {
		if len(sj.Tasks) != 2 {
			return fmt.Errorf("the members' roles of sparkapp are driver or executor respectively, " +
				"but the number of members isn't two")
		}
		if sj.Tasks[0].Image == "" {
			return fmt.Errorf("spark image is not defined")
		}
		sj.Image = sj.Tasks[0].Image
		for i, task := range sj.Tasks {
			if err := validateSparkMemory(&sj.Tasks[i].Flavour.Mem); err != nil {
				err = fmt.Errorf("validate spark.%s.memory failed, err: %v", task.Role, err)
				log.Errorln(err)
				return err
			}
		}
		// todo check all required fields when job is not custom
	} else if err := sj.validateCustomYaml(sparkApp); err != nil {
		log.Errorf("validate custom yaml failed, err: %v", err)
		return err
	}

	return nil
}

func (sj *SparkJob) validateCustomYaml(sparkApp *sparkapp.SparkApplication) error {
	log.Infof("validate custom yaml for spark job: %v, sparkApp from yaml: %v", sj, sparkApp)
	if err := validateSparkResource(sparkApp); err != nil {
		err = fmt.Errorf("validate spark resource failed, err: %v", err)
		log.Errorln(err)
		return err
	}
	return nil
}

func validateSparkResource(sparkApp *sparkapp.SparkApplication) error {
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
	if err := validateSparkMemory(sparkApp.Spec.Driver.Memory); err != nil {
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
	if err := validateSparkMemory(sparkApp.Spec.Executor.Memory); err != nil {
		err = fmt.Errorf("validate spark.Executor memory failed, err: %v", err)
		log.Errorln(err)
		return err
	}
	return nil
}

// validateSparkMemory the spark memory can only accept DecimalSI, so BinarySI would be converted to DecimalSI
func validateSparkMemory(memory *string) error {
	memoryQuantity, err := resource.ParseQuantity(*memory)
	if err != nil {
		log.Errorf("parse spark memory failed, err: %v", err)
		return err
	}
	switch memoryQuantity.Format {
	case resource.BinarySI:
		*memory = strings.TrimSuffix(memoryQuantity.String(), "i")
		log.Debugf("convert memory to decimalSI-style: %v", *memory)
	case resource.DecimalSI:
		return nil
	default:
		err = fmt.Errorf("the %v format of memory %s is not supported", memoryQuantity.Format, *memory)
		log.Errorln(err)
	}
	return err
}

// patchSparkAppVariable patch env variable to jobApplication, the order of patches following spark crd
func (sj *SparkJob) patchSparkAppVariable(jobApp *sparkapp.SparkApplication) error {
	log.Debugf("patchSparkAppVariable from kubejob: %v", sj)
	// metadata
	sj.patchMetadata(&jobApp.ObjectMeta, sj.ID)
	// spec, the order of patches following SparkApplicationSpec crd
	if err := sj.patchSparkSpec(jobApp, sj.GetID()); err != nil {
		log.Errorf("fill spark application spec failed, err: %v", err)
		return err
	}

	log.Debugf("jobApp: %v, driver=%v, executor=%v", jobApp, jobApp.Spec.Driver, jobApp.Spec.Executor)
	return nil
}

func (sj *SparkJob) patchSparkSpec(jobApp *sparkapp.SparkApplication, jobID string) error {
	// BatchScheduler && BatchSchedulerOptions
	schedulerName := config.GlobalServerConfig.Job.SchedulerName
	jobApp.Spec.BatchScheduler = &schedulerName
	if jobApp.Spec.BatchSchedulerOptions == nil {
		jobApp.Spec.BatchSchedulerOptions = &sparkapp.BatchSchedulerConfiguration{}
	}
	if len(sj.QueueName) > 0 {
		jobApp.Spec.BatchSchedulerOptions.Queue = &sj.QueueName
		priorityClass := sj.getPriorityClass()
		jobApp.Spec.BatchSchedulerOptions.PriorityClassName = &priorityClass
	}

	if sj.IsCustomYaml {
		log.Infof("%s job %s/%s using custom yaml, pass the patch from tasks", sj.JobType, sj.Namespace, sj.Name)
		return nil
	}
	// when job is not using custom yaml, patch from tasks
	// image
	jobApp.Spec.Image = &sj.Image

	// mainAppFile, mainClass and arguments
	if len(sj.SparkMainFile) > 0 {
		sparkMainFile := sj.SparkMainFile
		jobApp.Spec.MainApplicationFile = &sparkMainFile
	}

	if len(sj.SparkMainClass) != 0 {
		jobApp.Spec.MainClass = &sj.SparkMainClass
	}

	if len(sj.SparkArguments) > 0 {
		jobApp.Spec.Arguments = []string{sj.SparkArguments}
	}

	// resource of driver and executor
	var driverFlavour, executorFlavour schema.Flavour
	var taskFileSystem []schema.FileSystem
	for _, task := range sj.Tasks {
		if task.Role == schema.RoleDriver {
			driverFlavour = task.Flavour
			sj.patchSparkSpecDriver(jobApp, task)
		} else if task.Role == schema.RoleExecutor {
			executorFlavour = task.Flavour
			sj.patchSparkSpecExecutor(jobApp, task)
		} else {
			err := fmt.Errorf("unknown type[%s] in task[%v]", task.Role, task)
			log.Errorf("patchSparkSpec failed, err: %v", err)
			return err
		}
		taskFileSystem = append(taskFileSystem, task.Conf.GetAllFileSystem()...)
	}
	fillGPUSpec(driverFlavour, executorFlavour, jobApp)

	// volumes
	jobApp.Spec.Volumes = appendVolumesIfAbsent(jobApp.Spec.Volumes, generateVolumes(taskFileSystem))

	return nil
}

func (sj *SparkJob) patchPodByTask(podSpec *sparkapp.SparkPodSpec, task schema.Member) {
	flavour := task.Flavour
	coresInt, _ := strconv.Atoi(task.Flavour.CPU)
	cores := int32(coresInt)
	podSpec.Cores = &cores
	podSpec.CoreLimit = &flavour.CPU
	podSpec.Memory = &flavour.Mem

	if len(podSpec.Env) == 0 {
		podSpec.Env = make([]corev1.EnvVar, 0)
	}
	podSpec.Env = append(podSpec.Env, sj.generateEnvVars()...)

	taskFileSystems := task.Conf.GetAllFileSystem()
	if len(taskFileSystems) != 0 {
		podSpec.VolumeMounts = appendMountsIfAbsent(podSpec.VolumeMounts, generateVolumeMounts(taskFileSystems))
	}
}

func (sj *SparkJob) patchSparkSpecDriver(jobApp *sparkapp.SparkApplication, task schema.Member) {
	sj.patchPodByTask(&jobApp.Spec.Driver.SparkPodSpec, task)
	if task.Name != "" {
		jobApp.Spec.Driver.PodName = &task.Name
	}
	if jobApp.Spec.Driver.ServiceAccount == nil {
		serviceAccount := string(schema.TypeSparkJob)
		jobApp.Spec.Driver.ServiceAccount = &serviceAccount
	}
}

func (sj *SparkJob) patchSparkSpecExecutor(jobApp *sparkapp.SparkApplication, task schema.Member) {
	sj.patchPodByTask(&jobApp.Spec.Executor.SparkPodSpec, task)
	if len(sj.ExecutorReplicas) > 0 {
		replicasInt, _ := strconv.Atoi(sj.ExecutorReplicas)
		replicas := int32(replicasInt)
		jobApp.Spec.Executor.Instances = &replicas
	}
	if jobApp.Spec.Executor.Instances == nil || *jobApp.Spec.Executor.Instances <= 0 {
		instances := defaultExecutorInstances
		jobApp.Spec.Executor.Instances = &instances
	}
}

// CreateJob creates a SparkJob
func (sj *SparkJob) CreateJob() (string, error) {
	jobID := sj.GetID()
	log.Debugf("begin create job jobID:[%s]", jobID)

	jobApp := &sparkapp.SparkApplication{}
	if err := sj.createJobFromYaml(jobApp); err != nil {
		log.Errorf("create job failed, err %v", err)
		return "", err
	}
	if err := sj.validateJob(jobApp); err != nil {
		log.Errorf("validate job failed, err %v", err)
		return "", err
	}

	// paddleflow won't patch any param to job if it is workflow type
	if sj.JobType != schema.TypeWorkflow {
		if err := sj.patchSparkAppVariable(jobApp); err != nil {
			log.Errorf("patch spark app variable failed, err %v", err)
			return "", err
		}
	}

	log.Debugf("begin submit job jobID:[%s]", jobID)
	err := Create(jobApp, k8s.SparkAppGVK, sj.DynamicClientOption)
	if err != nil {
		log.Errorf("create job %v failed, err: %v", jobID, err)
		return "", err
	}
	return jobID, nil
}

func fillGPUSpec(driverFlavour schema.Flavour, executorFlavour schema.Flavour, jobSpec *sparkapp.SparkApplication) {
	if num, found := driverFlavour.ScalarResources["nvidia.com/gpu"]; found {
		quantity, _ := strconv.Atoi(num)
		// TODO(qinduohao): resource should not fixed here
		jobSpec.Spec.Driver.GPU = &sparkapp.GPUSpec{
			Name:     "nvidia.com/gpu",
			Quantity: int64(quantity),
		}
	}
	if num, found := executorFlavour.ScalarResources["nvidia.com/gpu"]; found {
		quantity, _ := strconv.Atoi(num)
		jobSpec.Spec.Executor.GPU = &sparkapp.GPUSpec{
			Name:     "nvidia.com/gpu",
			Quantity: int64(quantity),
		}
	}
}
