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
	"strconv"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"

	sparkapp "paddleflow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/common/schema"
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

// patchSparkAppVariable patch env variable to jobApplication, the order of patches following spark crd
func (sj *SparkJob) patchSparkAppVariable(jobApp *sparkapp.SparkApplication) error {
	// metadata
	sj.patchMetadata(&jobApp.ObjectMeta)
	// spec, the order of patches following SparkApplicationSpec crd
	sj.patchSparkSpec(jobApp, sj.GetID())

	// volumes
	jobApp.Spec.Volumes = sj.appendVolumeIfAbsent(jobApp.Spec.Volumes, sj.generateVolume())
	return nil
}

func (sj *SparkJob) patchSparkSpec(jobApp *sparkapp.SparkApplication, jobID string) {
	// image
	jobApp.Spec.Image = &sj.Image

	// mainAppFile, mainClass and arguments
	mainAppFile := sj.SparkMainFile
	jobApp.Spec.MainApplicationFile = &mainAppFile

	if len(sj.SparkMainClass) != 0 {
		jobApp.Spec.MainClass = &sj.SparkMainClass
	}
	if len(sj.SparkArguments) > 0 {
		jobApp.Spec.Arguments = []string{sj.SparkArguments}
	}
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

	// resource of driver and executor
	driverFlavour := config.GlobalServerConfig.FlavourMap[sj.DriverFlavour]
	driverCoresInt, _ := strconv.Atoi(driverFlavour.CPU)
	driverCores := int32(driverCoresInt)
	executorFlavour := config.GlobalServerConfig.FlavourMap[sj.ExecutorFlavour]
	executorCoresInt, _ := strconv.Atoi(executorFlavour.CPU)
	executorCores := int32(executorCoresInt)
	// driver
	sj.patchSparkSpecDriver(jobApp, driverCores, driverFlavour)
	// executor
	sj.patchSparkSpecExecutor(jobApp, executorCores, executorFlavour)

	fillGPUSpec(driverFlavour, executorFlavour, jobApp)

}

func (sj *SparkJob) patchSparkSpecDriver(jobApp *sparkapp.SparkApplication, cores int32, flavour schema.Flavour) {
	jobApp.Spec.Driver.Cores = &cores
	jobApp.Spec.Driver.CoreLimit = &flavour.CPU
	jobApp.Spec.Driver.Memory = &flavour.Mem
	if len(jobApp.Spec.Driver.Env) == 0 {
		jobApp.Spec.Driver.Env = make([]corev1.EnvVar, 0)
	}
	jobApp.Spec.Driver.Env = append(jobApp.Spec.Driver.Env, sj.generateEnvVars()...)
	jobApp.Spec.Driver.PodName = &sj.Name
	if jobApp.Spec.Driver.ServiceAccount == nil {
		serviceAccount := string(schema.TypeSparkJob)
		jobApp.Spec.Driver.ServiceAccount = &serviceAccount
	}
	if jobApp.Spec.Driver.SparkPodSpec.VolumeMounts == nil {
		jobApp.Spec.Driver.SparkPodSpec.VolumeMounts = []corev1.VolumeMount{}
	}
	volumeMount := sj.generateVolumeMount()
	jobApp.Spec.Driver.SparkPodSpec.VolumeMounts = sj.appendMountIfAbsent(jobApp.Spec.Driver.SparkPodSpec.VolumeMounts, volumeMount)
}

func (sj *SparkJob) patchSparkSpecExecutor(jobApp *sparkapp.SparkApplication, cores int32, flavour schema.Flavour) {
	if len(sj.ExecutorReplicas) > 0 {
		replicasInt, _ := strconv.Atoi(sj.ExecutorReplicas)
		replicas := int32(replicasInt)
		jobApp.Spec.Executor.Instances = &replicas
	}
	if jobApp.Spec.Executor.Instances == nil || *jobApp.Spec.Executor.Instances <= 0 {
		instances := defaultExecutorInstances
		jobApp.Spec.Executor.Instances = &instances
	}

	jobApp.Spec.Executor.Cores = &cores
	jobApp.Spec.Executor.CoreLimit = &flavour.CPU
	jobApp.Spec.Executor.Memory = &flavour.Mem
	if len(jobApp.Spec.Executor.Env) == 0 {
		jobApp.Spec.Executor.Env = make([]corev1.EnvVar, 0)
	}
	jobApp.Spec.Executor.Env = append(jobApp.Spec.Driver.Env, sj.generateEnvVars()...)
	volumeMount := sj.generateVolumeMount()
	jobApp.Spec.Executor.SparkPodSpec.VolumeMounts = sj.appendMountIfAbsent(jobApp.Spec.Executor.SparkPodSpec.VolumeMounts, volumeMount)
}

// CreateJob creates a SparkJob
func (sj *SparkJob) CreateJob() (string, error) {
	if err := sj.validateJob(); err != nil {
		log.Errorf("validate job failed, err %v", err)
		return "", err
	}
	jobID := sj.GetID()
	log.Debugf("begin create job jobID:[%s]", jobID)

	jobApp := &sparkapp.SparkApplication{}
	if err := sj.createJobFromYaml(jobApp); err != nil {
		log.Errorf("create job failed, err %v", err)
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
		log.Errorf("create job %v failed, err %v", jobID, err)
		return "", err
	}
	return jobID, nil
}

// StopJobByID stops a job by jobID
func (sj *SparkJob) StopJobByID(jobID string) error {
	job, err := models.GetJobByID(jobID)
	if err != nil {
		return err
	}
	namespace := job.Config.GetNamespace()
	if err = Delete(namespace, job.ID, k8s.SparkAppGVK, sj.DynamicClientOption); err != nil {
		log.Errorf("stop sparkjob %s in namespace %s failed, err %v", job.ID, namespace, err)
		return err
	}
	return nil
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
