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

package job

import (
	"fmt"
	"strconv"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"

	sparkapp "paddleflow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/job/submitter"
)

const defaultExecutorInstances int32 = 1

type SparkJob struct {
}

// getSparkJobYamlPath get job yaml path
// if EnvJobYamlPath not exist, default path would be "$DefaultYamlParentDir/spark.yaml"
func getSparkJobYamlPath(conf *models.Conf) string {
	jobType := conf.Env[schema.EnvJobType]
	return fmt.Sprintf("%s/%s.yaml", config.GlobalServerConfig.Job.DefaultJobYamlDir, jobType)
}

// patchSparkAppVariable patch env variable to jobApplication, the order of patches following spark crd
func patchSparkAppVariable(jobApp *sparkapp.SparkApplication, jobID string, conf *models.Conf) error {
	jobApp.Name = jobID
	// metadata
	patchSparkMetaData(jobApp, jobID, conf)
	// spec, the order of patches following SparkApplicationSpec crd
	patchSparkSpec(jobApp, jobID, conf)

	// volumes
	if jobApp.Spec.Volumes == nil {
		jobApp.Spec.Volumes = []corev1.Volume{}
	}
	jobFsVolume := generateVolume(conf.Env[schema.EnvJobFsID], conf.Env[schema.EnvJobPVCName])
	jobApp.Spec.Volumes = appendVolumeIfAbsent(jobApp.Spec.Volumes, jobFsVolume)
	return nil
}

func patchSparkMetaData(jobApp *sparkapp.SparkApplication, jobID string, conf *models.Conf) {
	if namespace, exist := conf.Env[schema.EnvJobNamespace]; exist {
		jobApp.Namespace = namespace
	}
	if jobApp.Labels == nil {
		jobApp.Labels = map[string]string{}
	}
	jobApp.Labels[schema.JobOwnerLabel] = schema.JobOwnerValue
	jobApp.Labels[schema.JobIDLabel] = jobID
}

func patchSparkSpec(jobApp *sparkapp.SparkApplication, jobID string, conf *models.Conf) {
	// image
	jobApp.Spec.Image = &conf.Image

	// mainAppFile, mainClass and arguments
	mainAppFile := conf.Env[schema.EnvJobSparkMainFile]
	jobApp.Spec.MainApplicationFile = &mainAppFile
	if mainClass, exist := conf.Env[schema.EnvJobSparkMainClass]; exist {
		jobApp.Spec.MainClass = &mainClass
	}
	if arguments, exist := conf.Env[schema.EnvJobSparkArguments]; exist && len(arguments) > 0 {
		jobApp.Spec.Arguments = []string{arguments}
	}
	// BatchScheduler && BatchSchedulerOptions
	schedulerName := config.GlobalServerConfig.Job.SchedulerName
	jobApp.Spec.BatchScheduler = &schedulerName
	if jobApp.Spec.BatchSchedulerOptions == nil {
		jobApp.Spec.BatchSchedulerOptions = &sparkapp.BatchSchedulerConfiguration{}
	}
	if queueName, exist := conf.Env[schema.EnvJobQueueName]; exist {
		jobApp.Spec.BatchSchedulerOptions.Queue = &queueName
		priorityClass := getPriorityClass(conf.Env[schema.EnvJobPriority])
		jobApp.Spec.BatchSchedulerOptions.PriorityClassName = &priorityClass
	}

	// resource of driver and executor
	driverFlavour := config.GlobalServerConfig.FlavourMap[conf.Env[schema.EnvJobDriverFlavour]]
	driverCoresInt, _ := strconv.Atoi(driverFlavour.Cpu)
	driverCores := int32(driverCoresInt)
	executorFlavour := config.GlobalServerConfig.FlavourMap[conf.Env[schema.EnvJobExecutorFlavour]]
	executorCoresInt, _ := strconv.Atoi(executorFlavour.Cpu)
	executorCores := int32(executorCoresInt)
	// driver
	patchSparkSpecDriver(jobApp, conf, driverCores, driverFlavour)
	// executor
	patchSparkSpecExecutor(jobApp, conf, executorCores, executorFlavour)

	fillGPUSpec(driverFlavour, executorFlavour, jobApp)

}

func patchSparkSpecDriver(jobApp *sparkapp.SparkApplication, conf *models.Conf, cores int32, flavour schema.Flavour) {
	jobApp.Spec.Driver.Cores = &cores
	jobApp.Spec.Driver.CoreLimit = &flavour.Cpu
	jobApp.Spec.Driver.Memory = &flavour.Mem
	if len(jobApp.Spec.Driver.Env) == 0 {
		jobApp.Spec.Driver.Env = make([]corev1.EnvVar, 0)
	}
	jobApp.Spec.Driver.Env = append(jobApp.Spec.Driver.Env, generateEnvVars(conf)...)
	jobApp.Spec.Driver.PodName = &conf.Name
	if jobApp.Spec.Driver.ServiceAccount == nil {
		serviceAccount := string(schema.TypeSparkJob)
		jobApp.Spec.Driver.ServiceAccount = &serviceAccount
	}
	if jobApp.Spec.Driver.SparkPodSpec.VolumeMounts == nil {
		jobApp.Spec.Driver.SparkPodSpec.VolumeMounts = []corev1.VolumeMount{}
	}
	fsVolumeMount := generateVolumeMount(conf.Env[schema.EnvJobFsID])
	jobApp.Spec.Driver.SparkPodSpec.VolumeMounts = appendMountIfAbsent(jobApp.Spec.Driver.SparkPodSpec.VolumeMounts, fsVolumeMount)
}

func patchSparkSpecExecutor(jobApp *sparkapp.SparkApplication, conf *models.Conf, cores int32, flavour schema.Flavour) {
	replicasStr, found := conf.Env[schema.EnvJobExecutorReplicas]
	if found {
		replicasInt, _ := strconv.Atoi(replicasStr)
		replicas := int32(replicasInt)
		jobApp.Spec.Executor.Instances = &replicas
	}
	if jobApp.Spec.Executor.Instances == nil || *jobApp.Spec.Executor.Instances <= 0 {
		instances := defaultExecutorInstances
		jobApp.Spec.Executor.Instances = &instances
	}

	jobApp.Spec.Executor.Cores = &cores
	jobApp.Spec.Executor.CoreLimit = &flavour.Cpu
	jobApp.Spec.Executor.Memory = &flavour.Mem
	if len(jobApp.Spec.Executor.Env) == 0 {
		jobApp.Spec.Executor.Env = make([]corev1.EnvVar, 0)
	}
	jobApp.Spec.Executor.Env = append(jobApp.Spec.Driver.Env, generateEnvVars(conf)...)
	fsVolumeMount := generateVolumeMount(conf.Env[schema.EnvJobFsID])
	jobApp.Spec.Executor.SparkPodSpec.VolumeMounts = appendMountIfAbsent(jobApp.Spec.Executor.SparkPodSpec.VolumeMounts, fsVolumeMount)
}

func (sparkJob *SparkJob) CreateJob(conf *models.Conf) (string, error) {
	jobID := generateJobID(conf.Name)
	log.Debugf("begin create job jobID:[%s]", jobID)

	jobApp := &sparkapp.SparkApplication{}
	if err := createJobFromYaml(conf, jobApp); err != nil {
		log.Errorf("create job failed, err %v", err)
		return "", err
	}

	patchSparkAppVariable(jobApp, jobID, conf)

	job := &models.Job{
		ID:       jobID,
		Type:     conf.Env[schema.EnvJobType],
		UserName: conf.Env[schema.EnvJobUserName],
		Config:   *conf,
	}
	log.Debugf("begin submit job jobID:[%s] job:[%s]", jobID, config.PrettyFormat(job))
	err := persistAndExecuteJob(job, func() error {
		return submitter.JobExecutor.StartJob(jobApp, k8s.SparkAppGVK)
	})
	if err != nil {
		log.Errorf("create job %v failed, err %v", job, err)
		return "", err
	}
	return jobID, nil
}

func (sparkJob *SparkJob) StopJobByID(jobID string) error {
	job, err := GetJobByID(jobID)
	if err != nil {
		return err
	}
	namespace := job.Config.Env[schema.EnvJobNamespace]
	if err = submitter.JobExecutor.StopJob(namespace, job.ID, k8s.SparkAppGVK); err != nil {
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
