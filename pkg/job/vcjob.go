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
	"strings"

	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	vcjob "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/job/submitter"
)

type VCJob struct {
}

const (
	psPort                    int32 = 8001
	defaultPodReplicas              = 1 // default replicas for pod mode
	defaultPSReplicas               = 1 // default replicas for PS mode, including ps-server and ps-worker
	defaultCollectiveReplicas       = 2 // default replicas for collective mode
)

// getVCJobFromDefaultPath get job yaml default path
// if EnvJobYamlPath not exist, default path would be the following
// "$DefaultYamlParentDir/vcjob_PS.yaml"
// "$DefaultYamlParentDir/vcjob_Collective.yaml"
// "$DefaultYamlParentDir/vcjob_Pod.yaml"
func getVCJobFromDefaultPath(conf *models.Conf) string {
	jobType := conf.Env[schema.EnvJobType]
	return fmt.Sprintf("%s/%s_%s.yaml", config.GlobalServerConfig.Job.DefaultJobYamlDir, jobType, strings.ToLower(conf.Env[schema.EnvJobMode]))
}

// patchVCJobVariable patch env variable to vcJob, the order of patches following vcJob crd
func patchVCJobVariable(jobApp *vcjob.Job, jobID string, conf *models.Conf) error {
	jobApp.Name = jobID
	// metadata
	if namespace, exist := conf.Env[schema.EnvJobNamespace]; exist {
		jobApp.Namespace = namespace
	}
	if jobApp.Labels == nil {
		jobApp.Labels = map[string]string{}
	}
	jobApp.Labels[schema.JobOwnerLabel] = schema.JobOwnerValue
	jobApp.Labels[schema.JobIDLabel] = jobID

	if queueName, exist := conf.Env[schema.EnvJobQueueName]; exist {
		jobApp.Spec.Queue = queueName
		priorityClass := getPriorityClass(conf.Env[schema.EnvJobPriority])
		jobApp.Spec.PriorityClassName = priorityClass
	}
	// SchedulerName
	jobApp.Spec.SchedulerName = config.GlobalServerConfig.Job.SchedulerName

	var err error
	switch conf.Env[schema.EnvJobMode] {
	case schema.EnvJobModePS:
		err = fillPSJobSpec(jobApp, conf)
	case schema.EnvJobModeCollective:
		err = fillCollectiveJobSpec(jobApp, conf)
	case schema.EnvJobModePod:
		err = fillPodJobSpec(jobApp, conf)
	}
	if err != nil {
		log.Errorf("patchVCJobVariable failed, err=[%v]", err)
		return err
	}
	return nil

}

func (vcJob *VCJob) CreateJob(conf *models.Conf) (string, error) {
	jobID := generateJobID(conf.Name)
	log.Debugf("begin create job jobID:[%s]", jobID)

	jobApp := &vcjob.Job{}
	if err := createJobFromYaml(conf, jobApp); err != nil {
		log.Errorf("create job failed, err %v", err)
		return "", err
	}

	patchVCJobVariable(jobApp, jobID, conf)

	job := &models.Job{
		ID:       jobID,
		Type:     conf.Env[schema.EnvJobType],
		UserName: conf.Env[schema.EnvJobUserName],
		Config:   *conf,
	}
	log.Debugf("begin submit job jobID:[%s] job:[%s]", jobID, config.PrettyFormat(job))
	err := persistAndExecuteJob(job, func() error {
		return submitter.JobExecutor.StartJob(jobApp, k8s.VCJobGVK)
	})
	if err != nil {
		log.Errorf("create job %v failed, err %v", job, err)
		return "", err
	}
	return jobID, nil
}

func (vcJob *VCJob) StopJobByID(jobID string) error {
	job, err := GetJobByID(jobID)
	if err != nil {
		return err
	}
	namespace := job.Config.Env[schema.EnvJobNamespace]
	if err = submitter.JobExecutor.StopJob(namespace, job.ID, k8s.VCJobGVK); err != nil {
		log.Errorf("stop vcjob %s in namespace %s failed, err %v", job.ID, namespace, err)
		return err
	}
	return nil
}

func fillPSJobSpec(jobSpec *vcjob.Job, conf *models.Conf) error {
	conf.Env[schema.EnvJobPSPort] = strconv.FormatInt(int64(psPort), 10)

	// ps mode only permit 2 tasks
	if len(jobSpec.Spec.Tasks) != 2 {
		return fmt.Errorf("vcjob[%s] must be contain two Tasks, actually [%d]", jobSpec.Name, len(jobSpec.Spec.Tasks))
	}
	// ps master
	psTask := jobSpec.Spec.Tasks[0]
	if err := fillTaskInPSMode(&psTask, conf, true, jobSpec.Name); err != nil {
		log.Errorf("fill Task[%s] in PS-Mode failed, err=[%v]", psTask.Name, err)
		return err
	}
	// worker
	workerTask := jobSpec.Spec.Tasks[1]
	if err := fillTaskInPSMode(&workerTask, conf, false, jobSpec.Name); err != nil {
		log.Errorf("fill Task[%s] in PS-Mode failed, err=[%v]", psTask.Name, err)
		return err
	}
	jobSpec.Spec.MinAvailable = psTask.Replicas + workerTask.Replicas

	return nil
}

func fillTaskInPSMode(vcTask *vcjob.TaskSpec, conf *models.Conf, isMaster bool, jobName string) error {
	psReplicaStr := ""
	commandEnv := ""
	flavourStr := ""
	if isMaster {
		psReplicaStr = conf.Env[schema.EnvJobPServerReplicas]
		flavourStr = conf.Env[schema.EnvJobPServerFlavour]
		commandEnv = conf.Env[schema.EnvJobPServerCommand]
	} else {
		psReplicaStr = conf.Env[schema.EnvJobWorkerReplicas]
		flavourStr = conf.Env[schema.EnvJobWorkerFlavour]
		commandEnv = conf.Env[schema.EnvJobWorkerCommand]
	}
	if commandEnv == "" {
		return fmt.Errorf("the env [command] is required")
	}

	if psReplicaStr != "" {
		replicasInt, _ := strconv.Atoi(psReplicaStr)
		vcTask.Replicas = int32(replicasInt)
	}
	if vcTask.Replicas <= 0 {
		vcTask.Replicas = defaultPSReplicas
	}

	// patch vcTask.Template.Labels
	if vcTask.Template.Labels == nil {
		vcTask.Template.Labels = map[string]string{}
	}
	vcTask.Template.Labels[schema.JobIDLabel] = jobName

	// patch Task.Template.Spec.Containers[0]
	if len(vcTask.Template.Spec.Containers) != 1 {
		vcTask.Template.Spec.Containers = []v1.Container{{}}
	}
	fillContainerInTask(&vcTask.Template.Spec.Containers[0], conf, flavourStr, commandEnv)

	// patch vcTask.Template.Spec.Volumes
	vcTask.Template.Spec.Volumes = appendVolumeIfAbsent(vcTask.Template.Spec.Volumes, generateVolume(conf.Env[schema.EnvJobFsID], conf.Env[schema.EnvJobPVCName]))

	return nil
}

func fillPodJobSpec(jobSpec *vcjob.Job, conf *models.Conf) error {
	log.Debugf("fillPodJobSpec for job[%s]", jobSpec.Name)
	if jobSpec.Spec.Tasks == nil {
		return fmt.Errorf("tasks is nil")
	}

	for i := range jobSpec.Spec.Tasks {
		if err := fillTaskInPodMode(&jobSpec.Spec.Tasks[i], conf, jobSpec.Name); err != nil {
			log.Errorf("fillTaskInPodMode occur a err[%v]", err)
			return err
		}
	}
	log.Debugf("job[%s].Spec.Tasks=[%+v]", jobSpec.Name, jobSpec.Spec.Tasks)
	return nil
}

// fillTaskInPodMode fill params into job's task in vcJob pod mode
func fillTaskInPodMode(taskSpec *vcjob.TaskSpec, conf *models.Conf, jobName string) error {
	log.Infof("fillTaskInPodMode: fill params in job[%s]-task[%s]", jobName, taskSpec.Name)

	if taskSpec.Replicas <= 0 {
		taskSpec.Replicas = defaultPodReplicas
	}

	// filter illegal task
	// only default yaml job can be patched,
	// user yaml may be muti-containers, and we cannot ensure format of user's yaml
	if taskSpec.Template.Spec.Containers == nil || len(taskSpec.Template.Spec.Containers) == 0 {
		return fmt.Errorf("task's container is nil")
	}

	// patch taskSpec.Template.Labels
	if taskSpec.Template.Labels == nil {
		taskSpec.Template.Labels = map[string]string{}
	}
	taskSpec.Template.Labels[schema.JobIDLabel] = jobName

	// patch taskSpec.Template.Spec.Containers
	fillContainerInTask(&taskSpec.Template.Spec.Containers[0], conf, conf.Env[schema.EnvJobFlavour], conf.Command)

	// patch taskSpec.Template.Spec.Volumes
	taskSpec.Template.Spec.Volumes = appendVolumeIfAbsent(taskSpec.Template.Spec.Volumes, generateVolume(conf.Env[schema.EnvJobFsID], conf.Env[schema.EnvJobPVCName]))
	log.Debugf("fillTaskInPodMode completed: job[%s]-task[%+v]", jobName, taskSpec)
	return nil
}

func fillCollectiveJobSpec(jobSpec *vcjob.Job, conf *models.Conf) error {
	replicasStr, found := conf.Env[schema.EnvJobReplicas]
	if found {
		replicas, _ := strconv.Atoi(replicasStr)
		jobSpec.Spec.MinAvailable = int32(replicas)
	}

	var err error
	if jobSpec.Spec.Tasks, err = fillTaskInCollectiveMode(jobSpec.Spec.Tasks, conf, jobSpec.Name); err != nil {
		log.Errorf("fillTaskInCollectiveMode for job[%s] failed, err=[%v]", jobSpec.Name, err)
		return err
	}

	return nil
}

func fillTaskInCollectiveMode(tasks []vcjob.TaskSpec, conf *models.Conf, jobName string) ([]vcjob.TaskSpec, error) {
	log.Debugf("fillTaskInCollectiveMode: job[%s]-task", jobName)

	// filter illegal job
	if len(tasks) != 1 {
		return nil, fmt.Errorf("the num of job[%s]-task must be 1, current is [%d]", jobName, len(tasks))
	}
	if len(tasks[0].Template.Spec.Containers) != 1 {
		return nil, fmt.Errorf("the num of job[%s]-task[%s]-container must be 1, current is [%d]", jobName, tasks[0].Name,
			len(tasks[0].Template.Spec.Containers))
	}

	// task.Metadata and labels
	task := &tasks[0]
	replicasStr, found := conf.Env[schema.EnvJobReplicas]
	if found {
		replicas, _ := strconv.Atoi(replicasStr)
		task.Replicas = int32(replicas)
	}
	if task.Replicas <= 0 {
		task.Replicas = defaultCollectiveReplicas
	}

	if task.Template.Labels == nil {
		task.Template.Labels = map[string]string{}
	}
	task.Template.Labels[schema.JobIDLabel] = jobName

	// todo : add affinity
	fillContainerInTask(&task.Template.Spec.Containers[0], conf, conf.Env[schema.EnvJobFlavour], conf.Command)

	// patch task.Template.Spec.Volumes
	jobVolume := generateVolume(conf.Env[schema.EnvJobFsID], conf.Env[schema.EnvJobPVCName])
	task.Template.Spec.Volumes = appendVolumeIfAbsent(task.Template.Spec.Volumes, jobVolume)

	return tasks, nil
}

// fillContainerInTask fill container in vcjob task
// flavourKey can be {schema.EnvJobFlavour, schema.EnvJobWorkerFlavour, schema.EnvJobPServerFlavour} of Env value
// command in conf.Command or {schema.EnvJobPServerCommand, schema.EnvJobWorkerCommand} in Env
func fillContainerInTask(container *v1.Container, conf *models.Conf, flavourKey, command string) {
	container.Image = conf.Image
	container.Command = []string{"bash", "-c", fixContainerCommand(command)}
	flavourValue := config.GlobalServerConfig.FlavourMap[flavourKey]
	container.Resources = generateResourceRequirements(flavourValue)
	container.VolumeMounts = appendMountIfAbsent(container.VolumeMounts, generateVolumeMount(conf.Env[schema.EnvJobFsID]))
	container.Env = generateEnvVars(conf)
}
