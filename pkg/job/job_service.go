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

package job

import (
	"fmt"
	"strconv"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/flavour"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	_ "github.com/PaddlePaddle/PaddleFlow/pkg/job/queue/sortpolicy"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
)

func CreateJob(conf schema.PFJobConf) (string, error) {
	log.Debugf("create job: %v", conf)
	if err := ValidateJob(conf); err != nil {
		return "", err
	}
	if err := checkResource(conf); err != nil {
		return "", err
	}
	jobConf := conf.(*schema.Conf)
	jobInfo := &models.Job{
		ID:       generateJobID(conf.GetName()),
		Type:     string(conf.Type()),
		UserName: conf.GetUserName(),
		QueueID:  conf.GetQueueID(),
		Status:   schema.StatusJobInit,
		Config:   jobConf,
	}
	if err := patchTasksFromEnv(jobConf, jobInfo); err != nil {
		log.Errorf("patch tasks from env failed, err: %v", err)
		return "", err
	}

	if err := models.CreateJob(jobInfo); err != nil {
		log.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
		return "", fmt.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
	}
	log.Infof("create job[%s] successful.", jobInfo.ID)
	return jobInfo.ID, nil
}

func patchTasksFromEnv(conf *schema.Conf, jobInfo *models.Job) error {
	log.Debugf("patch tasks from env: %v", conf)
	switch conf.Type() {
	case schema.TypePaddleJob:
		jobInfo.Framework = schema.FrameworkPaddle
		jobInfo.Type = string(schema.TypeDistributed)
	case schema.TypeSparkJob:
		jobInfo.Framework = schema.FrameworkSpark
		jobInfo.Type = string(schema.TypeDistributed)
	}

	var err error
	switch conf.GetJobMode() {
	case schema.EnvJobModePS:
		err = patchPSTasks(conf, jobInfo)
	case schema.EnvJobModeCollective:
		err = patchCollectiveTask(conf, jobInfo)
	case schema.EnvJobModePod:
		err = patchPodTask(conf, jobInfo)
	default:
		log.Errorf("unsupport job mode")
		return errors.InvalidJobModeError(conf.GetJobMode())
	}
	if err != nil {
		log.Errorf("patch tasks failed, err: %v", err)
		return err
	}
	return nil
}

func patchPSTasks(conf *schema.Conf, jobInfo *models.Job) error {
	jobInfo.Members = make([]models.Member, 2)
	// ps server task
	psTask, err := newPSServerTask(conf)
	if err != nil {
		log.Errorf("patch ps server task failed, err: %v", err)
		return err
	}
	// worker task
	workerTask, err := newPSWorkerTask(conf)
	if err != nil {
		log.Errorf("patch ps worker task failed, err: %v", err)
		return err
	}
	jobInfo.Members = append(jobInfo.Members, psTask, workerTask)
	return nil
}

func newPSServerTask(conf *schema.Conf) (models.Member, error) {
	psTask := models.Member{
		ID:   "ps",
		Role: schema.RolePServer,
		Conf: *conf,
	}
	var err error
	psTask.Flavour, err = flavour.GetFlavourWithCheck(schema.Flavour{Name: conf.GetPSFlavour()})
	if err != nil {
		log.Errorf("get worker flavour failed, err: %v", err)
		return psTask, fmt.Errorf("get worker flavour failed, err: %v", err)
	}
	if conf.GetPSCommand() != "" {
		psTask.Command = conf.GetPSCommand()
	} else {
		return psTask, fmt.Errorf("ps command is empty")
	}
	if conf.GetPSReplicas() != "" {
		psTask.Replicas, _ = strconv.Atoi(conf.GetPSReplicas())
	} else {
		return psTask, fmt.Errorf("ps replicas is empty")
	}
	return psTask, nil
}

func newPSWorkerTask(conf *schema.Conf) (models.Member, error) {
	workerTask := models.Member{
		ID:   "worker",
		Role: schema.RolePWorker,
		Conf: *conf,
	}
	var err error
	workerTask.Flavour, err = flavour.GetFlavourWithCheck(schema.Flavour{Name: conf.GetWorkerFlavour()})
	if err != nil {
		log.Errorf("get worker flavour failed, err: %v", err)
		return workerTask, fmt.Errorf("get worker flavour failed, err: %v", err)
	}
	if conf.GetWorkerCommand() != "" {
		workerTask.Command = conf.GetWorkerCommand()
	} else {
		return workerTask, fmt.Errorf("worker command is empty")
	}
	if conf.GetWorkerReplicas() != "" {
		replicasInt, _ := strconv.Atoi(conf.GetWorkerReplicas())
		workerTask.Replicas = replicasInt
	} else {
		return workerTask, fmt.Errorf("worker replicas is empty")
	}
	return workerTask, nil
}

func patchCollectiveTask(conf *schema.Conf, jobInfo *models.Job) error {
	jobInfo.Members = make([]models.Member, 1)
	// executor/worker
	workerTask := models.Member{
		ID:   "worker",
		Role: schema.RoleWorker,
		Conf: *conf,
	}
	// flavour
	var err error
	workerTask.Flavour, err = flavour.GetFlavourWithCheck(schema.Flavour{Name: conf.GetWorkerFlavour()})
	if err != nil {
		log.Errorf("get worker flavour failed, err: %v", err)
		return fmt.Errorf("get worker flavour failed, err: %v", err)
	}
	// commad
	if conf.GetWorkerCommand() != "" {
		workerTask.Command = conf.GetWorkerCommand()
	} else {
		return fmt.Errorf("worker command is empty")
	}
	// Replicase
	if conf.GetWorkerReplicas() != "" {
		replicasInt, _ := strconv.Atoi(conf.GetWorkerReplicas())
		workerTask.Replicas = replicasInt
	} else {
		return fmt.Errorf("worker replicas is empty")
	}
	jobInfo.Members = append(jobInfo.Members, workerTask)
	return nil
}

func patchPodTask(conf *schema.Conf, jobInfo *models.Job) error {
	var err error
	jobInfo.Config.Flavour, err = flavour.GetFlavourWithCheck(schema.Flavour{Name: conf.GetFlavour()})
	if err != nil {
		log.Errorf("get worker flavour failed, err: %v", err)
		return fmt.Errorf("get worker flavour failed, err: %v", err)
	}
	return nil
}

func generateJobID(param string) string {
	return uuid.GenerateID(fmt.Sprintf("%s-%s", schema.JobPrefix, param))
}

func ValidateJob(conf schema.PFJobConf) error {
	// check common config for job
	if len(conf.GetName()) == 0 {
		return errors.EmptyJobNameError()
	}
	if len(conf.GetImage()) == 0 {
		return errors.EmptyJobImageError()
	}
	if len(conf.Type()) == 0 {
		return errors.EmptyJobTypeError()
	}

	var userName string
	var queueName string
	if userName = conf.GetUserName(); len(userName) == 0 {
		return errors.EmptyUserNameError()
	}
	if queueName = conf.GetQueueName(); len(queueName) == 0 {
		return errors.EmptyQueueNameError()
	}
	return ValidateQueue(conf, userName, queueName)
}

func ValidateQueue(conf schema.PFJobConf, userName, queueName string) error {
	ctx := &logger.RequestContext{
		UserName: userName,
	}
	// check whether queue is exist or not
	queue, err := models.GetQueueByName(queueName)
	if err != nil {
		log.Errorf("get queue %s failed, err %v", queueName, err)
		return fmt.Errorf("queueName[%s] is not exist", queueName)
	}
	// check queue status
	if queue.Status != schema.StatusQueueOpen {
		errMsg := fmt.Sprintf("queue[%s] status is %s, and only open queue can submit jobs", queueName, queue.Status)
		log.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	// Check resource matching between queue and flavour
	if err := validateFlavours(conf, &queue); err != nil {
		log.Errorf("validateFlavours failed, err=%v", err)
		return err
	}

	// check whether cluster is exist or not
	cluster, err := models.GetClusterById(queue.ClusterId)
	if err != nil {
		log.Errorf("get cluster[%s] failed, err: %s", queue.ClusterId, err)
		return err
	}
	// check cluster status
	if cluster.Status != models.ClusterStatusOnLine {
		errMsg := fmt.Sprintf("cluster[%s] status is not %s", cluster.Status, models.ClusterStatusOnLine)
		log.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	conf.SetQueueID(queue.ID)
	conf.SetNamespace(queue.Namespace)
	conf.SetClusterID(cluster.ID)
	// check whether user has access to queue or not
	if !models.HasAccessToResource(ctx, common.ResourceTypeQueue, queueName) {
		return common.NoAccessError(userName, common.ResourceTypeQueue, queueName)
	}
	return nil
}

// validateFlavours checks flavour/psflavour/workflavour if exist
func validateFlavours(conf schema.PFJobConf, queue *models.Queue) error {
	flavors := []string{
		conf.GetFlavour(), conf.GetPSFlavour(), conf.GetWorkerFlavour(),
	}
	for _, flavor := range flavors {
		if len(flavor) == 0 {
			continue
		}
		if err := isEnoughQueueCapacity(flavor, queue.MaxResources); err != nil {
			errMsg := fmt.Sprintf("queue %s has no enough resource:%s", conf.GetQueueName(), err.Error())
			log.Errorf(errMsg)
			return fmt.Errorf(errMsg)
		}
	}
	return nil
}

// isEnoughQueueCapacity validate queue matching flavor
func isEnoughQueueCapacity(flavourKey string, queueResource schema.ResourceInfo) error {
	flavourValue, err := flavour.GetFlavourWithCheck(schema.Flavour{Name: flavourKey})
	if err != nil {
		log.Errorf("get flavour[%s] failed, err: %v", flavourKey, err)
		return err
	}

	// all field in flavour must be less equal than queue's
	if !flavourValue.ResourceInfo.LessEqual(queueResource) {
		errMsg := fmt.Sprintf("the request flavour[%s] is larger than queue's", flavourKey)
		log.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	return nil
}

func checkResource(conf schema.PFJobConf) error {
	// check job priority
	priority := conf.GetPriority()
	if len(priority) == 0 {
		conf.SetPriority(schema.EnvJobNormalPriority)
	} else {
		if priority != schema.EnvJobLowPriority &&
			priority != schema.EnvJobNormalPriority && priority != schema.EnvJobHighPriority {
			return errors.InvalidJobPriorityError(priority)
		}
	}
	return nil
}

func StopJobByID(jobID string) error {
	job, err := models.GetJobByID(jobID)
	if err != nil {
		return errors.JobIDNotFoundError(jobID)
	}
	pfJob := &api.PFJob{
		ID:        jobID,
		Name:      job.Config.GetName(),
		Namespace: job.Config.GetNamespace(),
		JobType:   job.Config.Type(),
		JobMode:   job.Config.GetJobMode(),
	}
	// create runtime for cluster
	clusterInfo, err := models.GetClusterById(job.Config.GetClusterID())
	if err != nil {
		return fmt.Errorf("stop job %s failed. cluster %s not found", jobID, clusterInfo.Name)
	}
	runtimeSvc, err := runtime.GetOrCreateRuntime(clusterInfo)
	if err != nil {
		errMsg := fmt.Sprintf("new runtime for cluster %s failed, err: %v.", clusterInfo.Name, err)
		log.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	// stop job on cluster
	if err = runtimeSvc.StopJob(pfJob); err != nil {
		log.Errorf("delete job %s from cluster %s failed, err: %v.", jobID, clusterInfo.Name, err)
		return err
	}
	if err = models.UpdateJobStatus(jobID, "job is terminated.", schema.StatusJobTerminated); err != nil {
		log.Errorf("update job[%s] status to [%s] failed, err: %v", jobID, schema.StatusJobTerminated, err)
		return err
	}
	return nil
}
