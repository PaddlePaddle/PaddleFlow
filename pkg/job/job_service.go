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

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/errors"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/common/uuid"
	"paddleflow/pkg/job/api"
	_ "paddleflow/pkg/job/queue/sortpolicy"
	"paddleflow/pkg/job/runtime"
)

func CreateJob(conf schema.PFJobConf) (string, error) {
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
		Config:   *jobConf,
	}
	if err := models.CreateJob(jobInfo); err != nil {
		log.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
		return "", fmt.Errorf("create job[%s] in database faield, err: %v", conf.GetName(), err)
	}
	log.Infof("create job[%s] successful.", jobInfo.ID)
	return jobInfo.ID, nil
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
	ctx := &logger.RequestContext{
		UserName: userName,
	}
	// check whether queue is exist or not
	queue, err := models.GetQueueByName(ctx, queueName)
	if err != nil {
		log.Errorf("get queue %s failed, err %v", queueName, err)
		return fmt.Errorf("queueName[%s] is not exist\n", queueName)
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
	cluster, err := models.GetClusterById(ctx, queue.ClusterId)
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
	queueResource, err := schema.NewResource(queue.MaxResources.Cpu, queue.MaxResources.Mem, queue.MaxResources.ScalarResources)
	if err != nil {
		log.Errorf("queue[%s]:[%+v] convert to Resource type failed, err=%v", queue.Name, queue, err)
		return err
	}

	flavors := []string{
		conf.GetFlavour(), conf.GetPSFlavour(), conf.GetWorkerFlavour(),
	}
	for _, flavor := range flavors {
		if len(flavor) == 0 {
			continue
		}
		if err := isEnoughQueueCapacity(flavor, queueResource); err != nil {
			errMsg := fmt.Sprintf("queue %s has no enough resource:%s", conf.GetQueueName(), err.Error())
			log.Errorf(errMsg)
			return fmt.Errorf(errMsg)
		}
	}
	return nil
}

// isEnoughQueueCapacity validate queue matching flavor
func isEnoughQueueCapacity(flavourKey string, queueResource *schema.Resource) error {
	flavourValue, exist := config.GlobalServerConfig.FlavourMap[flavourKey]
	if !exist {
		return errors.InvalidFlavourError(flavourKey)
	}
	fr := flavourValue.ResourceInfo
	flavourRes, err := schema.NewResource(fr.Cpu, fr.Mem, fr.ScalarResources)
	if err != nil {
		log.Errorf("flavour[%s]:[%+v] convert to Resource type failed, err=%v", flavourKey, fr, err)
		return err
	}
	// all field in flavour must be less equal than queue's
	if !flavourRes.LessEqual(queueResource) {
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
	ctx := &logger.RequestContext{
		UserName: job.UserName,
	}
	clusterInfo, err := models.GetClusterById(ctx, job.Config.GetClusterID())
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
