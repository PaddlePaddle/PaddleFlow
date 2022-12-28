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

package controller

import (
	"fmt"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	_ "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job"
	"github.com/PaddlePaddle/PaddleFlow/pkg/metrics"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	JobSyncControllerName = "JobSync"
	DefaultSyncRetryTimes = 3
	DefaultJobTTLSeconds  = 600
)

type JobSync struct {
	runtimeClient framework.RuntimeClientInterface
	// jobQueue contains job add/update/delete event
	jobQueue workqueue.RateLimitingInterface
	// taskQueue contains task add/update/delete event
	taskQueue workqueue.RateLimitingInterface
	//  waitedCleanQueue contains jobs to be deleted
	waitedCleanQueue workqueue.DelayingInterface
}

func NewJobSync() *JobSync {
	return &JobSync{}
}

func (j *JobSync) Name() string {
	return fmt.Sprintf("%s controller for %s", JobSyncControllerName, j.runtimeClient.Cluster())
}

func (j *JobSync) Initialize(runtimeClient framework.RuntimeClientInterface) error {
	if runtimeClient == nil {
		return fmt.Errorf("init %s failed", JobSyncControllerName)
	}
	j.runtimeClient = runtimeClient
	log.Infof("initialize %s!", j.Name())
	j.jobQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	j.taskQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	j.waitedCleanQueue = workqueue.NewDelayingQueue()

	// Register job listeners
	err := j.runtimeClient.RegisterListener(pfschema.ListenerTypeJob, j.jobQueue)
	if err != nil {
		log.Errorf("register job event listener for %s failed, err: %v", j.Name(), err)
		return err
	}
	// Register task listeners
	err = j.runtimeClient.RegisterListener(pfschema.ListenerTypeTask, j.taskQueue)
	if err != nil {
		log.Errorf("register task event listener for %s failed, err: %v", j.Name(), err)
		return err
	}
	return nil
}

func (j *JobSync) Run(stopCh <-chan struct{}) {
	log.Infof("Start %s successfully!", j.Name())
	err := j.runtimeClient.StartListener(pfschema.ListenerTypeJob, stopCh)
	if err != nil {
		log.Errorf("start job listener failed, err: %v", err)
		return
	}
	err = j.runtimeClient.StartListener(pfschema.ListenerTypeTask, stopCh)
	if err != nil {
		log.Errorf("start task listener failed, err: %v", err)
		return
	}

	j.preHandleTerminatingJob()
	go wait.Until(j.runJobWorker, 0, stopCh)
	go wait.Until(j.runTaskWorker, 0, stopCh)
	go wait.Until(j.runJobGCWorker, 0, stopCh)
}

func (j *JobSync) runJobWorker() {
	for j.processJobWorkItem() {
	}
}

func (j *JobSync) processJobWorkItem() bool {
	log.Debugf("processJobWorkItem: jobQueue length is %d", j.jobQueue.Len())
	obj, shutdown := j.jobQueue.Get()
	if shutdown {
		return false
	}
	jobSyncInfo := obj.(*api.JobSyncInfo)
	log.Debugf("process job sync. jobID: %s", jobSyncInfo.ID)
	defer j.jobQueue.Done(jobSyncInfo)

	if err := j.syncJobStatus(jobSyncInfo); err != nil {
		log.Errorf("sync job status failed. jobID: %s err: %s", jobSyncInfo.ID, err.Error())
		if jobSyncInfo.RetryTimes < DefaultSyncRetryTimes {
			jobSyncInfo.RetryTimes += 1
			j.jobQueue.AddRateLimited(jobSyncInfo)
		}
		j.jobQueue.Forget(jobSyncInfo)
		return true
	}

	j.jobQueue.Forget(jobSyncInfo)
	return true
}

func (j *JobSync) syncJobStatus(jobSyncInfo *api.JobSyncInfo) error {
	log.Infof("begin syncJobStatus jobID: %s, action: %s", jobSyncInfo.ID, jobSyncInfo.Action)
	var err error
	switch jobSyncInfo.Action {
	case pfschema.Create:
		j.gcFinishedJob(jobSyncInfo)
		err = j.doCreateAction(jobSyncInfo)
	case pfschema.Delete:
		err = j.doDeleteAction(jobSyncInfo)
	case pfschema.Update:
		j.gcFinishedJob(jobSyncInfo)
		err = j.doUpdateAction(jobSyncInfo)
	case pfschema.Terminate:
		err = j.doTerminateAction(jobSyncInfo)
	}
	return err
}

func (j *JobSync) doCreateAction(jobSyncInfo *api.JobSyncInfo) error {
	log.Infof("do create action, job sync info: %s", jobSyncInfo.String())
	_, err := storage.Job.GetJobByID(jobSyncInfo.ID)
	if err == nil {
		return j.doUpdateAction(jobSyncInfo)
	}
	// only create job for subtask
	if jobSyncInfo.ParentJobID != "" {
		// check weather parent job is exist or not
		parentJob, err := storage.Job.GetJobByID(jobSyncInfo.ParentJobID)
		if err != nil {
			log.Errorf("get parent job %s failed, err: %v", jobSyncInfo.ParentJobID, err)
			return err
		}
		// get job type and framework from FrameworkVersion
		jobType, framework := j.runtimeClient.GetJobTypeFramework(jobSyncInfo.FrameworkVersion)
		job := &model.Job{
			ID:   jobSyncInfo.ID,
			Type: string(jobType),
			Config: &pfschema.Conf{
				Env: map[string]string{
					pfschema.EnvJobNamespace: jobSyncInfo.Namespace,
				},
			},
			Framework:     framework,
			QueueID:       parentJob.QueueID,
			Status:        jobSyncInfo.Status,
			Message:       jobSyncInfo.Message,
			RuntimeInfo:   jobSyncInfo.RuntimeInfo,
			RuntimeStatus: jobSyncInfo.RuntimeStatus,
			ParentJob:     jobSyncInfo.ParentJobID,
		}
		if err = storage.Job.CreateJob(job); err != nil {
			log.Errorf("In %s, craete job %v failed, err: %v", j.Name(), job, err)
			return err
		}
	}
	return nil
}

func (j *JobSync) doDeleteAction(jobSyncInfo *api.JobSyncInfo) error {
	log.Infof("do delete action, job sync info are as follows. %s", jobSyncInfo.String())
	if _, err := storage.Job.UpdateJob(jobSyncInfo.ID, pfschema.StatusJobTerminated, jobSyncInfo.RuntimeInfo,
		jobSyncInfo.RuntimeStatus, "job is terminated"); err != nil {
		log.Errorf("sync job status failed. jobID: %s, err: %s", jobSyncInfo.ID, err.Error())
		return err
	}
	return nil
}

func (j *JobSync) doUpdateAction(jobSyncInfo *api.JobSyncInfo) error {
	log.Infof("do update action. jobID: %s, action: %s, status: %s, message: %s",
		jobSyncInfo.ID, jobSyncInfo.Action, jobSyncInfo.Status, jobSyncInfo.Message)

	// add time point
	if pfschema.IsImmutableJobStatus(jobSyncInfo.Status) {
		metrics.Job.AddTimestamp(jobSyncInfo.ID, metrics.T8, time.Now(), metrics.Info{
			metrics.FinishedStatusLabel: string(jobSyncInfo.Status),
		})
	}

	if _, err := storage.Job.UpdateJob(jobSyncInfo.ID, jobSyncInfo.Status, jobSyncInfo.RuntimeInfo,
		jobSyncInfo.RuntimeStatus, jobSyncInfo.Message); err != nil {
		log.Errorf("update job failed. jobID: %s, err: %s", jobSyncInfo.ID, err.Error())
		return err
	}
	return nil
}

func (j *JobSync) doTerminateAction(jobSyncInfo *api.JobSyncInfo) error {
	log.Infof("do terminate action. jobID: %s, action: %s, status: %s, message: %s",
		jobSyncInfo.ID, jobSyncInfo.Action, jobSyncInfo.Status, jobSyncInfo.Message)
	job, err := storage.Job.GetJobByID(jobSyncInfo.ID)
	if err != nil {
		log.Infof("do terminate action. jobID: %s not found", jobSyncInfo.ID)
		return nil
	}
	if job.Status != pfschema.StatusJobPending {
		return nil
	}
	err = j.runtimeClient.Delete(jobSyncInfo.ID, jobSyncInfo.Namespace, jobSyncInfo.FrameworkVersion)
	if err != nil {
		log.Errorf("do terminate action failed. jobID[%s] error:[%s]", jobSyncInfo.ID, err.Error())
	}
	return err
}

func (j *JobSync) runTaskWorker() {
	for j.processTaskWorkItem() {
	}
}

func (j *JobSync) processTaskWorkItem() bool {
	obj, shutdown := j.taskQueue.Get()
	if shutdown {
		return false
	}
	taskSyncInfo := obj.(*api.TaskSyncInfo)
	log.Debugf("process task sync. task name: %s/%s, id: %s", taskSyncInfo.Namespace, taskSyncInfo.Name, taskSyncInfo.ID)
	defer j.taskQueue.Done(taskSyncInfo)

	if err := j.syncTaskStatus(taskSyncInfo); err != nil {
		log.Errorf("sync task status failed. taskID: %s, err: %s", taskSyncInfo.ID, err.Error())
		if taskSyncInfo.RetryTimes < DefaultSyncRetryTimes {
			taskSyncInfo.RetryTimes += 1
			j.taskQueue.AddRateLimited(taskSyncInfo)
		}
		j.taskQueue.Forget(taskSyncInfo)
		return true
	}

	j.taskQueue.Forget(taskSyncInfo)
	return true
}

func (j *JobSync) syncTaskStatus(taskSyncInfo *api.TaskSyncInfo) error {
	name := taskSyncInfo.Name
	namespace := taskSyncInfo.Namespace
	_, err := storage.Job.GetJobByID(taskSyncInfo.JobID)
	if err != nil {
		log.Warnf("update task %s/%s status failed, job %s for task not found", namespace, name, taskSyncInfo.JobID)
		return err
	}

	// TODO: get logURL from pod resources
	taskStatus := &model.JobTask{
		ID:               taskSyncInfo.ID,
		JobID:            taskSyncInfo.JobID,
		Name:             taskSyncInfo.Name,
		Namespace:        taskSyncInfo.Namespace,
		NodeName:         taskSyncInfo.NodeName,
		MemberRole:       taskSyncInfo.MemberRole,
		Status:           taskSyncInfo.Status,
		Message:          taskSyncInfo.Message,
		ExtRuntimeStatus: taskSyncInfo.PodStatus,
	}
	if taskSyncInfo.Action == pfschema.Delete {
		taskStatus.DeletedAt.Time = time.Now()
		taskStatus.DeletedAt.Valid = true
	}
	log.Debugf("update job task %s/%s status: %v", namespace, name, taskStatus)
	err = storage.Job.UpdateTask(taskStatus)
	if err != nil {
		log.Errorf("update task %s/%s status in database failed, err %v", namespace, name, err)
		return err
	}
	return nil
}

func (j *JobSync) preHandleTerminatingJob() {
	queues := storage.Queue.ListQueuesByCluster(j.runtimeClient.ClusterID())
	if len(queues) == 0 {
		return
	}
	var queueIDs []string
	for _, q := range queues {
		queueIDs = append(queueIDs, q.ID)
	}

	jobs := storage.Job.ListJobsByQueueIDsAndStatus(queueIDs, pfschema.StatusJobTerminating)
	for _, job := range jobs {
		name := job.ID
		namespace := job.Config.GetNamespace()
		fwVersion := j.runtimeClient.JobFrameworkVersion(pfschema.JobType(job.Type), job.Framework)

		log.Debugf("pre handle terminating job, get %s job %s/%s from cluster", fwVersion, namespace, name)
		_, err := j.runtimeClient.Get(namespace, name, fwVersion)
		if err != nil && k8serrors.IsNotFound(err) {
			j.jobQueue.Add(&api.JobSyncInfo{
				ID:     job.ID,
				Action: pfschema.Delete,
			})
			log.Infof("pre handle terminating %s job enqueue, job name %s/%s", fwVersion, namespace, name)
		}
	}
}

// runJobGCWorker run job gc loop
func (j *JobSync) runJobGCWorker() {
	for j.processJobGCWorkItem() {
	}
}

// processJobGCWorkItem process job gc
func (j *JobSync) processJobGCWorkItem() bool {
	obj, shutdown := j.waitedCleanQueue.Get()
	if shutdown {
		log.Infof("shutdown waited clean queue for %s controller.", j.Name())
		return false
	}
	defer j.waitedCleanQueue.Done(obj)
	gcjob, ok := obj.(*api.FinishedJobInfo)
	if !ok {
		log.Errorf("job %v is not a valid finish job request struct.", obj)
		return true
	}
	log.Infof("clean job info: %+v", gcjob)

	err := j.runtimeClient.Delete(gcjob.Namespace, gcjob.Name, gcjob.FrameworkVersion)
	if err != nil {
		log.Errorf("clean %s job [%s/%s] failed, error：%v",
			gcjob.FrameworkVersion, gcjob.Namespace, gcjob.Name, err)
		return true
	}
	log.Infof("auto clean %s job [%s/%s] succeed.", gcjob.FrameworkVersion, gcjob.Namespace, gcjob.Name)
	return true
}

func (j *JobSync) gcFinishedJob(jobInfo *api.JobSyncInfo) {
	if jobInfo == nil {
		return
	}
	// 当任务结束时：Succeeded 或 Failed 入队
	if isCleanJob(jobInfo.Status) {
		log.Infof("gc finished job[%s/%s] with status %s", jobInfo.Namespace, jobInfo.ID, jobInfo.Status)
		finishedJob := api.FinishedJobInfo{
			Name:             jobInfo.ID,
			Namespace:        jobInfo.Namespace,
			Duration:         getJobTTLSeconds(jobInfo.Annotations, jobInfo.Status),
			FrameworkVersion: jobInfo.FrameworkVersion,
		}
		duration := finishedJob.Duration
		log.Infof("finishedJobDelayEnqueue job[%s] in ns[%s] duration[%v]",
			finishedJob.Name, finishedJob.Namespace, duration)
		j.waitedCleanQueue.AddAfter(&finishedJob, duration)
	}
}

func getJobTTLSeconds(annotation map[string]string, status pfschema.JobStatus) time.Duration {
	// get job TTL seconds from annotation first
	if annotation != nil && len(annotation[pfschema.JobTTLSeconds]) != 0 {
		ttlStr := annotation[pfschema.JobTTLSeconds]
		ttl, err := strconv.Atoi(ttlStr)
		if err == nil {
			return time.Duration(ttl) * time.Second
		}
		log.Warnf("convert ttl second string %s to int failed, err: %v", ttlStr, err)
	}
	// get job TTL seconds from config
	ttlSeconds := DefaultJobTTLSeconds
	switch status {
	case pfschema.StatusJobSucceeded:
		if config.GlobalServerConfig.Job.Reclaim.SucceededJobTTLSeconds > 0 {
			ttlSeconds = config.GlobalServerConfig.Job.Reclaim.SucceededJobTTLSeconds
		}
	case pfschema.StatusJobTerminated, pfschema.StatusJobFailed:
		if config.GlobalServerConfig.Job.Reclaim.FailedJobTTLSeconds > 0 {
			ttlSeconds = config.GlobalServerConfig.Job.Reclaim.FailedJobTTLSeconds
		}
	default:
		log.Warnf("job status %s is not supported", status)
	}
	return time.Duration(ttlSeconds) * time.Second
}

func isCleanJob(jobStatus pfschema.JobStatus) bool {
	if !config.GlobalServerConfig.Job.Reclaim.CleanJob {
		return false
	}
	if config.GlobalServerConfig.Job.Reclaim.SkipCleanFailedJob {
		return pfschema.StatusJobSucceeded == jobStatus
	}
	return pfschema.StatusJobSucceeded == jobStatus || pfschema.StatusJobTerminated == jobStatus || pfschema.StatusJobFailed == jobStatus
}
