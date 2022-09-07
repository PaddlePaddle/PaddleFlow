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
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/metrics"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	JobSyncControllerName = "JobSync"
	DefaultSyncRetryTimes = 3
)

type JobSync struct {
	sync.Mutex

	runtimeClient framework.RuntimeClientInterface

	jobQueue  workqueue.RateLimitingInterface
	taskQueue workqueue.RateLimitingInterface
}

func NewJobSync() *JobSync {
	return &JobSync{}
}

func (j *JobSync) Name() string {
	return JobSyncControllerName
}

func (j *JobSync) Initialize(runtimeClient framework.RuntimeClientInterface) error {
	if runtimeClient == nil {
		return fmt.Errorf("init %s controller failed", j.Name())
	}
	j.runtimeClient = runtimeClient
	log.Infof("Initialize %s controller for cluster [%s]!", j.Name(), j.runtimeClient.Cluster())
	j.jobQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	j.taskQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// Register job listeners
	err := j.runtimeClient.RegisterListeners(j.jobQueue, j.taskQueue)
	if err != nil {
		log.Errorf("register event listener failed, err: %v", err)
		return err
	}
	return nil
}

func (j *JobSync) Run(stopCh <-chan struct{}) {
	log.Infof("Start %s controller for cluster [%s] successfully!", j.Name(), j.runtimeClient.Cluster())
	j.runtimeClient.StartLister(stopCh)

	j.preHandleTerminatingJob()
	go wait.Until(j.runJobWorker, 0, stopCh)
	go wait.Until(j.runTaskWorker, 0, stopCh)
}

func (j *JobSync) runJobWorker() {
	for j.processWorkItem() {
	}
}

func (j *JobSync) processWorkItem() bool {
	obj, shutdown := j.jobQueue.Get()
	if shutdown {
		return false
	}
	jobSyncInfo := obj.(*api.JobSyncInfo)
	log.Debugf("process job sync. jobID:[%s]", jobSyncInfo.ID)
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
	log.Infof("begin syncJobStatus jobID:[%s] action:[%s]", jobSyncInfo.ID, jobSyncInfo.Action)
	switch jobSyncInfo.Action {
	case pfschema.Create:
		return j.doCreateAction(jobSyncInfo)
	case pfschema.Delete:
		return j.doDeleteAction(jobSyncInfo)
	case pfschema.Update:
		return j.doUpdateAction(jobSyncInfo)
	case pfschema.Terminate:
		return j.doTerminateAction(jobSyncInfo)
	}
	return nil
}

func getJobTypeFromFramework(framework pfschema.Framework) string {
	// TODO: optimize this code
	var jobType pfschema.JobType
	switch framework {
	case pfschema.FrameworkStandalone:
		jobType = pfschema.TypeSingle
	case pfschema.FrameworkMPI, pfschema.FrameworkPaddle, pfschema.FrameworkTF,
		pfschema.ListenerTypeTask, pfschema.FrameworkMXNet, pfschema.FrameworkPytorch:
		jobType = pfschema.TypeDistributed
	default:
		jobType = pfschema.TypeWorkflow
	}
	return string(jobType)
}

func (j *JobSync) doCreateAction(jobSyncInfo *api.JobSyncInfo) error {
	log.Infof("do create action, job sync info are as follows. %s", jobSyncInfo.String())
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
		// TODO: get job type from framework
		jobType := getJobTypeFromFramework(jobSyncInfo.Framework)
		job := &model.Job{
			ID:   jobSyncInfo.ID,
			Type: jobType,
			Config: &pfschema.Conf{
				Env: map[string]string{
					pfschema.EnvJobNamespace: jobSyncInfo.Namespace,
				},
			},
			Framework:     jobSyncInfo.Framework,
			QueueID:       parentJob.QueueID,
			Status:        jobSyncInfo.Status,
			Message:       jobSyncInfo.Message,
			RuntimeInfo:   jobSyncInfo.RuntimeInfo,
			RuntimeStatus: jobSyncInfo.RuntimeStatus,
			ParentJob:     jobSyncInfo.ParentJobID,
		}
		if err = storage.Job.CreateJob(job); err != nil {
			log.Errorf("craete job %v failed, err: %v", job, err)
			return err
		}
	}
	return nil
}

func (j *JobSync) doDeleteAction(jobSyncInfo *api.JobSyncInfo) error {
	log.Infof("do delete action, job sync info are as follows. %s", jobSyncInfo.String())
	if _, err := storage.Job.UpdateJob(jobSyncInfo.ID, pfschema.StatusJobTerminated, jobSyncInfo.RuntimeInfo,
		jobSyncInfo.RuntimeStatus, "job is terminated"); err != nil {
		log.Errorf("sync job status failed. jobID:[%s] err:[%s]", jobSyncInfo.ID, err.Error())
		return err
	}
	return nil
}

func (j *JobSync) doUpdateAction(jobSyncInfo *api.JobSyncInfo) error {
	log.Infof("do update action. jobID:[%s] action:[%s] status:[%s] message:[%s]",
		jobSyncInfo.ID, jobSyncInfo.Action, jobSyncInfo.Status, jobSyncInfo.Message)

	// add time point
	if pfschema.IsImmutableJobStatus(jobSyncInfo.Status) {
		metrics.Job.AddTimestamp(jobSyncInfo.ID, metrics.T8, time.Now(), metrics.Info{
			metrics.FinishedStatusLabel: string(jobSyncInfo.Status),
		})
	}

	if _, err := storage.Job.UpdateJob(jobSyncInfo.ID, jobSyncInfo.Status, jobSyncInfo.RuntimeInfo,
		jobSyncInfo.RuntimeStatus, jobSyncInfo.Message); err != nil {
		log.Errorf("update job failed. jobID:[%s] err:[%s]", jobSyncInfo.ID, err.Error())
		return err
	}
	return nil
}

func (j *JobSync) doTerminateAction(jobSyncInfo *api.JobSyncInfo) error {
	log.Infof("do terminate action. jobID:[%s] action:[%s] status:[%s] message:[%s]",
		jobSyncInfo.ID, jobSyncInfo.Action, jobSyncInfo.Status, jobSyncInfo.Message)
	job, err := storage.Job.GetJobByID(jobSyncInfo.ID)
	if err != nil {
		log.Infof("do terminate action. jobID[%s] not found", jobSyncInfo.ID)
		return nil
	}
	if job.Status != pfschema.StatusJobPending {
		return nil
	}
	err = j.runtimeClient.Delete(jobSyncInfo.ID, jobSyncInfo.Namespace, jobSyncInfo.FrameworkVersion)
	if err != nil {
		log.Errorf("do terminate action failed. jobID[%s] error:[%s]", jobSyncInfo.ID, err.Error())
	}

	//kubeJob, err := executor.NewKubeJob(&api.PFJob{
	//	JobType: pfschema.JobType(job.Type),
	//}, j.opt)
	//if err != nil {
	//	log.Errorf("do terminate action failed. jobID[%s] error:[%s]", jobSyncInfo.ID, err.Error())
	//	return err
	//}
	//err = kubeJob.StopJobByID(jobSyncInfo.ID)
	//if err != nil {
	//	log.Errorf("do terminate action failed. jobID[%s] error:[%s]", jobSyncInfo.ID, err.Error())
	//}
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
		log.Errorf("update task %s/%s status in database failed, err %v",
			namespace, name, err)
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
		// TODO: get job FrameworkVersion
		gvk, err := k8s.GetJobGVK(pfschema.JobType(job.Type), job.Framework)
		if err != nil {
			log.Warningf("get GroupVersionKind for job %s failed, err: %s", gvk.String(), err)
			continue
		}
		log.Debugf("pre handle terminating job, get %s job %s/%s from cluster", gvk.String(), namespace, name)
		frameworkVersion := pfschema.NewFrameworkVersion(gvk.Kind, gvk.GroupVersion().String())
		_, err = j.runtimeClient.Get(namespace, name, frameworkVersion)
		if err != nil && k8serrors.IsNotFound(err) {
			j.jobQueue.Add(&api.JobSyncInfo{
				ID:     job.ID,
				Action: pfschema.Delete,
			})
			log.Infof("pre handle terminating %s job enqueue, job name %s/%s", gvk.String(), namespace, name)
		}
	}
}
