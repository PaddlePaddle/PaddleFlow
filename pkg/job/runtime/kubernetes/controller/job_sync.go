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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	commonschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime/kubernetes/executor"
)

const (
	JobSyncControllerName = "JobSync"
	DefaultSyncRetryTimes = 3
)

type JobSyncInfo struct {
	ID          string
	Namespace   string
	ParentJobID string
	GVK         schema.GroupVersionKind
	Status      commonschema.JobStatus
	Runtime     interface{}
	Message     string
	Action      commonschema.ActionType
	RetryTimes  int
}

func (js *JobSyncInfo) String() string {
	return fmt.Sprintf("job id: %s, parentJobID: %s, gvk: %s, status: %s, message: %s",
		js.ID, js.ParentJobID, js.GVK, js.Status, js.Message)
}

type TaskSyncInfo struct {
	ID         string
	Name       string
	Namespace  string
	JobID      string
	NodeName   string
	MemberRole commonschema.MemberRole
	Status     commonschema.TaskStatus
	Message    string
	PodStatus  interface{}
	Action     commonschema.ActionType
	RetryTimes int
}

func NewJobSync() Controller {
	return &JobSync{}
}

type JobSync struct {
	sync.Mutex
	opt       *k8s.DynamicClientOption
	jobQueue  workqueue.RateLimitingInterface
	taskQueue workqueue.RateLimitingInterface

	// informerMap contains GroupVersionKind and informer for different kubernetes job
	informerMap map[schema.GroupVersionKind]cache.SharedIndexInformer

	podInformer cache.SharedIndexInformer
	podLister   cache.GenericLister
}

func (j *JobSync) Name() string {
	return JobSyncControllerName
}

func (j *JobSync) Initialize(opt *k8s.DynamicClientOption) error {
	if opt == nil || opt.ClusterInfo == nil {
		return fmt.Errorf("init %s controller failed", j.Name())
	}
	log.Infof("Initialize %s controller for cluster [%s]!", j.Name(), opt.ClusterInfo.Name)
	j.opt = opt
	j.jobQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	j.taskQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	j.informerMap = make(map[schema.GroupVersionKind]cache.SharedIndexInformer)

	for gvk := range k8s.GVKJobStatusMap {
		gvrMap, err := j.opt.GetGVR(gvk)
		if err != nil {
			log.Warnf("cann't find GroupVersionKind [%s], err: %v", gvk, err)
		} else {
			j.informerMap[gvk] = j.opt.DynamicFactory.ForResource(gvrMap.Resource).Informer()
			j.informerMap[gvk].AddEventHandler(cache.FilteringResourceEventHandler{
				FilterFunc: responsibleForJob,
				Handler: cache.ResourceEventHandlerFuncs{
					AddFunc:    j.add,
					UpdateFunc: j.update,
					DeleteFunc: j.delete,
				},
			})
		}
	}

	podGVRMap, err := j.opt.GetGVR(k8s.PodGVK)
	if err != nil {
		return err
	}
	j.podInformer = j.opt.DynamicFactory.ForResource(podGVRMap.Resource).Informer()
	j.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    j.addPod,
		UpdateFunc: j.updatePod,
		DeleteFunc: j.deletePod,
	})
	j.podLister = j.opt.DynamicFactory.ForResource(podGVRMap.Resource).Lister()
	return nil
}

func (j *JobSync) Run(stopCh <-chan struct{}) {
	if len(j.informerMap) == 0 {
		log.Infof("Cluster hasn't any GroupVersionKind, skip %s controller!", j.Name())
		return
	}
	go j.opt.DynamicFactory.Start(stopCh)

	for _, informer := range j.informerMap {
		if !cache.WaitForCacheSync(stopCh, informer.HasSynced) {
			log.Errorf("timed out waiting for caches to %s", j.Name())
			return
		}
	}
	if !cache.WaitForCacheSync(stopCh, j.podInformer.HasSynced) {
		log.Errorf("timed out waiting for pod caches to %s", j.Name())
		return
	}
	log.Infof("Start %s controller for cluster [%s] successfully!", j.Name(), j.opt.ClusterInfo.Name)
	go wait.Until(j.runWorker, 0, stopCh)
	go wait.Until(j.runTaskWorker, 0, stopCh)
}

func (j *JobSync) runWorker() {
	for j.processWorkItem() {
	}
}

func (j *JobSync) processWorkItem() bool {
	obj, shutdown := j.jobQueue.Get()
	if shutdown {
		return false
	}
	jobSyncInfo := obj.(*JobSyncInfo)
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

func (j *JobSync) syncJobStatus(jobSyncInfo *JobSyncInfo) error {
	log.Infof("begin syncJobStatus jobID:[%s] action:[%s]", jobSyncInfo.ID, jobSyncInfo.Action)
	switch jobSyncInfo.Action {
	case commonschema.Create:
		return j.doCreateAction(jobSyncInfo)
	case commonschema.Delete:
		return j.doDeleteAction(jobSyncInfo)
	case commonschema.Update:
		return j.doUpdateAction(jobSyncInfo)
	case commonschema.Terminate:
		return j.doTerminateAction(jobSyncInfo)
	}
	return nil
}

func (j *JobSync) doCreateAction(jobSyncInfo *JobSyncInfo) error {
	log.Infof("do create action, job sync info are as follows. %s", jobSyncInfo.String())
	_, err := models.GetJobByID(jobSyncInfo.ID)
	if err == nil {
		return j.doUpdateAction(jobSyncInfo)
	}
	// only create job for subtask
	if jobSyncInfo.ParentJobID != "" {
		// check weather parent job is exist or not
		parentJob, err := models.GetJobByID(jobSyncInfo.ParentJobID)
		if err != nil {
			log.Errorf("get parent job %s failed, err: %v", jobSyncInfo.ParentJobID, err)
			return err
		}
		jobType, framework := k8s.GetJobTypeAndFramework(jobSyncInfo.GVK)
		job := &models.Job{
			ID:   jobSyncInfo.ID,
			Type: string(jobType),
			Config: &commonschema.Conf{
				Env: map[string]string{
					commonschema.EnvJobNamespace: jobSyncInfo.Namespace,
				},
			},
			Framework:   framework,
			QueueID:     parentJob.QueueID,
			Status:      jobSyncInfo.Status,
			Message:     jobSyncInfo.Message,
			RuntimeInfo: jobSyncInfo.Runtime,
			ParentJob:   jobSyncInfo.ParentJobID,
		}
		if err = models.CreateJob(job); err != nil {
			log.Errorf("craete job %v failed, err: %v", job, err)
			return err
		}
	}
	return nil
}

func (j *JobSync) doDeleteAction(jobSyncInfo *JobSyncInfo) error {
	log.Infof("do delete action, job sync info are as follows. %s", jobSyncInfo.String())
	if _, err := models.UpdateJob(jobSyncInfo.ID, commonschema.StatusJobTerminated,
		jobSyncInfo.Runtime, "job is terminated"); err != nil {
		log.Errorf("sync job status failed. jobID:[%s] err:[%s]", jobSyncInfo.ID, err.Error())
		return err
	}
	return nil
}

func (j *JobSync) doUpdateAction(jobSyncInfo *JobSyncInfo) error {
	log.Infof("do update action. jobID:[%s] action:[%s] status:[%s] message:[%s]",
		jobSyncInfo.ID, jobSyncInfo.Action, jobSyncInfo.Status, jobSyncInfo.Message)

	if _, err := models.UpdateJob(jobSyncInfo.ID, jobSyncInfo.Status, jobSyncInfo.Runtime, jobSyncInfo.Message); err != nil {
		log.Errorf("update job failed. jobID:[%s] err:[%s]", jobSyncInfo.ID, err.Error())
		return err
	}
	return nil
}

func (j *JobSync) doTerminateAction(jobSyncInfo *JobSyncInfo) error {
	log.Infof("do terminate action. jobID:[%s] action:[%s] status:[%s] message:[%s]",
		jobSyncInfo.ID, jobSyncInfo.Action, jobSyncInfo.Status, jobSyncInfo.Message)
	job, err := models.GetJobByID(jobSyncInfo.ID)
	if err != nil {
		log.Infof("do terminate action. jobID[%s] not found", jobSyncInfo.ID)
		return nil
	}
	if job.Status != commonschema.StatusJobPending {
		return nil
	}
	kubeJob, err := executor.NewKubeJob(&api.PFJob{
		JobType: commonschema.JobType(job.Type),
	}, j.opt)
	if err != nil {
		log.Errorf("do terminate action failed. jobID[%s] error:[%s]", jobSyncInfo.ID, err.Error())
		return err
	}
	err = kubeJob.StopJobByID(jobSyncInfo.ID)
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
	taskSyncInfo := obj.(*TaskSyncInfo)
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

func (j *JobSync) syncTaskStatus(taskSyncInfo *TaskSyncInfo) error {
	name := taskSyncInfo.Name
	namespace := taskSyncInfo.Namespace
	_, err := models.GetJobByID(taskSyncInfo.JobID)
	if err != nil {
		log.Warnf("update task %s/%s status failed, job %s for task not found", namespace, name, taskSyncInfo.JobID)
		return err
	}

	// TODO: get logURL from pod resources
	taskStatus := &models.JobTask{
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
	if taskSyncInfo.Action == commonschema.Delete {
		taskStatus.DeletedAt.Time = time.Now()
		taskStatus.DeletedAt.Valid = true
	}
	log.Debugf("update job task %s/%s status: %v", namespace, name, taskStatus)
	err = models.UpdateTask(taskStatus)
	if err != nil {
		log.Errorf("update task %s/%s status in database failed, err %v",
			namespace, name, err)
		return err
	}
	return nil
}

func responsibleForJob(obj interface{}) bool {
	job := obj.(*unstructured.Unstructured)
	labels := job.GetLabels()
	if labels != nil && labels[commonschema.JobOwnerLabel] == commonschema.JobOwnerValue {
		log.Debugf("responsible for handle job. jobName:[%s]", job.GetName())
		return true
	}
	log.Debugf("responsible for skip job. jobName:[%s]", job.GetName())
	return false
}
