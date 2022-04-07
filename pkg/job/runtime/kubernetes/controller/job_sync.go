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
	"sync"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/k8s"
	commonschema "paddleflow/pkg/common/schema"
	"paddleflow/pkg/job/api"
	"paddleflow/pkg/job/runtime/kubernetes/executor"
)

const (
	JobSyncControllerName = "JobSync"
	DefaultSyncRetryTimes = 3
)

type JobSyncInfo struct {
	ID         string
	Status     commonschema.JobStatus
	Runtime    interface{}
	Message    string
	Type       commonschema.JobType
	Action     commonschema.ActionOnJob
	RetryTimes int
}

func NewJobSync() Controller {
	return &JobSync{}
}

type JobSync struct {
	sync.Mutex
	opt      *k8s.DynamicClientOption
	jobQueue workqueue.RateLimitingInterface

	// informerMap contains GroupVersionKind and informer for different kubernetes job
	informerMap map[schema.GroupVersionKind]cache.SharedIndexInformer

	podInformer cache.SharedIndexInformer
	podLister   cache.GenericLister
}

func (j *JobSync) Name() string {
	return JobSyncControllerName
}

func (j *JobSync) Initialize(opt *k8s.DynamicClientOption) error {
	log.Infof("Initialize %s controller!", j.Name())
	j.opt = opt
	j.jobQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	j.informerMap = make(map[schema.GroupVersionKind]cache.SharedIndexInformer)

	for gvk, _ := range k8s.GVKJobStatusMap {
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
		UpdateFunc: j.updatePod,
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
	log.Infof("Start %s controller successfully!", j.Name())
	go wait.Until(j.runWorker, 0, stopCh)
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
		log.Errorf("sync job status failed. jobID:[%s] err:[%s]", jobSyncInfo.ID, err.Error())
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
	case commonschema.Delete:
		return j.doDeleteAction(jobSyncInfo)
	case commonschema.Update:
		return j.doUpdateAction(jobSyncInfo)
	case commonschema.Terminate:
		return j.doTerminateAction(jobSyncInfo)
	}
	return nil
}

func (j *JobSync) doDeleteAction(jobSyncInfo *JobSyncInfo) error {
	log.Infof("sync job status job. action:[%s] jobID:[%s]", jobSyncInfo.Action, jobSyncInfo.ID)
	if _, err := models.UpdateJob(database.DB, jobSyncInfo.ID, commonschema.StatusJobTerminated,
		jobSyncInfo.Runtime, ""); err != nil {
		log.Errorf("sync job status failed. jobID:[%s] err:[%s]", jobSyncInfo.ID, err.Error())
		return err
	}
	return nil
}

func (j *JobSync) doUpdateAction(jobSyncInfo *JobSyncInfo) error {
	log.Infof("do update action. jobID:[%s] action:[%s] status:[%s] message:[%s]",
		jobSyncInfo.ID, jobSyncInfo.Action, jobSyncInfo.Status, jobSyncInfo.Message)

	if _, err := models.UpdateJob(database.DB, jobSyncInfo.ID, jobSyncInfo.Status, jobSyncInfo.Runtime, jobSyncInfo.Message); err != nil {
		log.Errorf("update job failed. jobID:[%s] err:[%s]", jobSyncInfo.ID, err.Error())
		return err
	}
	return nil
}

func (j *JobSync) doTerminateAction(jobSyncInfo *JobSyncInfo) error {
	log.Infof("do terminate action. jobID:[%s] action:[%s] status:[%s] message:[%s]",
		jobSyncInfo.ID, jobSyncInfo.Action, jobSyncInfo.Status, jobSyncInfo.Message)
	job, err := models.GetJobByID(database.DB, jobSyncInfo.ID)
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

func responsibleForJob(obj interface{}) bool {
	job := obj.(*unstructured.Unstructured)
	labels := job.GetLabels()
	if labels[commonschema.JobOwnerLabel] == commonschema.JobOwnerValue {
		log.Debugf("responsible for handle job. jobName:[%s]", job.GetName())
		return true
	}
	log.Debugf("responsible for skip job. jobName:[%s]", job.GetName())
	return false
}
