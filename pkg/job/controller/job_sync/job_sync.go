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

package job_sync

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"paddleflow/pkg/common/k8s"
	commonschema "paddleflow/pkg/common/schema"
	"paddleflow/pkg/job"
	"paddleflow/pkg/job/controller/framework"
)

const DefaultSyncRetryTimes = 3

type JobSyncInfo struct {
	ID         string
	Status     commonschema.JobStatus
	Runtime    interface{}
	Message    string
	Type       commonschema.JobType
	Action     commonschema.ActionOnJob
	RetryTimes int
}

func init() {
	err := framework.RegisterController(&JobSync{})
	if err != nil {
		log.Errorf("init JobSync failed. error:%s", err.Error())
	}
}

type JobSync struct {
	sync.Mutex
	opt                      *framework.ControllerOption
	jobQueue                 workqueue.RateLimitingInterface
	vcjobInformer            cache.SharedIndexInformer
	sparkApplicationInformer cache.SharedIndexInformer
	podInformer              cache.SharedIndexInformer
	podLister                cache.GenericLister
}

func (j *JobSync) Name() string {
	return "JobSync"
}

func (j *JobSync) Initialize(opt *framework.ControllerOption) error {
	log.Infof("Initialize %s controller!", j.Name())
	j.opt = opt
	sparkAppGVR, err := k8s.GetGVRByGVK(k8s.SparkAppGVK)
	if err != nil {
		log.Warnf("cann't find GroupVersionKind [%s]", k8s.SparkAppGVK)
	} else {
		j.sparkApplicationInformer = j.opt.DynamicFactory.ForResource(sparkAppGVR).Informer()
		j.sparkApplicationInformer.AddEventHandler(cache.FilteringResourceEventHandler{
			FilterFunc: j.responsibleForSparkJob,
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    j.addSparkJob,
				UpdateFunc: j.updateSparkJob,
				DeleteFunc: j.deleteSparkJob,
			},
		})
	}
	vcJobGVR, err := k8s.GetGVRByGVK(k8s.VCJobGVK)
	if err != nil {
		log.Warnf("cann't find GroupVersionKind [%s]", k8s.VCJobGVK)
	} else {
		j.vcjobInformer = j.opt.DynamicFactory.ForResource(vcJobGVR).Informer()
		j.vcjobInformer.AddEventHandler(cache.FilteringResourceEventHandler{
			FilterFunc: j.responsibleForVCJob,
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    j.addVCJob,
				UpdateFunc: j.updateVCJob,
				DeleteFunc: j.deleteVCJob,
			},
		})
	}

	podGVR, err := k8s.GetGVRByGVK(k8s.PodGVK)
	if err != nil {
		return err
	}
	j.podInformer = j.opt.DynamicFactory.ForResource(podGVR).Informer()
	j.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: j.updatePod,
	})
	j.podLister = j.opt.DynamicFactory.ForResource(podGVR).Lister()
	j.jobQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	return nil
}

func (j *JobSync) Run(stopCh <-chan struct{}) {
	log.Infof("Start %s controller!", j.Name())
	go j.opt.DynamicFactory.Start(stopCh)

	if j.sparkApplicationInformer != nil {
		if !cache.WaitForCacheSync(stopCh, j.sparkApplicationInformer.HasSynced) {
			utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to job_sync"))
			return
		}
	}
	if j.vcjobInformer != nil {
		if !cache.WaitForCacheSync(stopCh, j.vcjobInformer.HasSynced) {
			utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to job_sync"))
			return
		}
	}
	if !cache.WaitForCacheSync(stopCh, j.podInformer.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to job_sync"))
		return
	}
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
	if _, err := job.UpdateJob(jobSyncInfo.ID, commonschema.StatusJobTerminated,
		jobSyncInfo.Runtime, ""); err != nil {
		log.Errorf("sync job status failed. jobID:[%s] err:[%s]", jobSyncInfo.ID, err.Error())
		return err
	}
	return nil
}

func (j *JobSync) doUpdateAction(jobSyncInfo *JobSyncInfo) error {
	log.Infof("do update action. jobID:[%s] action:[%s] status:[%s] message:[%s]",
		jobSyncInfo.ID, jobSyncInfo.Action, jobSyncInfo.Status, jobSyncInfo.Message)

	if _, err := job.UpdateJob(jobSyncInfo.ID, jobSyncInfo.Status, jobSyncInfo.Runtime, jobSyncInfo.Message); err != nil {
		log.Errorf("update job failed. jobID:[%s] err:[%s]", jobSyncInfo.ID, err.Error())
		return err
	}
	return nil
}

func (j *JobSync) doTerminateAction(jobSyncInfo *JobSyncInfo) error {
	log.Infof("do terminate action. jobID:[%s] action:[%s] status:[%s] message:[%s]",
		jobSyncInfo.ID, jobSyncInfo.Action, jobSyncInfo.Status, jobSyncInfo.Message)
	jobStatus, err := job.GetJobStatusByID(jobSyncInfo.ID)
	if err != nil {
		log.Infof("do terminate action. jobID[%s] not found", jobSyncInfo.ID)
		return nil
	}
	if jobStatus != commonschema.StatusJobPending {
		return nil
	}
	err = job.StopJobByID(jobSyncInfo.ID)
	if err != nil {
		log.Errorf("do terminate action failed. jobID[%s] error:[%s]", jobSyncInfo.ID, err.Error())
	}
	return err
}
