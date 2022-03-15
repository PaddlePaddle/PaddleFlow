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
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/job/api"
	"paddleflow/pkg/job/runtime"
)

type ActiveClustersFunc func() []models.ClusterInfo
type ActiveQueuesFunc func() []models.Queue
type QueueJobsFunc func(string, []schema.JobStatus) []models.Job

type JobManagerImpl struct {
	// activeClusters is a method for listing active clusters from db
	activeClusters ActiveClustersFunc
	// activeQueues is a method for listing active queues from db
	activeQueues ActiveQueuesFunc
	// activeQueueJobs is a method for listing jobs on active queue
	activeQueueJobs QueueJobsFunc
	queueSyncPeriod time.Duration
	queueMaps       map[api.QueueID]*api.QueueInfo
	queueRWMutex    sync.RWMutex

	// clusterStatus contains cluster status
	clusterStatus map[api.ClusterID]chan struct{}
}

func NewJobManagerImpl() (*JobManagerImpl, error) {
	manager := &JobManagerImpl{
		queueMaps:     make(map[api.QueueID]*api.QueueInfo),
		clusterStatus: make(map[api.ClusterID]chan struct{}),
	}
	return manager, nil

}

func (m *JobManagerImpl) Start(activeClusters ActiveClustersFunc,
	activeQueues ActiveQueuesFunc, activeQueueJobs QueueJobsFunc) error {
	log.Infof("Start job manager!")
	m.activeClusters = activeClusters
	m.activeQueues = activeQueues
	m.activeQueueJobs = activeQueueJobs
	m.queueSyncPeriod = time.Duration(config.GlobalServerConfig.Job.QueueSyncPeriod) * time.Second

	// sync queue info
	go m.SyncQueues()
	for {
		// get active clusters
		clusters := m.activeClusters()

		for _, cluster := range clusters {
			clusterID := api.ClusterID(cluster.ID)
			// skip when cluster status is offline
			if cluster.Status == models.ClusterStatusOffLine {
				log.Warnf("cluster[%s] status is %s, skip it", cluster.ID, models.ClusterStatusOffLine)
				m.stopClusterRuntime(clusterID)
				continue
			}

			_, find := m.clusterStatus[clusterID]
			if !find {
				runtimeSvc, err := runtime.GetOrCreateRuntime(cluster)
				if err != nil {
					log.Errorf("new runtime for cluster[%s] failed, err: %v. skip it", cluster.ID, err)
					continue
				}
				log.Infof("Create new runtime with cluster <%s>", cluster.ID)
				m.clusterStatus[clusterID] = make(chan struct{})
				// start runtime for new cluster
				go m.Run(runtimeSvc, m.clusterStatus[clusterID], clusterID)
			}
		}
		time.Sleep(time.Duration(config.GlobalServerConfig.Job.ClusterSyncPeriod) * time.Second)
	}
	return nil
}

func (m *JobManagerImpl) stopClusterRuntime(clusterID api.ClusterID) {
	log.Infof("stop runtime for cluster: %s\n", clusterID)
	// stop runtime for offline cluster
	ch, ok := m.clusterStatus[clusterID]
	if ok {
		close(ch)
	}
	delete(m.clusterStatus, clusterID)
	runtime.PFRuntimeMap.Delete(clusterID)
}

func (m *JobManagerImpl) SyncQueues() error {
	for {
		// TODO: sync queues with watching
		log.Infof("list active queues!")
		queues := m.activeQueues()
		for _, queue := range queues {
			log.Debugf("queue info <%v>", queue)
			queueID := api.QueueID(queue.ID)
			m.queueRWMutex.Lock()
			m.queueMaps[queueID] = api.NewQueueInfo(queue)
			m.queueRWMutex.Unlock()
		}
		time.Sleep(m.queueSyncPeriod)
	}
	return nil
}

func (m *JobManagerImpl) syncClusterJobs(clusterID api.ClusterID, submitJobs *api.QueueJob, stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			log.Infof("exit sync job cache for cluster[%s] loop...", clusterID)
			return
		default:
			clusterQueues := models.ListQueuesByCluster(string(clusterID))
			for _, queue := range clusterQueues {
				if queue.Status != schema.StatusQueueOpen {
					log.Infof("skip queue %s when status is not open", queue.Name)
					continue
				}
				queueID := api.QueueID(queue.ID)
				// construct job submit queue
				if err := m.listQueueJobs(queueID, schema.StatusJobInit, submitJobs); err != nil {
					log.Errorf("failed to list pending jobs on queue[%s] from cluster[%s], err: %v", queueID, clusterID, err)
				}
			}
			time.Sleep(time.Duration(config.GlobalServerConfig.Job.JobLoopPeriod) * time.Second)
		}
	}
}

func (m *JobManagerImpl) listQueueJobs(queueID api.QueueID, status schema.JobStatus, queueJobs *api.QueueJob) error {
	if queueJobs == nil {
		return nil
	}
	// check whether queue is exist or not
	m.queueRWMutex.RLock()
	defer m.queueRWMutex.RUnlock()
	queue, find := m.queueMaps[queueID]
	if !find {
		log.Errorf("queue %s does not found", queueID)
		return fmt.Errorf("queue %s does not found", queueID)
	}

	pfJobs := m.activeQueueJobs(string(queueID), []schema.JobStatus{status})
	for _, pfJob := range pfJobs {
		job, err := api.NewJobInfo(&pfJob.Config, pfJob.ID)
		if err != nil {
			log.Errorf("new job failed, err: %v", err)
			continue
		}
		queueJobs.Insert(queueID, job, queue)
	}
	return nil
}

func (m *JobManagerImpl) Run(runtimeService runtime.RuntimeService, stopCh <-chan struct{}, clusterID api.ClusterID) {
	log.Infof("Start %s!", runtimeService.Name())
	jobsToSubmit := api.NewEmptyQueueJobs()
	// start list jobs for cluster
	go m.syncClusterJobs(clusterID, jobsToSubmit, stopCh)
	// start queue sync
	go runtimeService.SyncQueue(stopCh)
	// start job sync
	go runtimeService.SyncJob(stopCh)
	// start job gc
	go runtimeService.GCJob(stopCh)
	// submit job
	go m.JobProcessLoop(runtimeService.SubmitJob, stopCh, jobsToSubmit)
}

func (m *JobManagerImpl) JobProcessLoop(jobSubmit func(*api.PFJob) error, stopCh <-chan struct{}, queueJobs *api.QueueJob) {
	find := false
	var jobInfo *api.PFJob
	var msg string
	var jobStatus schema.JobStatus
	for {
		select {
		case <-stopCh:
			log.Infof("exit job loop...")
			return
		default:
			jobInfo, find = queueJobs.GetJob()
			if find {
				err := jobSubmit(jobInfo)
				if err != nil {
					// new job failed, update db and skip this job
					msg = err.Error()
					jobStatus = schema.StatusJobFailed
				} else {
					msg = "submit job to cluster successfully."
					jobStatus = schema.StatusJobPending
				}
				// new job failed, update db and skip this job
				if dbErr := models.UpdateJobStatus(jobInfo.ID, msg, jobStatus); dbErr != nil {
					log.Errorf("update job[%s] status to [%s] failed, err: %v", jobInfo.ID, schema.StatusJobFailed, dbErr)
				}
				queueJobs.DeleteMark(jobInfo.ID)
			} else {
				time.Sleep(time.Duration(config.GlobalServerConfig.Job.JobLoopPeriod) * time.Second)
			}
		}
	}
}

func (m *JobManagerImpl) GetQueue(queueID api.QueueID) (*api.QueueInfo, bool) {
	// check whether queue is exist or not
	m.queueRWMutex.RLock()
	defer m.queueRWMutex.RUnlock()
	queue, find := m.queueMaps[queueID]
	if !find {
		log.Errorf("queue %s does not found", queueID)
		return nil, false
	}
	return queue, find
}
