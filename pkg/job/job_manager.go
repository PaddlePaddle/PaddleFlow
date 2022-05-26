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
	"time"

	"github.com/bluele/gcache"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
)

const (
	defaultCacheSize  = 100
	defaultExpireTime = 30
)

type ActiveClustersFunc func() []models.ClusterInfo
type ActiveQueuesFunc func() []models.Queue
type QueueJobsFunc func(string, []schema.JobStatus) []models.Job

type JobManagerImpl struct {
	// activeClusters is a method for listing active clusters from db
	activeClusters ActiveClustersFunc
	// activeQueueJobs is a method for listing jobs on active queue
	activeQueueJobs QueueJobsFunc
	queueExpireTime time.Duration
	queueCache      gcache.Cache

	// clusterStatus contains cluster status
	clusterStatus map[api.ClusterID]chan struct{}
}

func NewJobManagerImpl() (*JobManagerImpl, error) {
	manager := &JobManagerImpl{
		clusterStatus: make(map[api.ClusterID]chan struct{}),
	}
	return manager, nil

}

func (m *JobManagerImpl) Start(activeClusters ActiveClustersFunc, activeQueueJobs QueueJobsFunc) {
	log.Infof("Start job manager!")
	m.activeClusters = activeClusters
	m.activeQueueJobs = activeQueueJobs
	// init queue cache
	cacheSize := config.GlobalServerConfig.Job.QueueCacheSize
	if cacheSize < defaultCacheSize {
		cacheSize = defaultCacheSize
	}
	expireTime := config.GlobalServerConfig.Job.QueueExpireTime
	if expireTime < defaultExpireTime {
		expireTime = defaultExpireTime
	}
	m.queueCache = gcache.New(cacheSize).LRU().Build()
	m.queueExpireTime = time.Duration(expireTime) * time.Second

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
	queue, find := m.GetQueue(queueID)
	if !find {
		log.Errorf("queue %s does not found", queueID)
		return fmt.Errorf("queue %s does not found", queueID)
	}

	pfJobs := m.activeQueueJobs(string(queueID), []schema.JobStatus{status})
	for idx := range pfJobs {
		job, err := api.NewJobInfo(&pfJobs[idx])
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
	var err error
	value, err := m.queueCache.GetIFPresent(queueID)
	if err == nil {
		return value.(*api.QueueInfo), true
	}
	// get queue from db
	q, err := models.GetQueueByID(string(queueID))
	if err != nil {
		log.Errorf("get queue from database failed, err: %s", err)
		return nil, false
	}
	log.Debugf("get queue from database, and queue info: %v", q)
	queueInfo := api.NewQueueInfo(q)
	// set key
	err = m.queueCache.SetWithExpire(queueID, queueInfo, m.queueExpireTime)
	if err != nil {
		log.Warningf("set cache for queue %s failed, err: %s", queueID, err)
	}
	return queueInfo, true
}
