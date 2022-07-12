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
	defaultJobLoop    = 1
)

type ActiveClustersFunc func() []models.ClusterInfo
type ActiveQueuesFunc func() []models.Queue
type QueueJobsFunc func(string, []schema.JobStatus) []models.Job

type JobManagerImpl struct {
	// activeClusters is a method for listing active clusters from db
	activeClusters ActiveClustersFunc
	// activeQueueJobs is a method for listing jobs on active queue
	// deprecated
	activeQueueJobs QueueJobsFunc
	queueExpireTime time.Duration
	queueCache      gcache.Cache

	listQueueInitJobs func(string) []models.Job
	jobLoopPeriod     time.Duration
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
	m.listQueueInitJobs = models.ListQueueInitJob
	// init queue cache
	cacheSize := config.GlobalServerConfig.Job.QueueCacheSize
	if cacheSize < defaultCacheSize {
		cacheSize = defaultCacheSize
	}
	expireTime := config.GlobalServerConfig.Job.QueueExpireTime
	if expireTime < defaultExpireTime {
		expireTime = defaultExpireTime
	}
	jobLoopPeriod := config.GlobalServerConfig.Job.JobLoopPeriod
	if jobLoopPeriod < defaultJobLoop {
		jobLoopPeriod = defaultJobLoop
	}
	m.queueCache = gcache.New(cacheSize).LRU().Build()
	m.queueExpireTime = time.Duration(expireTime) * time.Second
	m.jobLoopPeriod = time.Duration(jobLoopPeriod) * time.Second

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

// syncClusterJobs deprecated
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
			time.Sleep(m.jobLoopPeriod)
		}
	}
}

// listQueueJobs deprecated
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
	// start queue sync
	go runtimeService.SyncQueue(stopCh)
	// start job sync
	go runtimeService.SyncJob(stopCh)
	// start job gc
	go runtimeService.GCJob(stopCh)
	// submit job to cluster
	go m.clusterJobProcessLoop(runtimeService.SubmitJob, clusterID, stopCh)
}

// clusterJobProcessLoop start job process on cluster
func (m *JobManagerImpl) clusterJobProcessLoop(jobSubmit func(*api.PFJob) error, clusterID api.ClusterID, stopCh <-chan struct{}) {
	queueStatus := make(map[api.QueueID]chan struct{})
	for {
		select {
		case <-stopCh:
			for qid, ch := range queueStatus {
				if ch != nil {
					log.Infof("stop submit loop for queue %s ...", qid)
					close(ch)
				}
			}
			log.Infof("exit job process loop for cluster[%s] ...", clusterID)
			return
		default:
			clusterQueues := models.ListQueuesByCluster(string(clusterID))
			for _, queue := range clusterQueues {
				queueID := api.QueueID(queue.ID)
				if queue.Status != schema.StatusQueueOpen {
					log.Infof("skip queue %s when status is not open", queue.Name)
					// stop submit job loop for queue
					ch, ok := queueStatus[queueID]
					if ok {
						close(ch)
					}
					delete(queueStatus, queueID)
					continue
				}
				_, find := queueStatus[queueID]
				if !find {
					queueStatus[queueID] = make(chan struct{})
					log.Infof("create new submit loop for queue [%s]", queue.Name)
					go m.submitQueueJob(jobSubmit, queueID, queueStatus[queueID])
				}
			}
			time.Sleep(m.jobLoopPeriod)
		}
	}
}

// submitQueueJob submit jobs in queue
func (m *JobManagerImpl) submitQueueJob(jobSubmit func(*api.PFJob) error, queueID api.QueueID, stopCh <-chan struct{}) {
	queue, find := m.GetQueue(queueID)
	if !find {
		log.Errorf("queue %s does not found", queue.Name)
		return
	}
	// construct job submit queue
	jobQueue := api.NewPriorityQueue(queue.JobOrderFn)
	for {
		select {
		case <-stopCh:
			log.Infof("exit submit job loop for queue[%s]...", queue.Name)
			return
		default:
			// check whether queue is exist or not
			queue, find = m.GetQueue(queueID)
			if !find {
				log.Errorf("queue %s does not found", queue.Name)
				return
			}
			// get init job from database
			pfJobs := m.listQueueInitJobs(string(queueID))
			if len(pfJobs) == 0 {
				log.Debugf("sleep %d second when not job on queue %s", m.jobLoopPeriod, queue.Name)
				time.Sleep(m.jobLoopPeriod)
				continue
			}
			// loop for submit queue jobs
			for idx := range pfJobs {
				jobInfo, err := api.NewJobInfo(&pfJobs[idx])
				if err != nil {
					log.Errorf("create paddleflow job %s failed, err: %v", jobInfo.ID, err)
					continue
				}
				jobQueue.Push(jobInfo)
			}

			jobCount := len(pfJobs)
			log.Infof("Entering submit %d jobs in queue %s", jobCount, queue.Name)
			var wg = &sync.WaitGroup{}
			startTime := time.Now()
			for !jobQueue.Empty() {
				job := jobQueue.Pop().(*api.PFJob)
				wg.Add(1)
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					m.submitJob(jobSubmit, job)
				}(wg)
			}
			wg.Wait()
			log.Infof("Leaving submit %d jobs in queue %s, total elapsed time: %s", jobCount, queue.Name, time.Since(startTime))
		}
	}
}

// submitJob submit a job to cluster
func (m *JobManagerImpl) submitJob(jobSubmit func(*api.PFJob) error, jobInfo *api.PFJob) {
	job, err := models.GetJobByID(jobInfo.ID)
	if err != nil {
		log.Errorf("get job %s from database failed, err: %v", job.ID, err)
		return
	}
	// check job status before create job on cluster
	if job.Status == schema.StatusJobInit {
		var jobStatus schema.JobStatus
		var msg string
		err = jobSubmit(jobInfo)
		if err != nil {
			// new job failed, update db and skip this job
			msg = fmt.Sprintf("submit job to cluster failed, err: %s", err)
			log.Errorln(msg)
			jobStatus = schema.StatusJobFailed
		} else {
			msg = "submit job to cluster successfully."
			jobStatus = schema.StatusJobPending
		}
		// new job failed, update db and skip this job
		if dbErr := models.UpdateJobStatus(jobInfo.ID, msg, jobStatus); dbErr != nil {
			log.Errorf("update job[%s] status to [%s] failed, err: %v", jobInfo.ID, schema.StatusJobFailed, dbErr)
		}
	} else {
		log.Errorf("job %s is already submit to cluster, skip it", job.ID)
	}
}

// JobProcessLoop deprecated
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
				job, err := models.GetJobByID(jobInfo.ID)
				if err != nil {
					log.Errorf("get job %s from database failed, err: %v", jobInfo.ID, err)
					queueJobs.DeleteMark(jobInfo.ID)
					continue
				}
				// check job status before create job on cluster
				if job.Status == schema.StatusJobInit {
					err = jobSubmit(jobInfo)
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
