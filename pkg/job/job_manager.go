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
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

const (
	defaultCacheSize  = 500
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

	// jobQueues contains JobQueue for jobs in queue
	jobQueues api.JobQueues
	// clusterRuntimes contains cluster status and runtime services
	clusterRuntimes ClusterRuntimes
}

func NewJobManagerImpl() (*JobManagerImpl, error) {
	manager := &JobManagerImpl{
		clusterRuntimes: NewClusterRuntimes(),
		jobQueues:       api.NewJobQueues(),
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
	clusterSyncTime := config.GlobalServerConfig.Job.ClusterSyncPeriod
	if clusterSyncTime < defaultExpireTime {
		clusterSyncTime = defaultExpireTime
	}
	jobLoopPeriod := config.GlobalServerConfig.Job.JobLoopPeriod
	if jobLoopPeriod < defaultJobLoop {
		jobLoopPeriod = defaultJobLoop
	}
	m.queueCache = gcache.New(cacheSize).LRU().Build()
	m.queueExpireTime = time.Duration(expireTime) * time.Second
	m.jobLoopPeriod = time.Duration(jobLoopPeriod) * time.Second
	clusterSyncPeriod := time.Duration(clusterSyncTime) * time.Second

	// submit job to cluster
	go m.pJobProcessLoop()

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

			_, find := m.clusterRuntimes.Get(clusterID)
			if !find {
				runtimeSvc, err := runtime.GetOrCreateRuntime(cluster)
				if err != nil {
					log.Errorf("new runtime for cluster[%s] failed, err: %v. skip it", cluster.ID, err)
					continue
				}
				log.Infof("Create new runtime with cluster <%s>", cluster.ID)

				cr := NewClusterRuntimeInfo(cluster.Name, runtimeSvc)
				m.clusterRuntimes.Store(clusterID, cr)
				// start runtime for new cluster
				go m.Run(runtimeSvc, cr.StopCh, clusterID)
			}
		}
		time.Sleep(clusterSyncPeriod)
	}
}

func (m *JobManagerImpl) stopClusterRuntime(clusterID api.ClusterID) {
	log.Infof("stop runtime for cluster: %s\n", clusterID)
	// stop runtime for offline cluster
	cr, ok := m.clusterRuntimes.Get(clusterID)
	if ok && cr != nil {
		close(cr.StopCh)
	}
	m.clusterRuntimes.Delete(clusterID)
	runtime.PFRuntimeMap.Delete(clusterID)
	m.stopClusterQueueSubmit(clusterID)
}

func (m *JobManagerImpl) Run(runtimeService runtime.RuntimeService, stopCh <-chan struct{}, clusterID api.ClusterID) {
	log.Infof("Start %s!", runtimeService.Name())
	// start queue sync
	go runtimeService.SyncQueue(stopCh)
	// start job sync
	go runtimeService.SyncJob(stopCh)
	// start job gc
	go runtimeService.GCJob(stopCh)
}

// jobProcessLoop start job process on cluster deprecated
func (m *JobManagerImpl) jobProcessLoop(jobSubmit func(*api.PFJob) error, clusterID api.ClusterID, stopCh <-chan struct{}) {
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
					log.Debugf("skip queue %s when status is not open", queue.Name)
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
			time.Sleep(m.queueExpireTime)
		}
	}
}

// submitQueueJob submit jobs in queue deprecated
func (m *JobManagerImpl) submitQueueJob(jobSubmit func(*api.PFJob) error, queueID api.QueueID, stopCh <-chan struct{}) {
	cqueue, find := m.GetQueue(queueID)
	if !find {
		log.Errorf("queue %s does not found", queueID)
		return
	}
	queue := cqueue.Queue
	// construct job submit queue
	jobQueue := api.NewPriorityQueue(queue.JobOrderFn)
	queueName := queue.Name
	for {
		select {
		case <-stopCh:
			log.Infof("exit submit job loop for queue[%s]...", queueName)
			return
		default:
			// check whether queue is exist or not
			_, find = m.GetQueue(queueID)
			if !find {
				log.Errorf("queue %s does not found", queueName)
				return
			}
			// get init job from database
			pfJobs := m.listQueueInitJobs(string(queueID))
			if len(pfJobs) == 0 {
				log.Debugf("sleep %s when no job on queue %s", m.jobLoopPeriod, queueName)
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

			jobCount := jobQueue.Len()
			log.Infof("Entering submit %d jobs in queue %s", jobCount, queueName)
			startTime := time.Now()
			for !jobQueue.Empty() {
				// get enqueue job
				job := jobQueue.Pop().(*api.PFJob)
				m.submitJob(jobSubmit, job)
			}
			log.Infof("Leaving submit %d jobs in queue %s, total elapsed time: %s", jobCount, queueName, time.Since(startTime))
		}
	}
}

// TODO: add trace logger support
// submitJob submit a job to cluster
func (m *JobManagerImpl) submitJob(jobSubmit func(*api.PFJob) error, jobInfo *api.PFJob) {
	log.Infof("begin to submit job %s to cluster", jobInfo.ID)
	startTime := time.Now()
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
			trace_logger.KeyWithUpdate(jobInfo.ID).Errorf(msg)
			jobStatus = schema.StatusJobFailed
		} else {
			msg = "submit job to cluster successfully."
			trace_logger.KeyWithUpdate(jobInfo.ID).Infof(msg)
			jobStatus = schema.StatusJobPending
		}
		// new job failed, update db and skip this job
		if dbErr := models.UpdateJobStatus(jobInfo.ID, msg, jobStatus); dbErr != nil {
			errMsg := fmt.Sprintf("update job[%s] status to [%s] failed, err: %v", jobInfo.ID, schema.StatusJobFailed, dbErr)
			log.Errorf(errMsg)
			trace_logger.KeyWithUpdate(jobInfo.ID).Errorf(errMsg)
		}
		log.Infof("submit job %s to cluster elasped time %s", jobInfo.ID, time.Since(startTime))
	} else {
		log.Errorf("job %s is already submit to cluster, skip it", job.ID)
	}
}

type clusterQueue struct {
	Queue      *api.QueueInfo
	RuntimeSvc runtime.RuntimeService
}

func (m *JobManagerImpl) GetQueue(queueID api.QueueID) (*clusterQueue, bool) {
	// check whether queue is exist or not
	var err error
	value, err := m.queueCache.GetIFPresent(queueID)
	if err == nil {
		return value.(*clusterQueue), true
	}
	// get queue from db
	q, err := models.GetQueueByID(string(queueID))
	if err != nil {
		log.Errorf("get queue from database failed, err: %s", err)
		return nil, false
	}
	log.Debugf("get queue from database, and queue info: %v", q)
	if q.Status != schema.StatusQueueOpen {
		log.Debugf("the status of queue %s is %s, skip it", q.Name, q.Status)
		return nil, false
	}
	queueInfo := api.NewQueueInfo(q)

	clusterID := api.ClusterID(q.ClusterId)
	cRuntime, ok := m.clusterRuntimes.Get(clusterID)
	if !ok || cRuntime == nil {
		log.Errorf("get cluster runtime failed, err: %s", err)
		return nil, false
	}

	// set key
	cq := &clusterQueue{
		Queue:      queueInfo,
		RuntimeSvc: cRuntime.RuntimeSvc,
	}
	err = m.queueCache.SetWithExpire(queueID, cq, m.queueExpireTime)
	if err != nil {
		log.Warningf("set cache for queue %s failed, err: %s", queueID, err)
	}
	return cq, true
}

func (m *JobManagerImpl) pJobProcessLoop() {
	log.Infof("start job process loop ...")
	for {
		jobs := models.ListJobByStatus(schema.StatusJobInit)
		startTime := time.Now()
		for idx, job := range jobs {
			// TODO: batch insert group by queue
			queueID := api.QueueID(job.QueueID)
			cQueue, find := m.GetQueue(queueID)
			if !find {
				m.stopQueueSubmit(queueID)
				log.Warnf("get queue from cache failed, stop queue submit")
				continue
			}
			// add more metric
			qInfo := cQueue.Queue
			pfJob, err := api.NewJobInfo(&jobs[idx])
			if err != nil {
				continue
			}

			jobQueue, find := m.jobQueues.Get(queueID)
			if !find {
				jobQueue = api.NewJobQueue(qInfo)
				m.jobQueues.Insert(queueID, jobQueue)
				go m.pSubmitQueueJob(jobQueue, cQueue.RuntimeSvc)
			}

			jobQueue.Insert(pfJob)
		}
		elapsedTime := time.Since(startTime)
		if elapsedTime < m.jobLoopPeriod {
			time.Sleep(m.jobLoopPeriod - elapsedTime)
		}
		log.Debugf("total job %d, job loop elapsed time: %s", len(jobs), elapsedTime)
	}
}

func (m *JobManagerImpl) pSubmitQueueJob(jobQueue *api.JobQueue, runtimeSvc runtime.RuntimeService) {
	if jobQueue == nil || runtimeSvc == nil {
		log.Infof("exit submit job loop, as jobQueue or runtimeSvc is nil")
		return
	}
	name := jobQueue.GetName()
	log.Infof("start submit job loop for queue: %s", name)
	for {
		select {
		case <-jobQueue.StopCh:
			log.Infof("exit submit job loop for queue %s ...", name)
			return
		default:
			startTime := time.Now()
			job, ok := jobQueue.GetJob()
			if ok {
				log.Infof("Entering submit %s job in queue %s", job.ID, name)
				// get enqueue job
				m.submitJob(runtimeSvc.SubmitJob, job)
				jobQueue.DeleteMark(job.ID)
				log.Infof("Leaving submit %s job in queue %s, total elapsed time: %s", job.ID, name, time.Since(startTime))
			} else {
				time.Sleep(m.jobLoopPeriod)
			}
		}
	}
}

func (m *JobManagerImpl) stopClusterQueueSubmit(clusterID api.ClusterID) {
	clusterQueues := models.ListQueuesByCluster(string(clusterID))
	for _, q := range clusterQueues {
		queueID := api.QueueID(q.ID)
		m.stopQueueSubmit(queueID)
	}
}

func (m *JobManagerImpl) stopQueueSubmit(queueID api.QueueID) {
	qc, find := m.jobQueues.Get(queueID)
	if find {
		if qc.StopCh != nil {
			close(qc.StopCh)
		}
		m.jobQueues.Delete(queueID)
	}
}

// ClusterRuntimeInfo defines cluster runtime
type ClusterRuntimeInfo struct {
	Name       string
	StopCh     chan struct{}
	RuntimeSvc runtime.RuntimeService
}

func NewClusterRuntimeInfo(name string, r runtime.RuntimeService) *ClusterRuntimeInfo {
	return &ClusterRuntimeInfo{
		Name:       name,
		StopCh:     make(chan struct{}),
		RuntimeSvc: r,
	}
}

// ClusterRuntimes contains cluster runtimes
type ClusterRuntimes struct {
	sync.RWMutex
	clusterRuntimes map[api.ClusterID]*ClusterRuntimeInfo
}

func NewClusterRuntimes() ClusterRuntimes {
	return ClusterRuntimes{
		clusterRuntimes: make(map[api.ClusterID]*ClusterRuntimeInfo),
	}
}

func (cr *ClusterRuntimes) Get(id api.ClusterID) (*ClusterRuntimeInfo, bool) {
	var result *ClusterRuntimeInfo
	find := false

	if cr.clusterRuntimes != nil {
		cr.RLock()
		defer cr.RUnlock()
		result, find = cr.clusterRuntimes[id]
	}
	return result, find
}

func (cr *ClusterRuntimes) Delete(id api.ClusterID) {
	if cr.clusterRuntimes != nil {
		cr.Lock()
		defer cr.Unlock()
		delete(cr.clusterRuntimes, id)
	}
}

func (cr *ClusterRuntimes) Store(id api.ClusterID, runtimeInfo *ClusterRuntimeInfo) {
	if cr.clusterRuntimes != nil && runtimeInfo != nil {
		cr.Lock()
		defer cr.Unlock()
		cr.clusterRuntimes[id] = runtimeInfo
	}
}
