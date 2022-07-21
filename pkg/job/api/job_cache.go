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

package api

import (
	"sync"
)

type JobQueue struct {
	sync.RWMutex
	StopCh   chan struct{}
	Queue    *QueueInfo
	jobExist sync.Map
	Jobs     *PriorityQueue
}

func NewJobQueue(q *QueueInfo) *JobQueue {
	return &JobQueue{
		StopCh: make(chan struct{}),
		Queue:  q,
		Jobs:   NewPriorityQueue(q.JobOrderFn),
	}
}

func (qj *JobQueue) GetName() string {
	name := ""
	if qj.Queue != nil {
		name = qj.Queue.Name
	}
	return name
}

func (qj *JobQueue) Insert(job *PFJob) {
	if qj.Jobs != nil && job != nil {
		qj.Lock()
		defer qj.Unlock()
		if _, exist := qj.jobExist.Load(job.ID); !exist {
			qj.jobExist.Store(job.ID, struct{}{})
			qj.Jobs.Push(job)
		}
	}
}

func (qj *JobQueue) GetJob() (*PFJob, bool) {
	if qj.Jobs != nil {
		qj.RLock()
		defer qj.RUnlock()
		if qj.Jobs.Empty() {
			return nil, false
		} else {
			return qj.Jobs.Pop().(*PFJob), true
		}
	}
	return nil, false
}

func (qj *JobQueue) DeleteMark(jobID string) {
	qj.jobExist.Delete(jobID)
}

// JobQueues the collect of JobQueue
type JobQueues struct {
	sync.RWMutex
	queueJobs map[QueueID]*JobQueue
}

func NewJobQueues() JobQueues {
	return JobQueues{
		queueJobs: make(map[QueueID]*JobQueue),
	}
}

func (jq *JobQueues) Get(id QueueID) (*JobQueue, bool) {
	if jq.queueJobs != nil {
		jq.RLock()
		defer jq.RUnlock()
		q, find := jq.queueJobs[id]
		return q, find
	}
	return nil, false
}

func (jq *JobQueues) Insert(id QueueID, jobQueue *JobQueue) {
	if jq.queueJobs != nil && jobQueue != nil {
		jq.Lock()
		defer jq.Unlock()
		jq.queueJobs[id] = jobQueue
	}
}

func (jq *JobQueues) Delete(id QueueID) {
	if jq.queueJobs != nil {
		jq.Lock()
		defer jq.Unlock()
		delete(jq.queueJobs, id)
	}
}
