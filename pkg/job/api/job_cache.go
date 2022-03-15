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

type QueueJob struct {
	sync.RWMutex
	jobExist sync.Map
	jobMaps  map[QueueID]*PriorityQueue // Keyed by queueID
}

func NewEmptyQueueJobs() *QueueJob {
	return &QueueJob{
		jobMaps: make(map[QueueID]*PriorityQueue),
	}
}

func (qj *QueueJob) GetJob() (*PFJob, bool) {
	qj.RLock()
	defer qj.RUnlock()
	// TODO: get job with fair schedule
	for _, jobQueue := range qj.jobMaps {
		if jobQueue.Empty() {
			continue
		} else {
			return jobQueue.Pop().(*PFJob), true
		}
	}
	return nil, false
}

func (qj *QueueJob) Insert(queueID QueueID, job *PFJob, q *QueueInfo) {
	qj.Lock()
	defer qj.Unlock()
	if _, queueExists := qj.jobMaps[queueID]; !queueExists {
		qj.jobMaps[queueID] = NewPriorityQueue(q.JobOrderFn)
	}
	if _, exist := qj.jobExist.Load(job.ID); !exist {
		qj.jobExist.Store(job.ID, struct{}{})
		jobQueue := qj.jobMaps[queueID]
		jobQueue.Push(job)
	}
}

func (qj *QueueJob) DeleteMark(jobID string) {
	qj.jobExist.Delete(jobID)
}
