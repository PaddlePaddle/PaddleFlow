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
	StopCh   chan struct{}
	Queue    *QueueInfo
	jobExist sync.Map
	Jobs     *PriorityQueue
}

func NewQueueJob(q *QueueInfo) *QueueJob {
	return &QueueJob{
		StopCh: make(chan struct{}),
		Queue:  q,
		Jobs:   NewPriorityQueue(q.JobOrderFn),
	}
}

func (qj *QueueJob) GetName() string {
	return qj.Queue.Name
}

func (qj *QueueJob) Insert(job *PFJob) {
	qj.Lock()
	defer qj.Unlock()
	if qj.Jobs != nil && job != nil {
		if _, exist := qj.jobExist.Load(job.ID); !exist {
			qj.jobExist.Store(job.ID, struct{}{})
			qj.Jobs.Push(job)
		}
	}
}

func (qj *QueueJob) GetJob() (*PFJob, bool) {
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

func (qj *QueueJob) DeleteMark(jobID string) {
	qj.jobExist.Delete(jobID)
}
