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

/**
 * @Author: kiritoxkiriko
 * @Date: 2022/7/26
 * @Description:
 */

package job_perf

import (
	"time"
)

type JobTimePoint int

const (
	// T1 api query time
	T1 JobTimePoint = iota
	// T2 db update time
	T2
	// T3 enqueue time
	T3
	// T4 dequeue time
	T4
	// T5 creat time
	T5
	// T6 run time
	T6
	// T7 finish time
	T7
)

type JobStatus int

const (
	DBUpdating JobStatus = iota
	Enqueue
	Dequeue
	Pending
	Creating
	Running
)

// for job creating monitor

const (
	MetricJobDBUpdatingTime = "job_db_updating_time"
	MetricJobEnqueueTime    = "job_enqueue_time"
	MetricJobDequeueTime    = "job_dequeue_time"
	MetricJobPendingTime    = "job_pending_time"
	MetricJobCreatingTime   = "job_creating_time"
	MetricJobRunningTime    = "job_running_time"
	MetricJobTime           = "job_time"
)

const (
	JobIDLabel     = "jobID"
	JobStatusLabel = "status"
)

var (
	Manager JobPerfManager
)

func init() {
	Manager = newDefaultJobPerfManager()
}

func (j JobStatus) toMetric() string {
	switch j {
	case DBUpdating:
		return MetricJobDBUpdatingTime
	case Enqueue:
		return MetricJobEnqueueTime
	case Dequeue:
		return MetricJobDequeueTime
	case Pending:
		return MetricJobPendingTime
	case Creating:
		return MetricJobCreatingTime
	case Running:
		return MetricJobRunningTime
	}
	return ""
}

func (j JobStatus) String() string {
	switch j {
	case DBUpdating:
		return "DBUpdating"
	case Enqueue:
		return "Enqueue"
	case Dequeue:
		return "Dequeue"
	case Pending:
		return "Pending"
	case Creating:
		return "Creating"
	case Running:
		return "Running"
	}
	return ""
}

func AddTimestamp(jobID string, timePoint JobTimePoint, timestamp time.Time) {
	Manager.AddTimestamp(jobID, timePoint, timestamp)
}
func GetStatusTime(jobID string, status JobStatus) (time.Duration, bool) {
	return Manager.GetStatusTime(jobID, status)
}
func GetTimestamp(jobID string, timePoint JobTimePoint) (time.Time, bool) {
	return Manager.GetTimestamp(jobID, timePoint)
}
func GetTimestampsCache() map[string]Timestamps {
	return Manager.GetTimestampsCache()
}
