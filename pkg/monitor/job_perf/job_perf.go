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
	"strconv"
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
	// T5 create time
	T5
	// T6 schedule time
	// TODO: T6 is not supported yet
	T6
	// T7 run time
	T7
	// T8 finish time
	T8
)

type JobStatus int

const (
	StatusUnknown JobStatus = iota
	StatusDBInserting
	StatusEnqueue
	StatusDequeue
	StatusSubmitting
	StatusPending
	StatusCreating
	StatusRunning
)

const (
	MinTimePoint = T1
	MinStatus    = StatusDBInserting
	MaxTimePoint = T8
	MaxStatus    = StatusRunning
)

// for job creating monitor

const (
	MetricJobTime  = "pf_perf_job_time"
	MetricJobCount = "pf_perf_job_count"
)

const (
	JobIDLabel             = "jobID"
	JobStatusLabel         = "status"
	JobQueueIdLabel        = "queueID"
	JobFinishedStatusLabel = "finishedStatus"
)

var (
	Manager JobPerfManager
)

func init() {
	Manager = newDefaultJobPerfManager()
}

func (t JobTimePoint) ToStatus() JobStatus {
	if t == MaxTimePoint || t < MinTimePoint {
		return StatusUnknown
	}
	return MinStatus + JobStatus(t)
}

func (j JobStatus) String() string {
	var str string
	switch j {
	case StatusDBInserting:
		str = "DBInserting"
	case StatusEnqueue:
		str = "Enqueue"
	case StatusDequeue:
		str = "Dequeue"
	case StatusPending:
		str = "Pending"
	case StatusSubmitting:
		str = "Submitting"
	case StatusRunning:
		str = "Running"
	case StatusCreating:
		str = "Creating"
	default:
		j = StatusUnknown
		str = "Unknown"
	}
	str = strconv.Itoa(int(j)) + "-" + str
	return str
}

func AddTimestamp(jobID string, timePoint JobTimePoint, timestamp time.Time, jobInfos ...JobInfo) {
	Manager.AddTimestamp(jobID, timePoint, timestamp, jobInfos...)
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
