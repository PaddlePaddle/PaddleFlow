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

package metrics

import (
	"strconv"
)

const (
	// T1 api query time
	T1 JobTimePoint = iota
	// T2 db insert time
	T2
	// T3 enqueue time
	T3
	// T4 dequeue time
	T4
	// T5 submit time
	T5
	// T6 scheduled time
	// TODO: T6 is not supported yet
	T6
	// T7 run time
	T7
	// T8 finish(success/fail) time
	T8
)

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

type JobTimePoint int

func (t JobTimePoint) Status() Status {
	if t == MaxTimePoint || t < MinTimePoint {
		return StatusUnknown
	}
	return MinStatus + JobStatus(t)
}

func (t JobTimePoint) Index() int {
	return int(t)
}

type JobStatus int

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

func (j JobStatus) TimePoint() (start TimePoint, end TimePoint) {
	if j == StatusUnknown {
		return MinTimePoint, MinTimePoint
	}
	start, end = JobTimePoint(j-1), JobTimePoint(j)
	return
}
