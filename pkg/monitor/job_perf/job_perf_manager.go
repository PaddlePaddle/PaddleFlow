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

	"github.com/goburrow/cache"
)

const (
	MaxNum       = 10000
	MaxTimePoint = int(T7 + 1)
	Timeout      = time.Hour
	ZeroDuration = time.Duration(0)
)

var (
	ZeroTime = time.Time{}
	Manager  JobPerfManager
)

type JobPerfManager interface {
	AddTimePoint(jobID string, timePoint JobTimePoint, timestamp time.Time)
	GetStatusTime(jobID string, status JobStatus) (time.Duration, bool)
	GetTimePoint(jobID string, timePoint JobTimePoint) (time.Time, bool)
}

func init() {
	Manager = newDefaultJobPerfManager()
}

type defaultJobPerfManager struct {
	cache cache.Cache
}

type cacheItem []time.Time

// Implementation of default job perf manager
func newDefaultJobPerfManager() JobPerfManager {
	// use slru cache (default)
	// high performance concurrent memery cache, see https://github.com/goburrow/cache
	c := cache.New(
		cache.WithMaximumSize(MaxNum),
		cache.WithExpireAfterWrite(Timeout),
	)
	return &defaultJobPerfManager{
		cache: c,
	}
}

func (d *defaultJobPerfManager) AddTimePoint(jobID string, timePoint JobTimePoint, timestamp time.Time) {
	val, ok := d.cache.GetIfPresent(jobID)
	if !ok {
		val = make(cacheItem, MaxTimePoint)
	}
	timePoints := val.(cacheItem)
	timePoints[timePoint] = timestamp
	d.cache.Put(jobID, timePoints)
}

// GetTimePoint returns the time point of the job, if not finished, then return ZeroTime, false
func (d *defaultJobPerfManager) GetTimePoint(jobID string, timePoint JobTimePoint) (time.Time, bool) {
	val, ok := d.cache.GetIfPresent(jobID)
	if !ok {
		return ZeroTime, false
	}
	timePoints := val.(cacheItem)
	return timePoints[timePoint], true
}

// GetStatusTime returns the status time of the job, if not finished, then duration = Now - StartTime
func (d defaultJobPerfManager) GetStatusTime(jobID string, status JobStatus) (time.Duration, bool) {
	val, ok := d.cache.GetIfPresent(jobID)
	if !ok {
		return ZeroDuration, false
	}
	timePoints := val.(cacheItem)

	start, end := getTimePointsByStatus(status)
	startT, endT := timePoints[start], timePoints[end]
	// if start time is zero, means this status has not begun yet
	if startT.Equal(ZeroTime) {
		return ZeroDuration, false
	}
	// if start time is zero, means this status has started but not finished yet
	if endT.Equal(ZeroTime) {
		return time.Now().Sub(startT), true
	}
	return endT.Sub(startT), true
}

func getTimePointsByStatus(status JobStatus) (start, end JobTimePoint) {
	start, end = JobTimePoint(status), JobTimePoint(status+1)
	return
}
