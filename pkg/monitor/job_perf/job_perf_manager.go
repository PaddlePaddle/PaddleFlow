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
	"sync"
	"time"

	//"github.com/goburrow/cache"
	"github.com/bluele/gcache"
)

const (
	MaxNum       = 10000
	Timeout      = time.Hour
	ZeroDuration = time.Duration(0)
)

var (
	ZeroTime = time.Time{}
)

type JobPerfManager interface {
	AddTimestamp(jobID string, timePoint JobTimePoint, timestamp time.Time)
	GetStatusTime(jobID string, status JobStatus) (time.Duration, bool)
	GetTimestamp(jobID string, timePoint JobTimePoint) (time.Time, bool)
	GetTimestampsCache() map[string]Timestamps
	GetStatusCount(status JobStatus) int64
}

type defaultJobPerfManager struct {
	cache       gcache.Cache
	statusCache *sync.Map
}

type Timestamps []time.Time

// Implementation of default job perf manager
func newDefaultJobPerfManager() JobPerfManager {
	// TODO: use higher performance cache
	// use arc cache (default)
	c := gcache.New(MaxNum).ARC().Build()
	c2 := sync.Map{}
	return &defaultJobPerfManager{
		cache:       c,
		statusCache: &c2,
	}
}

// AddTimestamp TODO: value may not consist in concurrent env
func (d *defaultJobPerfManager) AddTimestamp(jobID string, timePoint JobTimePoint, timestamp time.Time) {

	val, err := d.cache.GetIFPresent(jobID)
	if err != nil {
		val = make(Timestamps, MaxTimePoint+1)
	}
	timePoints := val.(Timestamps)
	// make sure the time only be set once
	if timePoints[timePoint] != ZeroTime {
		return
	}
	timePoints[timePoint] = timestamp
	_ = d.cache.SetWithExpire(jobID, timePoints, Timeout)
	d.increaseStatusCount(timePoint.ToStatus())

	//// TODO: support T5
	//if timePoint == T5 {
	//	d.AddTimestamp(jobID, T6, timestamp)
	//}
}

// GetTimestamp returns the time point of the job, if not finished, then return ZeroTime, false
func (d *defaultJobPerfManager) GetTimestamp(jobID string, timePoint JobTimePoint) (time.Time, bool) {
	val, err := d.cache.GetIFPresent(jobID)
	if err != nil {
		return ZeroTime, false
	}
	timePoints := val.(Timestamps)
	return timePoints[timePoint], true
}

// GetStatusTime returns the status time of the job, if not finished, then duration = Now - StartTime
func (d *defaultJobPerfManager) GetStatusTime(jobID string, status JobStatus) (time.Duration, bool) {
	val, err := d.cache.GetIFPresent(jobID)
	if err != nil {
		return ZeroDuration, false
	}
	timePoints := val.(Timestamps)
	return timePoints.GetStatusTime(status)
}

func (d *defaultJobPerfManager) GetTimestampsCache() map[string]Timestamps {
	cacheMap := d.cache.GetALL(true)
	timePointsCache := make(map[string]Timestamps)
	for key, val := range cacheMap {
		timePointsCache[key.(string)] = val.(Timestamps)
	}
	return timePointsCache
}

func (d *defaultJobPerfManager) GetStatusCount(status JobStatus) int64 {
	val, _ := d.statusCache.LoadOrStore(status, int64(0))
	return val.(int64)
}

func (d *defaultJobPerfManager) increaseStatusCount(status JobStatus) {
	if status == StatusUnknown {
		return
	}
	val, _ := d.statusCache.LoadOrStore(status, int64(0))
	count := val.(int64)
	d.statusCache.Store(status, count+1)
}

func (i Timestamps) GetStatusTime(status JobStatus) (time.Duration, bool) {
	timePoints := i

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
	if status == StatusUnknown {
		return MinTimePoint, MinTimePoint
	}
	start, end = JobTimePoint(status-1), JobTimePoint(status-1)
	return
}
