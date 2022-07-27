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

	//"github.com/goburrow/cache"
	"github.com/bluele/gcache"
)

const (
	MaxNum       = 10000
	MaxTimePoint = int(T7 + 1)
	MaxStatus    = int(Running + 1)
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
	GetTimePointsCache() map[string]TimePoints
}

func init() {
	Manager = newDefaultJobPerfManager()
}

type defaultJobPerfManager struct {
	cache gcache.Cache
}

type TimePoints []time.Time

// Implementation of default job perf manager
func newDefaultJobPerfManager() JobPerfManager {
	// TODO: use higher performance cache
	// use arc cache (default)
	c := gcache.New(MaxNum).ARC().Build()
	return &defaultJobPerfManager{
		cache: c,
	}
}

func (d *defaultJobPerfManager) AddTimePoint(jobID string, timePoint JobTimePoint, timestamp time.Time) {
	val, err := d.cache.GetIFPresent(jobID)
	if err != nil {
		val = make(TimePoints, MaxTimePoint)
	}
	timePoints := val.(TimePoints)
	timePoints[timePoint] = timestamp
	_ = d.cache.SetWithExpire(jobID, timePoints, Timeout)
}

// GetTimePoint returns the time point of the job, if not finished, then return ZeroTime, false
func (d *defaultJobPerfManager) GetTimePoint(jobID string, timePoint JobTimePoint) (time.Time, bool) {
	val, err := d.cache.GetIFPresent(jobID)
	if err != nil {
		return ZeroTime, false
	}
	timePoints := val.(TimePoints)
	return timePoints[timePoint], true
}

// GetStatusTime returns the status time of the job, if not finished, then duration = Now - StartTime
func (d *defaultJobPerfManager) GetStatusTime(jobID string, status JobStatus) (time.Duration, bool) {
	val, err := d.cache.GetIFPresent(jobID)
	if err != nil {
		return ZeroDuration, false
	}
	timePoints := val.(TimePoints)
	return timePoints.GetStatusTime(status)
}

func (d *defaultJobPerfManager) GetTimePointsCache() map[string]TimePoints {
	cacheMap := d.cache.GetALL(true)
	timePointsCache := make(map[string]TimePoints)
	for key, val := range cacheMap {
		timePointsCache[key.(string)] = val.(TimePoints)
	}
	return timePointsCache
}

func (i TimePoints) GetStatusTime(status JobStatus) (time.Duration, bool) {
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
	start, end = JobTimePoint(status), JobTimePoint(status+1)
	return
}
