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
	"strings"
	"sync"
	"time"

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

type jobMetricTimePointManager struct {
	cache       gcache.Cache
	infoCache   *sync.Map
	statusCache *sync.Map
}

// NewJobMetricTimePointManager Implementation of default job metric manager
func NewJobMetricTimePointManager() TimePointManager {
	// TODO: use higher performance cache
	// use arc cache (default)
	c2 := sync.Map{}
	c3 := sync.Map{}
	d := &jobMetricTimePointManager{
		statusCache: &c2,
		infoCache:   &c3,
	}
	c := gcache.New(MaxNum).ARC().Build()
	d.cache = c
	return d
}

// AddTimestamp TODO: value may not consist in concurrent env
func (d *jobMetricTimePointManager) AddTimestamp(jobID string, timePoint TimePoint, timestamp time.Time, queueInfos ...Info) {
	val, err := d.cache.GetIFPresent(jobID)
	if err != nil {
		val = newJobTimestamps()
	}
	timePoints, _ := val.(Timestamps)
	// make sure the time only be set once
	_, ok := timePoints.GetTimestamp(timePoint)
	if ok {
		return
	}
	timePoints.AddTimestamp(timePoint, timestamp)
	_ = d.cache.SetWithExpire(jobID, timePoints, Timeout)
	d.increaseStatusCount(timePoint.Status())
	if len(queueInfos) > 0 {
		d.addInfo(jobID, queueInfos[0])
	}
	// TODO: support T5
	if timePoint == T5 {
		d.AddTimestamp(jobID, T6, timestamp)
	}
}

// GetTimestamp returns the time point of the job, if not finished, then return ZeroTime, false
func (d *jobMetricTimePointManager) GetTimestamp(jobID string, timePoint TimePoint) (time.Time, bool) {
	val, err := d.cache.GetIFPresent(jobID)
	if err != nil {
		return ZeroTime, false
	}
	timePoints, _ := val.(Timestamps)
	return timePoints.GetTimestamp(timePoint)
}

// GetStatusTime returns the status time of the job, if not finished, then duration = Now - StartTime
func (d *jobMetricTimePointManager) GetStatusTime(jobID string, status Status) (time.Duration, bool) {
	val, err := d.cache.GetIFPresent(jobID)
	if err != nil {
		return ZeroDuration, false
	}
	timePoints, _ := val.(Timestamps)
	return timePoints.GetStatusTime(status)
}

func (d *jobMetricTimePointManager) GetTimestampsCache() map[string]Timestamps {
	cacheMap := d.cache.GetALL(true)
	timePointsCache := make(map[string]Timestamps)
	for key, val := range cacheMap {
		timePointsCache[key.(string)], _ = val.(Timestamps)
	}
	return timePointsCache
}

func (d *jobMetricTimePointManager) GetStatusCount(status Status) int64 {
	val, _ := d.statusCache.LoadOrStore(status, int64(0))
	return val.(int64)
}

func (d *jobMetricTimePointManager) addInfo(jobID string, newJobInfo Info) {
	val, ok := d.infoCache.Load(jobID)
	if ok {
		jobInfo, _ := val.(Info)
		if newJobInfo[FinishedStatusLabel] == "" {
			return
		}
		jobInfo[FinishedStatusLabel] = newJobInfo[FinishedStatusLabel]
		newJobInfo = jobInfo
	}
	d.infoCache.Store(jobID, newJobInfo)
}

func (d *jobMetricTimePointManager) GetInfo(jobID string) (Info, bool) {
	val, ok := d.infoCache.Load(jobID)
	if !ok {
		return nil, false
	}
	return val.(Info), true
}

func (d *jobMetricTimePointManager) increaseStatusCount(status Status) {
	if status == StatusUnknown {
		return
	}
	val, _ := d.statusCache.LoadOrStore(status, int64(0))
	count, _ := val.(int64)
	d.statusCache.Store(status, count+1)
}

// implementation for metrics.Timestamps

type jobTimestamps []time.Time

func newJobTimestamps() Timestamps {
	return make(jobTimestamps, MaxTimePoint+1)
}

func (t jobTimestamps) GetStatusTime(status Status) (time.Duration, bool) {
	timePoints := t
	start, end := status.TimePoint()
	startT, endT := timePoints[start.Index()], timePoints[end.Index()]
	// if start time is zero, means this status has not begun yet
	if startT.Equal(ZeroTime) {
		return ZeroDuration, false
	}
	// if start time is zero, means this status has started but not finished yet
	if endT.Equal(ZeroTime) {
		return time.Since(startT), true
	}
	return endT.Sub(startT), true
}

func (t jobTimestamps) GetTimestamp(timePoint TimePoint) (time.Time, bool) {
	ts := t[timePoint.Index()]
	if ts == ZeroTime {
		return ZeroTime, false
	}
	return ts, true
}

func (t jobTimestamps) AddTimestamp(timePoint TimePoint, timestamp time.Time) {
	t[timePoint.Index()] = timestamp
}

func (t jobTimestamps) String() string {
	strs := make([]string, 0, len(t))
	for _, x := range t {
		strs = append(strs, strconv.Itoa(int(x.UnixNano()*1e3)))
	}
	return strings.Join(strs, " ")
}
