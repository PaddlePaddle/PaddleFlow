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
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type JobPerfCollector struct {
	JobDBUpdatingTime     *prometheus.CounterVec
	JobEnqueueTime        *prometheus.CounterVec
	JobDequeueTime        *prometheus.CounterVec
	JobPendingTime        *prometheus.CounterVec
	JobCreatingTime       *prometheus.CounterVec
	JobRunningTime        *prometheus.CounterVec
	lastSyncedStatusTimes []time.Duration
}

func toJobHelp(name string) string {
	return strings.ReplaceAll(name, "_", " ")
}

func newJobPerfCollector() *JobPerfCollector {
	return &JobPerfCollector{
		JobDBUpdatingTime: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: MetricJobDBUpdatingTime,
				Help: toJobHelp(MetricJobDBUpdatingTime),
			},
			[]string{JobIDLabel},
		),
		JobEnqueueTime: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: MetricJobEnqueueTime,
				Help: toJobHelp(MetricJobEnqueueTime),
			},
			[]string{JobIDLabel},
		),
		JobDequeueTime: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: MetricJobDequeueTime,
				Help: toJobHelp(MetricJobDequeueTime),
			},
			[]string{JobIDLabel},
		),
		JobPendingTime: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: MetricJobPendingTime,
				Help: toJobHelp(MetricJobPendingTime),
			},
			[]string{JobIDLabel},
		),
		JobCreatingTime: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: MetricJobCreatingTime,
				Help: toJobHelp(MetricJobCreatingTime),
			},
			[]string{JobIDLabel},
		),
		JobRunningTime: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: MetricJobRunningTime,
				Help: toJobHelp(MetricJobRunningTime),
			},
			[]string{JobIDLabel},
		),
		lastSyncedStatusTimes: make([]time.Duration, MaxStatus),
	}
}

func (j *JobPerfCollector) Describe(descs chan<- *prometheus.Desc) {
	j.JobDequeueTime.Describe(descs)
	j.JobEnqueueTime.Describe(descs)
	j.JobPendingTime.Describe(descs)
	j.JobCreatingTime.Describe(descs)
	j.JobRunningTime.Describe(descs)
	j.JobDBUpdatingTime.Describe(descs)
}

func (j *JobPerfCollector) Collect(metrics chan<- prometheus.Metric) {
	cache := GetTimestampsCache()
	log.Debugf("%+v", cache)
	j.updateJobPerf()
	j.JobDBUpdatingTime.Collect(metrics)
	j.JobEnqueueTime.Collect(metrics)
	j.JobDequeueTime.Collect(metrics)
	j.JobPendingTime.Collect(metrics)
	j.JobCreatingTime.Collect(metrics)
	j.JobRunningTime.Collect(metrics)
}

func (j *JobPerfCollector) updateJobPerf() {
	timePointsCache := Manager.GetTimestampsCache()
	for jobID, timePoints := range timePointsCache {
		// update metrics
		statusTime, _ := timePoints.GetStatusTime(DBUpdating)
		lastSyncedStatusTime := j.lastSyncedStatusTimes[DBUpdating]
		j.JobDBUpdatingTime.With(prometheus.Labels{JobIDLabel: jobID}).Add(float64(statusTime.Microseconds() - lastSyncedStatusTime.Microseconds()))
		j.lastSyncedStatusTimes[DBUpdating] = statusTime

		statusTime, _ = timePoints.GetStatusTime(EnQueue)
		lastSyncedStatusTime = j.lastSyncedStatusTimes[EnQueue]
		j.JobEnqueueTime.With(prometheus.Labels{JobIDLabel: jobID}).Add(float64(statusTime.Microseconds() - lastSyncedStatusTime.Microseconds()))
		j.lastSyncedStatusTimes[EnQueue] = statusTime

		statusTime, _ = timePoints.GetStatusTime(DeQueue)
		lastSyncedStatusTime = j.lastSyncedStatusTimes[DeQueue]
		j.JobDequeueTime.With(prometheus.Labels{JobIDLabel: jobID}).Add(float64(statusTime.Microseconds() - lastSyncedStatusTime.Microseconds()))
		j.lastSyncedStatusTimes[DeQueue] = statusTime

		statusTime, _ = timePoints.GetStatusTime(Pending)
		lastSyncedStatusTime = j.lastSyncedStatusTimes[Pending]
		j.JobPendingTime.With(prometheus.Labels{JobIDLabel: jobID}).Add(float64(statusTime.Microseconds() - lastSyncedStatusTime.Microseconds()))
		j.lastSyncedStatusTimes[Pending] = statusTime

		statusTime, _ = timePoints.GetStatusTime(Creating)
		lastSyncedStatusTime = j.lastSyncedStatusTimes[Creating]
		j.JobCreatingTime.With(prometheus.Labels{JobIDLabel: jobID}).Add(float64(statusTime.Microseconds() - lastSyncedStatusTime.Microseconds()))
		j.lastSyncedStatusTimes[Creating] = statusTime

		statusTime, _ = timePoints.GetStatusTime(Running)
		lastSyncedStatusTime = j.lastSyncedStatusTimes[Running]
		j.JobRunningTime.With(prometheus.Labels{JobIDLabel: jobID}).Add(float64(statusTime.Microseconds() - lastSyncedStatusTime.Microseconds()))
		j.lastSyncedStatusTimes[Running] = statusTime
	}
}
