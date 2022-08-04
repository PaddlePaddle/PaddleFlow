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
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	//"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type JobMetricCollector struct {
	jobCount *prometheus.CounterVec
	jobTime  *prometheus.GaugeVec
	manager  TimePointManager
}

func toJobHelp(name string) string {
	return strings.ReplaceAll(name, "_", " ")
}

func NewJobMetricsCollector(manager TimePointManager) *JobMetricCollector {
	return &JobMetricCollector{
		jobCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: MetricJobCount,
				Help: toJobHelp(MetricJobCount),
			},
			[]string{JobIDLabel},
		),
		jobTime: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: MetricJobTime,
				Help: toJobHelp(MetricJobTime),
			},
			[]string{JobIDLabel, StatusLabel, QueueIDLabel, FinishedStatusLabel, QueueNameLabel},
		),
		manager: manager,
	}
}

func (j *JobMetricCollector) Describe(descs chan<- *prometheus.Desc) {
	j.jobCount.Describe(descs)
	j.jobTime.Describe(descs)
}

func (j *JobMetricCollector) Collect(metrics chan<- prometheus.Metric) {
	cache := j.manager.GetTimestampsCache()
	printCache(cache)
	j.updateJobPerf()
	j.jobCount.Collect(metrics)
	j.jobTime.Collect(metrics)
}

func printCache(cache map[string]Timestamps) {
	for k, v := range cache {
		log.Debugf("[job perf] cache key: %s, value: %s", k, v)
	}
}

func (j *JobMetricCollector) incrJobTime() {
	j.jobTime.With(prometheus.Labels{})
}

func (j *JobMetricCollector) updateJobPerf() {
	timePointsCache := j.manager.GetTimestampsCache()
	for jobID, timePoints := range timePointsCache {
		// add new metric
		info, ok := j.manager.GetInfo(jobID)
		if !ok {
			continue
		}
		for status := MinStatus; status <= MaxStatus; status++ {
			statusTime, _ := timePoints.GetStatusTime(status)
			j.jobTime.With(prometheus.Labels{
				JobIDLabel:          jobID,
				StatusLabel:         status.String(),
				QueueIDLabel:        info[QueueIDLabel],
				FinishedStatusLabel: info[FinishedStatusLabel],
				QueueNameLabel:      info[QueueNameLabel],
			}).Set(float64(statusTime.Microseconds()))
			log.Debugf("[job perf] job %s, status %s, time: %d", jobID, status, statusTime.Microseconds())
		}
	}
}
