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
	//"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type JobPerfCollector struct {
	JobCount *prometheus.CounterVec
	JobTime  *prometheus.GaugeVec
}

func toJobHelp(name string) string {
	return strings.ReplaceAll(name, "_", " ")
}

func newJobPerfCollector() *JobPerfCollector {
	return &JobPerfCollector{
		JobCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: MetricJobCount,
				Help: toJobHelp(MetricJobCount),
			},
			[]string{JobIDLabel},
		),
		JobTime: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: MetricJobTime,
				Help: toJobHelp(MetricJobTime),
			},
			[]string{JobIDLabel, JobStatusLabel, JobQueueIdLabel},
		),
	}
}

func (j *JobPerfCollector) Describe(descs chan<- *prometheus.Desc) {
	j.JobCount.Describe(descs)
	j.JobTime.Describe(descs)
}

func (j *JobPerfCollector) Collect(metrics chan<- prometheus.Metric) {
	cache := GetTimestampsCache()
	printCache(cache)
	j.updateJobPerf()
	j.JobCount.Collect(metrics)
	j.JobTime.Collect(metrics)
}

func printCache(cache map[string]Timestamps) {
	for k, v := range cache {
		log.Debugf("[job perf] cache key: %s, value: %s", k, v)
	}
}

func (j *JobPerfCollector) incrJobTime() {
	j.JobTime.With(prometheus.Labels{})
}

func (j *JobPerfCollector) updateJobPerf() {
	timePointsCache := Manager.GetTimestampsCache()
	for jobID, timePoints := range timePointsCache {
		// add new metric
		info, _ := Manager.GetJobInfo(jobID)
		queueID := info.QueueID
		for status := MinStatus; status <= MaxStatus; status++ {
			statusTime, _ := timePoints.GetStatusTime(status)
			j.JobTime.With(prometheus.Labels{
				JobIDLabel:      jobID,
				JobStatusLabel:  status.String(),
				JobQueueIdLabel: queueID,
			}).Set(float64(statusTime.Milliseconds()))
			log.Debugf("[job perf] job %s, status %s, time: %d", jobID, status, statusTime.Milliseconds())
		}

	}
}

//
//func getQueueNameByJobID(jobID string) string {
//	job, err := storage.Job.GetJobByID(jobID)
//	if err != nil {
//		return ""
//	}
//	return job.QueueID
//}

func timeDiff(a, b time.Duration) time.Duration {
	res := a - b
	if res < 0 {
		log.Warnf("[job perf] time %s, %s diff is negative", a, b)
		return 0
	}
	return res
}
