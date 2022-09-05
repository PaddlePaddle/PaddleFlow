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

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type JobMetricCollector struct {
	jobTime    *prometheus.GaugeVec
	jobGpuInfo *prometheus.GaugeVec
	manager    TimePointManager
	listJob    ListJobFunc
}

func NewJobMetricsCollector(manager TimePointManager, listJob ListJobFunc) *JobMetricCollector {
	c := &JobMetricCollector{
		jobTime: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: MetricJobTime,
				Help: toHelp(MetricJobTime),
			},
			[]string{JobIDLabel, StatusLabel, QueueIDLabel, FinishedStatusLabel, QueueNameLabel},
		),
		jobGpuInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: MetricJobGPUInfo,
				Help: toHelp(MetricJobGPUInfo),
			},
			[]string{JobIDLabel, GpuIdxLabel},
		),
		manager: manager,
		listJob: listJob,
	}
	return c
}

func (j *JobMetricCollector) Describe(descs chan<- *prometheus.Desc) {
	j.jobTime.Describe(descs)
	j.jobGpuInfo.Describe(descs)
}

func (j *JobMetricCollector) Collect(metrics chan<- prometheus.Metric) {
	cache := j.manager.GetTimestampsCache()
	printCache(cache)
	j.updateJobPerf()
	j.updateGpuInfo()
	j.jobTime.Collect(metrics)
	j.jobGpuInfo.Collect(metrics)
}

func printCache(cache map[string]Timestamps) {
	for k, v := range cache {
		log.Debugf("[job perf] cache key: %s, value: %s", k, v)
	}
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

// query labels
// TODO: add cache to change query from sync to async
func (j *JobMetricCollector) updateGpuInfo() {
	jobs := j.listJob()
	for _, job := range jobs {
		gpuIdxs := getGPUIdxFromJob(job)
		log.Debugf("[gpu info] job %s gpu idxs: %+v", job.ID, gpuIdxs)
		for _, idx := range gpuIdxs {
			j.jobGpuInfo.With(prometheus.Labels{
				JobIDLabel:  job.ID,
				GpuIdxLabel: strconv.Itoa(idx),
			}).Set(1.0)
		}
	}
}

func getGPUIdxFromJob(job model.Job) []int {
	var idxs []int
	annotations := GetAnnotationsFromRuntimeInfo(job.RuntimeInfo)
	log.Debugf("[gpu info]job %s annotations: %+v", job.ID, annotations)
	for annotation, value := range annotations {
		if strings.ToLower(annotation) == BaiduGpuIndexLabel {
			idxStrs := strings.Split(value, ",")
			idxs = make([]int, 0, len(idxStrs))
			for _, idxStr := range idxStrs {
				idx, err := strconv.Atoi(idxStr)
				if err != nil {
					continue
				}
				idxs = append(idxs, idx)
			}
			return idxs
		}
	}
	return idxs
}
