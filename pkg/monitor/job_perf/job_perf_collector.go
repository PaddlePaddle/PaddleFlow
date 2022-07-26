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

	"github.com/prometheus/client_golang/prometheus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/monitor"
)

type JobPerfCollector struct {
	JobDBUpdatingTime *prometheus.CounterVec
	JobEnqueueTime    *prometheus.CounterVec
	JobDequeueTime    *prometheus.CounterVec
	JobPendingTime    *prometheus.CounterVec
	JobCreatingTime   *prometheus.CounterVec
	JobRunningTime    *prometheus.CounterVec
}

func toJobHelp(name string) string {
	return strings.ReplaceAll(name, "_", " ")
}

func newJobPerfCollector() *JobPerfCollector {
	return &JobPerfCollector{
		JobDBUpdatingTime: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: monitor.MetricJobAPIHandlingTime,
				Help: toJobHelp(monitor.MetricJobAPIHandlingTime),
			},
			[]string{monitor.JobIDLabel},
		),
		JobEnqueueTime: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: monitor.MetricJobEnqueueTime,
				Help: toJobHelp(monitor.MetricJobEnqueueTime),
			},
			[]string{monitor.JobIDLabel},
		),
		JobDequeueTime: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: monitor.MetricJobDequeueTime,
				Help: toJobHelp(monitor.MetricJobDequeueTime),
			},
			[]string{monitor.JobIDLabel},
		),
		JobPendingTime: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: monitor.MetricJobPendingTime,
				Help: toJobHelp(monitor.MetricJobPendingTime),
			},
			[]string{monitor.JobIDLabel},
		),
		JobCreatingTime: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: monitor.MetricJobCreatingTime,
				Help: toJobHelp(monitor.MetricJobCreatingTime),
			},
			[]string{monitor.JobIDLabel},
		),
		JobRunningTime: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: monitor.MetricJobRunningTime,
				Help: toJobHelp(monitor.MetricJobRunningTime),
			},
			[]string{monitor.JobIDLabel},
		),
	}
}
