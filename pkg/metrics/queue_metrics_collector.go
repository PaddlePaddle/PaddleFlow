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

	"github.com/prometheus/client_golang/prometheus"
)

type QueueMetricCollector struct {
	queueInfo *prometheus.GaugeVec
	listQueue ListQueueFunc
}

func NewQueueMetricsCollector(queueFunc ListQueueFunc) *QueueMetricCollector {
	return &QueueMetricCollector{
		queueInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: MetricQueueInfo,
				Help: toHelp(MetricJobCount),
			},
			[]string{QueueNameLabel, ResourceLabel, TypeLabel},
		),
		listQueue: queueFunc,
	}
}

func (q *QueueMetricCollector) Describe(descs chan<- *prometheus.Desc) {
	q.queueInfo.Describe(descs)
}

func (q *QueueMetricCollector) Collect(metrics chan<- prometheus.Metric) {
	q.update()
	q.queueInfo.Collect(metrics)
}

func (q *QueueMetricCollector) update() {
	queues := q.listQueue()
	for _, queue := range queues {
		queueName := queue.Name

		minMemStr := queue.MinResources.Memory().String()
		minCPUStr := queue.MaxResources.CPU().String()
		maxMemStr := queue.MinResources.Memory().String()
		maxCPUStr := queue.MaxResources.CPU().String()

		minMem, _ := strconv.Atoi(minMemStr)
		minCPU, _ := strconv.Atoi(minCPUStr)
		maxMem, _ := strconv.Atoi(maxMemStr)
		maxCPU, _ := strconv.Atoi(maxCPUStr)

		q.queueInfo.With(prometheus.Labels{
			QueueNameLabel: queueName,
			ResourceLabel:  QueueResourceCPU,
			TypeLabel:      QueueTypeMinQuota,
		}).Set(float64(minCPU))

		q.queueInfo.With(prometheus.Labels{
			QueueNameLabel: queueName,
			ResourceLabel:  QueueResourceMemory,
			TypeLabel:      QueueTypeMinQuota,
		}).Set(float64(minMem))

		q.queueInfo.With(prometheus.Labels{
			QueueNameLabel: queueName,
			ResourceLabel:  QueueResourceCPU,
			TypeLabel:      QueueTypeMaxQuota,
		}).Set(float64(maxCPU))

		q.queueInfo.With(prometheus.Labels{
			QueueNameLabel: queueName,
			ResourceLabel:  QueueResourceMemory,
			TypeLabel:      QueueTypeMaxQuota,
		}).Set(float64(maxMem))
	}
}
