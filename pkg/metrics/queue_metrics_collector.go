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
		// add all resource in map
		for resource, quan := range queue.MinResources.Resources {
			val := float64(quan)
			switch resource {
			case QueueResourceCPU:
				//  convert cpu from mill-core to core
				val /= 1000
			default:
				// pass
			}
			q.queueInfo.With(prometheus.Labels{
				QueueNameLabel: queueName,
				ResourceLabel:  resource,
				TypeLabel:      QueueTypeMinQuota,
			}).Set(val)
		}

		for resource, quan := range queue.MaxResources.Resources {
			val := float64(quan)
			switch resource {
			case QueueResourceCPU:
				//  convert cpu from mill-core to core
				val /= 1000
			default:
				// pass
			}
			q.queueInfo.With(prometheus.Labels{
				QueueNameLabel: queueName,
				ResourceLabel:  resource,
				TypeLabel:      QueueTypeMaxQuota,
			}).Set(val)
		}
	}
}
