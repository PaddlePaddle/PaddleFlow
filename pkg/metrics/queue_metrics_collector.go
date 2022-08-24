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
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type QueueMetricCollector struct {
	queueInfo *prometheus.GaugeVec
}

func NewQueueMetricsCollector() *QueueMetricCollector {
	return &QueueMetricCollector{
		queueInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: MetricQueueInfo,
				Help: toHelp(MetricJobCount),
			},
			[]string{QueueNameLabel, ResourceLabel, TypeLabel},
		),
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
	queues := getQueue()
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

func getQueue() []model.Queue {
	queues, err := storage.Queue().ListQueue(0, 0, "", "root")
	if err != nil {
		log.Errorf("%s", err)
	}
	return queues
}
