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
	"fmt"
	"net/http"

	prom_v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

const (
	DefaultMetricPort = 8231
)

var (
	registry *prometheus.Registry
)

var (
	Job           TimePointManager
	PromAPIClient prom_v1.API
)

func InitMetrics() {
	Job = NewJobMetricTimePointManager()
	// Deprecated
	// this for prometheus way to locate label only, now we change the way to db update
	//PromAPIClient = apiClient
}

func initRegistry(queueFunc ListQueueFunc, jobFunc ListJobFunc) {
	if Job == nil {
		panic("metrics not initialized")
	}
	registry = prometheus.NewRegistry()
	jobCollector := NewJobMetricsCollector(Job, jobFunc)
	queueCollector := NewQueueMetricsCollector(queueFunc)
	registry.MustRegister(jobCollector)
	registry.MustRegister(queueCollector)
}

func StartMetricsService(port int, queueFunc ListQueueFunc, jobFunc ListJobFunc) string {
	initRegistry(queueFunc, jobFunc)
	if port == 0 {
		port = DefaultMetricPort
	}
	if port < 1000 {
		panic("metric port cannot below 1000")
	}
	mx := http.NewServeMux()
	mx.Handle("/metrics", promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support metric.
			EnableOpenMetrics: true,
		},
	))
	metricsAddr := fmt.Sprintf(":%d", port)
	go func() {
		if err := http.ListenAndServe(metricsAddr, mx); err != nil {
			log.Errorf("metrics listenAndServe error: %s", err)
		}
	}()

	log.Infof("metrics listening on %s", metricsAddr)
	return metricsAddr
}

func init() {
	// in case panic in testing
	InitMetrics()
}
