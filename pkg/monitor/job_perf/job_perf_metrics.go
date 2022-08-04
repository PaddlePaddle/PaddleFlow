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

package job_perf

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var (
	registry *prometheus.Registry
)

func InitRegistry() {
	registry = prometheus.NewRegistry()
	collector := newJobPerfCollector()
	registry.MustRegister(collector)
}

func StartJobPerfMetricsService(port int) string {
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
			log.Errorf("job perf metrics listenAndServe error: %s", err)
		}
	}()

	log.Infof("job perf metrics listening on %s", metricsAddr)
	return metricsAddr
}
