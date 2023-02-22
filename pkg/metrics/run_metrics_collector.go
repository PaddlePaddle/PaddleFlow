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

type RunMetricCollector struct {
	RunRequest   *prometheus.GaugeVec
	RunReponse   *prometheus.GaugeVec
	RunStartTime *prometheus.GaugeVec
	RunEndTime   *prometheus.GaugeVec
	RunStage     *prometheus.GaugeVec
}

func NewRunMetricsCollector() *RunMetricCollector {
	return &RunMetricCollector{
		RunRequest:   RUNREQUEST,
		RunReponse:   RUNRESPONSE,
		RunStartTime: RUNSTARTTIME,
		RunEndTime:   RUNENDTIME,
		RunStage:     RUNSTAGE,
	}
}

func (r *RunMetricCollector) Describe(descs chan<- *prometheus.Desc) {
	r.RunRequest.Describe(descs)
	r.RunReponse.Describe(descs)
	r.RunStartTime.Describe(descs)
	r.RunEndTime.Describe(descs)
	r.RunStage.Describe(descs)
}

func (r *RunMetricCollector) Collect(metrics chan<- prometheus.Metric) {
	r.RunRequest.Collect(metrics)
	r.RunReponse.Collect(metrics)
	r.RunStartTime.Collect(metrics)
	r.RunEndTime.Collect(metrics)
	r.RunStage.Collect(metrics)
}
