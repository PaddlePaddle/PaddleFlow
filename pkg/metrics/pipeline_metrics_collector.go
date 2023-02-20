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

type PipelineMetricCollector struct {
	PipelineRequest *prometheus.GaugeVec
	PipelineReponse *prometheus.GaugeVec
}

func NewPipelineMetricsCollector() *PipelineMetricCollector {
	return &PipelineMetricCollector{
		PipelineRequest: PIPELINEREQUEST,
		PipelineReponse: PIPELINERESPONSE,
	}
}

func (p *PipelineMetricCollector) Describe(descs chan<- *prometheus.Desc) {
	p.PipelineRequest.Describe(descs)
	p.PipelineReponse.Describe(descs)

}

func (p *PipelineMetricCollector) Collect(metrics chan<- prometheus.Metric) {
	p.PipelineRequest.Collect(metrics)
	p.PipelineReponse.Collect(metrics)
}
