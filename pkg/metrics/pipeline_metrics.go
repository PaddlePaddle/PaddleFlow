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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var PIPELINEREQUEST = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricPipelineRequest,
		Help: "the time of the pipeline receive request",
	},
	[]string{RequestIDLabel, ApiNameLabel, RequestMethodLabel},
)

var PIPELINERESPONSE = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricPipelineResponse,
		Help: "the time of the pipeline send response",
	},
	[]string{RequestIDLabel, ApiNameLabel, ResponseCodeLabel, RequestMethodLabel},
)

func AddPipelineResponseMetrics(requestID, apiName, code, method string) {
	PIPELINERESPONSE.With(prometheus.Labels{
		RequestIDLabel:     requestID,
		ApiNameLabel:       apiName,
		ResponseCodeLabel:  code,
		RequestMethodLabel: method,
	}).Set(float64(time.Now().UnixMicro()))
}

func AddPipelineResquestMetrics(requestID, apiName, method string) {
	PIPELINEREQUEST.With(prometheus.Labels{
		RequestIDLabel:     requestID,
		ApiNameLabel:       apiName,
		RequestMethodLabel: method,
	}).Set(float64(time.Now().UnixMicro()))
}
