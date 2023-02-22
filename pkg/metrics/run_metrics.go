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

var RUNREQUEST = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricRunRequest,
		Help: "the time of the run receive request",
	},
	[]string{RequestIDLabel, ApiNameLabel, RequestMethodLabel},
)

var RUNRESPONSE = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricRUNResponse,
		Help: "the time of the run send response",
	},
	[]string{RequestIDLabel, ApiNameLabel, ResponseCodeLabel, RequestMethodLabel},
)

var RUNSTARTTIME = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricRunStartTime,
		Help: "the time of the run start",
	},
	[]string{RequestIDLabel, RunIDLabel},
)

var RUNENDTIME = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricRunEndTime,
		Help: "the time of the run end",
	},
	[]string{RunIDLabel},
)

var RUNSTAGE = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricRunStage,
		Help: "stage of run",
	},
	[]string{RunIDLabel, RunStageLabel, RequestIDLabel, RunStepNameLabel, RunJobIDLabel},
)

func AddRUNResponseMetrics(requestID, apiName, code, method string) {
	RUNRESPONSE.With(prometheus.Labels{
		RequestIDLabel:     requestID,
		ApiNameLabel:       apiName,
		ResponseCodeLabel:  code,
		RequestMethodLabel: method,
	}).Set(float64(time.Now().UnixMicro()))
}

func AddRUNResquestMetrics(requestID, apiName, method string) {
	RUNREQUEST.With(prometheus.Labels{
		RequestIDLabel:     requestID,
		ApiNameLabel:       apiName,
		RequestMethodLabel: method,
	})
}

func AddRunStartTimeMetrics(requestID, runID string) {
	RUNSTARTTIME.With(prometheus.Labels{
		RequestIDLabel: requestID,
		RunIDLabel:     runID,
	})
}

func AddRunENDTimeMetrics(requestID, runID string) {
	RUNENDTIME.With(prometheus.Labels{
		RequestIDLabel: requestID,
		RunIDLabel:     runID,
	})
}

func AddRunStageMetrics(runID, runStage, stepName, jobID, requestID string) {
	RUNSTAGE.With(prometheus.Labels{
		RunIDLabel:       runID,
		RunStageLabel:    runStage,
		RunJobIDLabel:    jobID,
		RunStepNameLabel: stepName,
		RequestIDLabel:   requestID,
	})
}
