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
	log "github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"
)

type MetricRunCollector struct {
	durationManager      *RunRecorderManager
	runDurationMetric    *prometheus.GaugeVec
	stepDurationMetric   *prometheus.GaugeVec
	runJobDurationMetric *prometheus.GaugeVec
}

func NewMetricRunCollector() *MetricRunCollector {
	return &MetricRunCollector{
		durationManager: RunMetricManger,

		runDurationMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: MetricRunDuration,
				Help: toHelp(MetricRunDuration),
			},
			[]string{RunIDLabel, RunStageLabel, RequestIDLabel, FinishedStatusLabel}),

		stepDurationMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: MetricStepDuration,
				Help: toHelp(MetricStepDuration),
			},
			[]string{RunIDLabel, RunStepNameLabel, RunStepStageLabel}),

		runJobDurationMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: MetricRunJobDuration,
				Help: toHelp(MetricRunJobDuration),
			},
			[]string{RunJobNameLabel, RunStepNameLabel, FinishedStatusLabel, RunJobStageLabel}),
	}
}

func (rm *MetricRunCollector) Describe(descs chan<- *prometheus.Desc) {
	rm.runDurationMetric.Describe(descs)
	rm.stepDurationMetric.Describe(descs)
	rm.runJobDurationMetric.Describe(descs)
}

func (rm *MetricRunCollector) Collect(ch chan<- prometheus.Metric) {
	rm.generateMetric()
	rm.runDurationMetric.Collect(ch)
	rm.stepDurationMetric.Collect(ch)
	rm.runJobDurationMetric.Collect(ch)
}

func (rm *MetricRunCollector) generateMetric() {
	for id, runRecorderInf := range rm.durationManager.Cache.GetALL(true) {
		// 为了协程安全以及实现起来简单，如果run没有处于終态，则不会计算相应的metric
		runRecorder := runRecorderInf.(*RunStageTimeRecorder)
		if _, ok := runRecorder.StageTime.Load(StageRunEndTime); !ok {
			continue
		}

		rm.durationManager.Cache.Remove(id)
		rm.generateRunMetricByRunRecorder(runRecorder)
		runRecorder.StepStages.Range(rm.generateStepMetricByStepRecorder)
	}
}

func (rm *MetricRunCollector) generateRunMetricByRunRecorder(runRecorder *RunStageTimeRecorder) {
	executeDuration, err := runRecorder.calculateExcuteDuration()
	if err != nil {
		log.Error(err.Error())
	} else {
		rm.runDurationMetric.With(
			prometheus.Labels{
				RunIDLabel:          runRecorder.RunID,
				RunStageLabel:       StageRunExecuteDuration,
				RequestIDLabel:      runRecorder.RequestID,
				FinishedStatusLabel: runRecorder.Status,
			}).Set(float64(executeDuration))
	}

	parseDuration, err := runRecorder.calculateParseDuration()
	if err != nil {
		log.Error(err.Error())
	} else {
		rm.runDurationMetric.With(
			prometheus.Labels{
				RunIDLabel:          runRecorder.RunID,
				RunStageLabel:       StageRunParseDuration,
				RequestIDLabel:      runRecorder.RequestID,
				FinishedStatusLabel: runRecorder.Status,
			}).Set(float64(parseDuration))
	}

	validateDuration, err := runRecorder.calculateValidateDuration()
	if err != nil {
		log.Error(err.Error())
	} else {
		rm.runDurationMetric.With(
			prometheus.Labels{
				RunIDLabel:          runRecorder.RunID,
				RunStageLabel:       StageRunValidateDuration,
				RequestIDLabel:      runRecorder.RequestID,
				FinishedStatusLabel: runRecorder.Status,
			}).Set(float64(validateDuration))
	}

	aftertreatmentDuration, err := runRecorder.calculateAftertreatmentDuration()
	if err != nil {
		log.Error(err.Error())
	} else {
		rm.runDurationMetric.With(
			prometheus.Labels{
				RunIDLabel:          runRecorder.RunID,
				RunStageLabel:       StageRunAftertreatmentDuration,
				RequestIDLabel:      runRecorder.RequestID,
				FinishedStatusLabel: runRecorder.Status,
			}).Set(float64(aftertreatmentDuration))
	}
}

func (rm *MetricRunCollector) generateStepMetricByStepRecorder(id any, recorder any) bool {
	stepRecorder := recorder.(*StepStageTimeRecorder)

	scheduleDuration, err := stepRecorder.calculateScheduleDuration()
	if err != nil {
		log.Error(err.Error())
	} else {
		rm.stepDurationMetric.With(
			prometheus.Labels{
				RunIDLabel:        stepRecorder.RunID,
				RunStepNameLabel:  stepRecorder.StepName,
				RunStepStageLabel: StageStepScheduleDuration,
			}).Set(float64(scheduleDuration))
	}

	stepRecorder.JobStages.Range(rm.generateJobMetricByJobRecorder)
	return true
}

func (rm *MetricRunCollector) generateJobMetricByJobRecorder(id any, recorder any) bool {
	jobRecorder := recorder.(*JobStageTimeRecorder)
	d, err := jobRecorder.calculateAftertreatmentDuration()
	if err != nil {
		log.Error(err.Error())
	} else {
		rm.runJobDurationMetric.With(
			prometheus.Labels{
				RunJobNameLabel:     jobRecorder.JobName,
				RunStepNameLabel:    jobRecorder.StepName,
				FinishedStatusLabel: jobRecorder.Status.String(),
				RunJobStageLabel:    StageRunJobAftertreatmentDuration,
			}).Set(float64(d))
	}
	return true
}
