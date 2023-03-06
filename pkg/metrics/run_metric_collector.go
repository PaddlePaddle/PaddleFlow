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
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/prometheus/client_golang/prometheus"
)

type MetricRunCollector struct {
	durationManager      *RunRecorderManager
	runDurationMetric    *prometheus.GaugeVec
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

		runJobDurationMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: MetricRunJobDuration,
				Help: toHelp(MetricRunJobDuration),
			},
			[]string{RunStepNameLabel, FinishedStatusLabel, RunJobStageLabel, RunJobIDLabel}),
	}
}

func (rm *MetricRunCollector) Describe(descs chan<- *prometheus.Desc) {
	rm.runDurationMetric.Describe(descs)
	rm.runJobDurationMetric.Describe(descs)
}

func (rm *MetricRunCollector) Collect(ch chan<- prometheus.Metric) {
	rm.generateMetric()
	rm.runDurationMetric.Collect(ch)
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
		rm.generateRunMetricByRunRecorder(id.(string), runRecorder)
		runRecorder.StepStages.Range(rm.generateStepMetricByStepRecorder)
	}
}

func (rm *MetricRunCollector) generateRunMetricByRunRecorder(id string, runRecorder *RunStageTimeRecorder) {
	logger.LoggerForMetric(MetricRunDuration).Debugf("begin to calculate run duration metric for run %s", id)
	executeDuration, err := runRecorder.calculateExcuteDuration()
	if err != nil {
		logger.LoggerForMetric(MetricRunDuration).Error(err.Error())
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
		logger.LoggerForMetric(MetricRunDuration).Error(err.Error())
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
		logger.LoggerForMetric(MetricRunDuration).Error(err.Error())
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
		logger.LoggerForMetric(MetricRunDuration).Error(err.Error())
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
	stepRecorder.JobStages.Range(rm.generateJobMetricByJobRecorder)
	return true
}

func (rm *MetricRunCollector) generateJobMetricByJobRecorder(id any, recorder any) bool {
	logger.LoggerForMetric(MetricRunDuration).Debugf("begin to calculate job duration metric for job %s", id)
	jobRecorder := recorder.(*JobStageTimeRecorder)
	scheduleDuration, err := jobRecorder.calculateScheduleDuration()
	if err != nil {
		logger.LoggerForMetric(MetricRunJobDuration).Error(err.Error())
	} else {
		rm.runJobDurationMetric.With(
			prometheus.Labels{
				RunStepNameLabel:    jobRecorder.StepName,
				FinishedStatusLabel: string(jobRecorder.Status),
				RunJobStageLabel:    StageRunJobScheduleDuration,
				RunJobIDLabel:       jobRecorder.JobID,
			}).Set(float64(scheduleDuration))

	}

	createDuration, err := jobRecorder.calculateCreateDuration()
	if err != nil {
		logger.LoggerForMetric(MetricRunJobDuration).Error(err.Error())
	} else {
		rm.runJobDurationMetric.With(
			prometheus.Labels{
				RunStepNameLabel:    jobRecorder.StepName,
				FinishedStatusLabel: string(jobRecorder.Status),
				RunJobStageLabel:    StageRunJobCreateDuration,
				RunJobIDLabel:       jobRecorder.JobID,
			}).Set(float64(createDuration))

	}

	d, err := jobRecorder.calculateAftertreatmentDuration()
	if err != nil {
		logger.LoggerForMetric(MetricRunJobDuration).Error(err.Error())
	} else {
		rm.runJobDurationMetric.With(
			prometheus.Labels{
				RunStepNameLabel:    jobRecorder.StepName,
				FinishedStatusLabel: string(jobRecorder.Status),
				RunJobStageLabel:    StageRunJobAftertreatmentDuration,
				RunJobIDLabel:       jobRecorder.JobID,
			}).Set(float64(d))

	}
	return true
}
