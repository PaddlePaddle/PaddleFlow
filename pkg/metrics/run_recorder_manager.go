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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/bluele/gcache"
)

type RunRecorderManager struct {
	Cache gcache.Cache
}

func NewRunRecorderManager() *RunRecorderManager {
	cache := gcache.New(MaxNum).ARC().Build()
	return &RunRecorderManager{
		Cache: cache,
	}
}

func (m *RunRecorderManager) AddRunStageTimeRecord(runID, requestID, status string, stage stageTimeType, timestamp time.Time) {
	logger.LoggerForMetric(MetricRunDuration).Debugf("add run stage[%s] time[%s] with runID[%s]",
		stage, timestamp, runID)
	var runStage *RunStageTimeRecorder
	if m.Cache.Has(runID) {
		runInfo, err := m.Cache.Get(runID)
		if err != nil {
			logger.LoggerForMetric(MetricRunDuration).Errorf("get RunStageTimeRecorder with runID[%s] from MetricManager failed", runID)
			return
		}
		runStage = runInfo.(*RunStageTimeRecorder)
	} else {
		runStage = NewRunStageTimeRecorder(runID, requestID)
		m.Cache.SetWithExpire(runID, runStage, Timeout)
	}

	err := runStage.setStageTime(stage, timestamp)
	if err != nil {
		logger.LoggerForMetric(MetricRunDuration).Errorf(err.Error())
	}

	runStage.Status = status
}

func (m *RunRecorderManager) AddStepStageTimeRecord(runID, stepName string, stage stageTimeType, timestamp time.Time) {
	logger.LoggerForMetric(MetricRunDuration).Debugf("add step[%s] stage[%s] time[%s] with runID[%s] ",
		stepName, stage, timestamp, runID)
	runInfo, err := m.Cache.Get(runID)
	if err != nil {
		logger.LoggerForMetric(MetricRunJobDuration).Errorf("get RunStageTimeRecorder with runID[%s] from MetricManager failed when set StageTime[%s] of step[%s]",
			runID, stage, stepName)
		return
	}

	runStage := runInfo.(*RunStageTimeRecorder)

	var stepStage *StepStageTimeRecorder
	stepInfo, ok := runStage.StepStages.Load(stepName)
	if ok {
		stepStage = stepInfo.(*StepStageTimeRecorder)
	} else {
		stepStage = NewStepStageTimeRecorder(stepName, runID)
		runStage.StepStages.Store(stepName, stepStage)
	}

	err = stepStage.setStageTime(stage, timestamp)
	if err != nil {
		logger.LoggerForMetric(MetricRunJobDuration).Errorf(err.Error())
	}
}

func (m *RunRecorderManager) AddJobStageTimeRecord(runID, stepName, jobID string, status schema.JobStatus, stage stageTimeType, timestamp time.Time) {
	logger.LoggerForMetric(MetricRunDuration).Debugf("add job[%s] stage[%s] time with stepName[%s] of runID[%s] with time[%s]",
		jobID, stage, stepName, runID, timestamp)
	runInfo, err := m.Cache.Get(runID)
	if err != nil {
		logger.LoggerForMetric(MetricRunJobDuration).Errorf("get RunStageTimeRecorder with runID[%s] from MetricManager failed when set StageTime[%s] of job[%s] of step[%s]",
			runID, stage, jobID, stepName)
		return
	}

	runStage := runInfo.(*RunStageTimeRecorder)
	stepInfo, ok := runStage.StepStages.Load(stepName)
	if !ok {
		logger.LoggerForMetric(MetricRunJobDuration).Errorf("get StepStageTimeRecorder with StepName[%s] from RunStageTimeRecorder[%s] failed when set StageTime[%s] of job[%s] ",
			stepName, runID, stage, jobID)
		return
	}
	stepStage := stepInfo.(*StepStageTimeRecorder)

	var jobStage *JobStageTimeRecorder
	jobInfo, ok := stepStage.JobStages.Load(jobID)
	if ok {
		jobStage = jobInfo.(*JobStageTimeRecorder)
		stepStage.JobStages.Store(jobID, jobStage)
	} else {
		jobStage = NewJobStageTimeRecorder(jobID, stepName, runID)
		stepStage.JobStages.Store(jobID, jobStage)
	}

	if _, ok = jobStage.StageTime.Load(StageJobScheduleStartTime); !ok {
		scheduleStartTime, ok := stepStage.StageTime.Load(StageStepScheduleStartTime)
		if !ok {
			logger.LoggerForMetric(MetricRunJobDuration).Errorf("cannot get scheduleStartTime for Job[%s]", jobID)
			return
		}
		err = jobStage.setStageTime(StageJobScheduleStartTime, scheduleStartTime.(time.Time))
		if err != nil {
			logger.LoggerForMetric(MetricRunJobDuration).Error(err.Error())
		}
	}
	err = jobStage.setStageTime(stage, timestamp)
	if err != nil {
		logger.LoggerForMetric(MetricRunJobDuration).Error(err.Error())
	}

	jobStage.Status = status
}
