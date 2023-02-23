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

	"github.com/bluele/gcache"
	log "github.com/sirupsen/logrus"
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

func (m *RunRecorderManager) AddRunStageTimeRecord(runID, requestID string, stage stageTimeType, timestamp time.Time) {
	var runStage *RunStageTimeRecorder
	if m.Cache.Has(runID) {
		runInfo, err := m.Cache.Get(runID)
		if err != nil {
			log.Errorf("get RunStageTimeRecorder with runID[%s] from MetricManager failed", runID)
			return
		}
		runStage = runInfo.(*RunStageTimeRecorder)
	} else {
		runStage = NewRunStageTimeRecorder(runID, requestID)
		m.Cache.SetWithExpire(runID, runStage, Timeout)
	}

	err := runStage.setStageTime(stage, timestamp)
	if err != nil {
		log.Errorf(err.Error())
	}
}

func (m *RunRecorderManager) AddStepStageTimeRecord(runID, stepName string, stage stageTimeType, timestamp time.Time) {
	runInfo, err := m.Cache.Get(runID)
	if err != nil {
		log.Errorf("get RunStageTimeRecorder with runID[%s] from MetricManager failed when set StageTime[%s] of step[%s]",
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
		log.Errorf(err.Error())
	}
}

func (m *RunRecorderManager) AddJobStageTimeRecord(runID, stepName, jobID, jobName string, stage stageTimeType, timestamp time.Time) {
	runInfo, err := m.Cache.Get(runID)
	if err != nil {
		log.Errorf("get RunStageTimeRecorder with runID[%s] from MetricManager failed when set StageTime[%s] of job[%s] of step[%s]",
			runID, stage, jobID, stepName)
		return
	}

	runStage := runInfo.(*RunStageTimeRecorder)
	stepInfo, ok := runStage.StepStages.Load(stepName)
	if !ok {
		log.Errorf("get StepStageTimeRecorder with StepName[%s] from RunStageTimeRecorder[%s] failed when set StageTime[%s] of job[%s] ",
			stepName, runID, stage, jobID)
	}
	stepStage := stepInfo.(*StepStageTimeRecorder)

	var jobStage *JobStageTimeRecorder
	jobInfo, ok := stepStage.JobStages.Load(jobID)
	if ok {
		jobStage = jobInfo.(*JobStageTimeRecorder)
		stepStage.JobStages.Store(jobID, jobStage)
	} else {
		jobStage = NewJobStageTimeRecorder(jobName, jobID, stepName, runID)
		stepStage.JobStages.Store(jobID, jobStage)
	}

	err = jobStage.setStageTime(stage, timestamp)
	if err != nil {
		log.Error(err.Error())
	}
}
