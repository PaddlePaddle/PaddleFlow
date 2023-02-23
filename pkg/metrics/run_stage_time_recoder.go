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
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type stageTimeType = string

const (
	StageRunStartTime               stageTimeType = "run start"
	StageRunEndTime                 stageTimeType = "run end"
	StageRunParseStartTime          stageTimeType = "run parse start"
	StageRunValidateStartTime       stageTimeType = "run validate start"
	StageRunValidateEndTime         stageTimeType = "run validate end"
	StageRunAftertreatmentStartTime stageTimeType = "run aftertreatment start"
	StageRunAftertreatmentEndTime   stageTimeType = "run aftertreatment end"

	StageStepScheduleStartTime stageTimeType = "step schedule start"
	StageStepScheduleEndTime   stageTimeType = "step schedule end"

	StageJobAftertreatmentStartTime stageTimeType = "job aftertreatment start"
	StageJobAftertreatmentEndTime   stageTimeType = "job aftertreatment end"
)

type stageDurationType = string

const (
	// run stage
	StageRunExecuteDuration        = "execution"
	StageRunParseDuration          = "parse"
	StageRunValidateDuration       = "validate"
	StageRunAftertreatmentDuration = "aftertreatment"

	// step stage
	StageStepScheduleDuration = "step schedule"

	// job stage
	StageRunJobAftertreatmentDuration = "job aftertreatment"
)

type StageTimeRecorder struct {
	// 用于进行日志标识
	LoggerMeta string

	StageTime sync.Map
	Support   []stageTimeType
}

func NewStageTimeRecorder(suppport []stageTimeType, loggerMeta string) *StageTimeRecorder {
	return &StageTimeRecorder{
		StageTime:  sync.Map{},
		Support:    suppport,
		LoggerMeta: loggerMeta,
	}
}

func (sr *StageTimeRecorder) isSupported(stage stageTimeType) bool {
	for _, stg := range sr.Support {
		if stage == stg {
			return true
		}
	}

	return false
}

func (sr *StageTimeRecorder) setStageTime(stage stageTimeType, timestamp time.Time) error {
	if !sr.isSupported(stage) {
		return fmt.Errorf("stage[%s] is not supported in %s", stage, sr.LoggerMeta)
	}

	if _, ok := sr.StageTime.Load(stage); ok {
		// 这里只是为了避免重复设置，因此，不返回error
		log.Debugf("the timestamp of the stage[%s] of %s has already been set", stage, sr.LoggerMeta)
		return nil
	}

	sr.StageTime.Store(stage, timestamp)

	return nil
}

func (sr *StageTimeRecorder) calculateStageDuration(start, end stageTimeType) (int64, error) {
	if !sr.isSupported(start) || !sr.isSupported(end) {
		return -1, fmt.Errorf("invailid stage[%s, %s] in %s", start, end, sr.LoggerMeta)
	}

	startTime, ok := sr.StageTime.Load(start)
	if !ok {
		return -1, fmt.Errorf("stage[%s] of %s has not been registered", start, sr.LoggerMeta)
	}
	startTimeStamp := startTime.(time.Time)

	endTime, ok := sr.StageTime.Load(end)
	if !ok {
		return -1, fmt.Errorf("stage[%s] of %s has not been registered", end, sr.LoggerMeta)
	}
	endTimeStamp := endTime.(time.Time)
	return int64(endTimeStamp.Sub(startTimeStamp).Milliseconds()), nil
}

type JobStageTimeRecorder struct {
	*StageTimeRecorder
	JobName  string
	JobID    string
	StepName string
	RunID    string
	Status   JobStatus
}

func NewJobStageTimeRecorder(jobName string, jobID string, stepName string, runID string) *JobStageTimeRecorder {
	LoggerMeta := "job_stage[" + jobID + "]"
	stageTimeRecorder := NewStageTimeRecorder([]stageTimeType{StageJobAftertreatmentStartTime, StageJobAftertreatmentEndTime},
		LoggerMeta)
	return &JobStageTimeRecorder{
		StageTimeRecorder: stageTimeRecorder,
		JobName:           jobName,
		JobID:             jobID,
		StepName:          stepName,
		RunID:             runID,
	}
}

func (j *JobStageTimeRecorder) calculateAftertreatmentDuration() (int64, error) {
	return j.calculateStageDuration(StageJobAftertreatmentStartTime, StageJobAftertreatmentEndTime)
}

type StepStageTimeRecorder struct {
	*StageTimeRecorder
	StepName  string
	RunID     string
	JobStages sync.Map
}

func NewStepStageTimeRecorder(stepName string, runID string) *StepStageTimeRecorder {
	LoggerMeta := "step_stage[" + stepName + "][" + runID + "]"
	support := []stageTimeType{StageStepScheduleStartTime, StageStepScheduleEndTime}
	stageTimeRecorder := NewStageTimeRecorder(support, LoggerMeta)
	return &StepStageTimeRecorder{
		StageTimeRecorder: stageTimeRecorder,
		StepName:          stepName,
		RunID:             runID,
		JobStages:         sync.Map{},
	}
}

func (s *StepStageTimeRecorder) calculateScheduleDuration() (int64, error) {
	return s.calculateStageDuration(StageStepScheduleStartTime, StageStepScheduleEndTime)
}

type RunStageTimeRecorder struct {
	*StageTimeRecorder
	RunID      string
	RequestID  string
	StepStages sync.Map
	Status     string
}

func NewRunStageTimeRecorder(runID, reqID string) *RunStageTimeRecorder {
	LoggerMeta := "run_stage[" + runID + "]"
	support := []stageTimeType{
		StageRunStartTime,
		StageRunEndTime,
		StageRunParseStartTime,
		StageRunValidateStartTime,
		StageRunValidateEndTime,
		StageRunAftertreatmentStartTime,
		StageRunAftertreatmentEndTime,
	}

	stageTimeRecorder := NewStageTimeRecorder(support, LoggerMeta)
	return &RunStageTimeRecorder{
		StageTimeRecorder: stageTimeRecorder,
		RunID:             runID,
		RequestID:         reqID,
		StepStages:        sync.Map{},
	}
}

func (r *RunStageTimeRecorder) calculateExcuteDuration() (int64, error) {
	return r.calculateStageDuration(StageRunStartTime, StageRunEndTime)
}

func (r *RunStageTimeRecorder) calculateParseDuration() (int64, error) {
	return r.calculateStageDuration(StageRunValidateStartTime, StageRunParseStartTime)
}

func (r *RunStageTimeRecorder) calculateValidateDuration() (int64, error) {
	return r.calculateStageDuration(StageRunValidateStartTime, StageRunValidateEndTime)
}

func (r *RunStageTimeRecorder) calculateAftertreatmentDuration() (int64, error) {
	return r.calculateStageDuration(StageRunAftertreatmentStartTime, StageRunAftertreatmentEndTime)
}
