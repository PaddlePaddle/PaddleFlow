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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type stageTimeType = string

const (
	// 开始创建run的时间
	StageRunStartTime stageTimeType = "run start"

	// run处于終态的时间
	StageRunEndTime stageTimeType = "run end"

	// 开始解析 runyaml 的时间
	StageRunParseStartTime stageTimeType = "run parse start"

	// 完成 runyaml 解析的时间
	StageRunParseEndTime stageTimeType = "run parse end"

	// 对runyaml 以及相关参数进行校验的开始时间
	StageRunValidateStartTime stageTimeType = "run validate start"

	// 完成对runyaml 以及相关参数进行校验的结束时间
	StageRunValidateEndTime stageTimeType = "run validate end"

	// 开始进行Run后处理的时间：即Run检测到处于终态的时间
	StageRunAftertreatmentStartTime stageTimeType = "run aftertreatment start"

	// 开始进行Step的调度时间：即确定Step可以运行的时间点
	StageStepScheduleStartTime stageTimeType = "step schedule start"

	// Job 开始调度的时间，等价于StageStepScheduleStartTime
	// 这里需要再次进行记录是因为循环结构中，不同的job的调度结束时间不一致，为了方便计算每个job的调度时间，所以会在每一个jobTimeRecorder单独记录一次
	// 在StepTimeRecorder中记录该时间点，是因为在Job阶段无法获取到改信息，因此需要在StepTimeRecorder进行记录，然后在jobTimeRecorder进行拷贝操作。
	StageJobScheduleStartTime stageTimeType = "job schedule start"

	// 完成job调度的时间：即在调用Job模块的Create前的时间
	StageJobScheduleEndTime stageTimeType = "job schedule end"

	// 完成Job创建的时间
	StageJobCreateEndTime stageTimeType = "job create end"

	// 开始进行 Job 后处理的时间：也即Job处于終态的时间点
	StageJobAftertreatmentStartTime stageTimeType = "job aftertreatment start"

	// 完成Job 后处理的时间：也即 Job的終态写入数据库的时间
	StageJobAftertreatmentEndTime stageTimeType = "job aftertreatment end"
)

type stageDurationType = string

const (
	// run stage
	StageRunExecuteDuration        = "execution"
	StageRunParseDuration          = "parse"
	StageRunValidateDuration       = "validate"
	StageRunAftertreatmentDuration = "aftertreatment"

	// step stage

	// job stage
	StageRunJobScheduleDuration       = "job schedule"
	StageRunJobCreateDuration         = "job create"
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
		logger.Logger().Debugf("the timestamp of the stage[%s] of %s has already been set", stage, sr.LoggerMeta)
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
	JobID    string
	StepName string
	RunID    string
	Status   schema.JobStatus
}

func NewJobStageTimeRecorder(jobID string, stepName string, runID string) *JobStageTimeRecorder {
	LoggerMeta := "job_stage[" + jobID + "]"
	support := []stageTimeType{
		StageJobScheduleStartTime,
		StageJobScheduleEndTime,
		StageJobCreateEndTime,
		StageJobAftertreatmentStartTime,
		StageJobAftertreatmentEndTime,
	}

	stageTimeRecorder := NewStageTimeRecorder(support, LoggerMeta)
	return &JobStageTimeRecorder{
		StageTimeRecorder: stageTimeRecorder,
		JobID:             jobID,
		StepName:          stepName,
		RunID:             runID,
	}
}

func (j *JobStageTimeRecorder) calculateScheduleDuration() (int64, error) {
	return j.calculateStageDuration(StageJobScheduleStartTime, StageJobScheduleEndTime)
}

func (j *JobStageTimeRecorder) calculateCreateDuration() (int64, error) {
	return j.calculateStageDuration(StageJobScheduleEndTime, StageJobCreateEndTime)
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
	support := []stageTimeType{StageStepScheduleStartTime}
	stageTimeRecorder := NewStageTimeRecorder(support, LoggerMeta)
	return &StepStageTimeRecorder{
		StageTimeRecorder: stageTimeRecorder,
		StepName:          stepName,
		RunID:             runID,
		JobStages:         sync.Map{},
	}
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
		StageRunParseEndTime,
		StageRunValidateStartTime,
		StageRunValidateEndTime,
		StageRunAftertreatmentStartTime,
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
	return r.calculateStageDuration(StageRunParseStartTime, StageRunParseEndTime)
}

func (r *RunStageTimeRecorder) calculateValidateDuration() (int64, error) {
	return r.calculateStageDuration(StageRunValidateStartTime, StageRunValidateEndTime)
}

func (r *RunStageTimeRecorder) calculateAftertreatmentDuration() (int64, error) {
	return r.calculateStageDuration(StageRunAftertreatmentStartTime, StageRunEndTime)
}
