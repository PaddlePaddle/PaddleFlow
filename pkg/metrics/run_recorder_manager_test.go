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
	"testing"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/stretchr/testify/assert"
)

func TestAddRunStageTimeRecord(t *testing.T) {
	rm := NewRunRecorderManager()
	rm.AddRunStageTimeRecord("run-01", "req-12", "init", StageRunStartTime, time.Now())
	rm.AddRunStageTimeRecord("run-01", "req-12", "init", StageRunStartTime, time.Now())
	rm.AddRunStageTimeRecord("run-01", "req-12", "success", StageRunEndTime, time.Now())
	rm.AddRunStageTimeRecord("run-01", "req-12", "success", StageJobAftertreatmentEndTime, time.Now())

	assert.Equal(t, 1, len(rm.Cache.GetALL(false)))
}

func TestAddJobStageTimeRecord(t *testing.T) {
	rm := NewRunRecorderManager()
	rm.AddStepStageTimeRecord("run-01", "step1", StageStepScheduleStartTime, time.Now())
	rm.AddJobStageTimeRecord("run-01", "step1", "job-01", schema.StatusJobPending, StageJobAftertreatmentEndTime, time.Now())

	rm.AddRunStageTimeRecord("run-01", "req-12", "init", StageRunStartTime, time.Now())
	rm.AddJobStageTimeRecord("run-01", "step1", "job-01", schema.StatusJobPending, StageJobAftertreatmentEndTime, time.Now())

	rm.AddStepStageTimeRecord("run-01", "step1", StageStepScheduleStartTime, time.Now())
	rm.AddJobStageTimeRecord("run-01", "step1", "job-01", schema.StatusJobPending, StageJobAftertreatmentEndTime, time.Now())
	rm.AddJobStageTimeRecord("run-01", "step1", "job-01", schema.StatusJobPending, StageJobAftertreatmentStartTime, time.Now())

	rm.AddJobStageTimeRecord("run-01", "step1", "job-01", schema.StatusJobPending, StageJobAftertreatmentEndTime, time.Now())
}
