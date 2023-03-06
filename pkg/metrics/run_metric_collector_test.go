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
	"github.com/prometheus/client_golang/prometheus"
)

func TestCollect(t *testing.T) {
	RunMetricManger = NewRunRecorderManager()
	c := NewMetricRunCollector()

	RunMetricManger.AddRunStageTimeRecord("run-01", "req-01", "init", StageRunStartTime, time.Now())
	RunMetricManger.AddRunStageTimeRecord("run-01", "req-01", "init", StageRunParseStartTime, time.Now())
	RunMetricManger.AddRunStageTimeRecord("run-01", "req-01", "init", StageRunParseEndTime, time.Now())
	RunMetricManger.AddRunStageTimeRecord("run-01", "req-01", "init", StageRunValidateStartTime, time.Now())
	RunMetricManger.AddRunStageTimeRecord("run-01", "req-01", "init", StageRunValidateEndTime, time.Now())
	RunMetricManger.AddRunStageTimeRecord("run-01", "req-01", "init", StageRunAftertreatmentStartTime, time.Now())
	RunMetricManger.AddRunStageTimeRecord("run-01", "req-01", "init", StageRunEndTime, time.Now())

	RunMetricManger.AddStepStageTimeRecord("run-01", "step1", StageStepScheduleStartTime, time.Now())
	RunMetricManger.AddJobStageTimeRecord("run-01", "step1", "job-01", schema.StatusJobPending, StageJobScheduleEndTime, time.Now())
	RunMetricManger.AddJobStageTimeRecord("run-01", "step1", "job-01", schema.StatusJobPending, StageJobCreateEndTime, time.Now())
	RunMetricManger.AddJobStageTimeRecord("run-01", "step1", "job-01", schema.StatusJobPending, StageJobAftertreatmentStartTime, time.Now())
	RunMetricManger.AddJobStageTimeRecord("run-01", "step1", "job-01", schema.StatusJobPending, StageJobAftertreatmentEndTime, time.Now())

	ch := make(chan prometheus.Metric, 1)
	go c.Collect(ch)

	time.Sleep(time.Second * 2)
}
