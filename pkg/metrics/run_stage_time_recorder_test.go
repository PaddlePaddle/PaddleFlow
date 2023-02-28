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

	"github.com/stretchr/testify/assert"
)

func TestIsSupported(t *testing.T) {
	s := []stageTimeType{
		StageRunEndTime,
		StageRunStartTime,
	}

	r := NewStageTimeRecorder(s, "test_stage")

	assert.True(t, r.isSupported(StageRunEndTime))
	assert.False(t, r.isSupported(StageRunValidateEndTime))
}

func TestSetStageTime(t *testing.T) {
	s := []stageTimeType{
		StageRunEndTime,
		StageRunStartTime,
	}

	r := NewStageTimeRecorder(s, "test_stage")

	err := r.setStageTime(StageRunValidateEndTime, time.Now())
	assert.Contains(t, err.Error(), "is not supported")

	err = r.setStageTime(StageRunEndTime, time.Now())
	assert.Nil(t, err)

	_, ok := r.StageTime.Load(StageRunEndTime)
	assert.True(t, ok)
}

func TestCalculateStageDuration(t *testing.T) {
	s := []stageTimeType{
		StageRunEndTime,
		StageRunStartTime,
	}

	r := NewStageTimeRecorder(s, "test_stage")
	r.setStageTime(StageRunEndTime, time.Now())
	_, err := r.calculateStageDuration(StageRunStartTime, StageRunEndTime)
	assert.Contains(t, err.Error(), "not been registered")

	_, err = r.calculateStageDuration(StageRunEndTime, StageRunStartTime)
	assert.Contains(t, err.Error(), "not been registered")

	r.setStageTime(StageRunStartTime, time.Now())
	_, err = r.calculateStageDuration(StageRunStartTime, StageRunEndTime)
	assert.Nil(t, err)
}
