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

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type TimePoint interface {
	Status() Status
	Index() int
}

type Status interface {
	String() string
	TimePoint() (start TimePoint, end TimePoint)
}

type Info map[string]string

type TimePointManager interface {
	AddTimestamp(key string, timePoint TimePoint, timestamp time.Time, extraInfos ...Info)
	GetStatusTime(key string, status Status) (time.Duration, bool)
	GetTimestamp(key string, timePoint TimePoint) (time.Time, bool)
	GetTimestampsCache() map[string]Timestamps
	GetStatusCount(status Status) int64
	GetInfo(key string) (Info, bool)
}

type Timestamps interface {
	GetStatusTime(status Status) (time.Duration, bool)
	GetTimestamp(timePoint TimePoint) (time.Time, bool)
	AddTimestamp(timePoint TimePoint, timestamp time.Time)
}

type ListQueueFunc func() []model.Queue
type ListJobFunc func() []model.Job
