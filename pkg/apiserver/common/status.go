/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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

package common

import "strings"

const (
	StatusRunInitiating  = "initiating"
	StatusRunPending     = "pending"
	StatusRunRunning     = "running"
	StatusRunSucceeded   = "succeeded"
	StatusRunFailed      = "failed"
	StatusRunTerminating = "terminating"
	StatusRunTerminated  = "terminated"
	StatusRunSkipped     = "skipped"

	StatusFinal  = "final"
	StatusActive = "active"

	WfEventKeyRunID         = "runID"
	WfEventKeyPK            = "pk"
	WfEventKeyStatus        = "status"
	WfEventKeyView          = "runtime"
	WfEventKeyComponentName = "componentName"
	WfEventKeyStartTime     = "startTime"
)

var (
	RunFinalStatus = []string{
		StatusRunFailed,
		StatusRunSucceeded,
		StatusRunTerminated,
		StatusRunSkipped,
	}

	RunActiveStatus = []string{
		StatusRunPending,
		StatusRunRunning,
		StatusRunTerminating,
		StatusRunInitiating,
	}
)

func IsRunFinalStatus(status string) bool {
	if strings.EqualFold(status, StatusRunFailed) ||
		strings.EqualFold(status, StatusRunSucceeded) ||
		strings.EqualFold(status, StatusRunTerminated) ||
		strings.EqualFold(status, StatusRunSkipped) {
		return true
	}
	return false
}
