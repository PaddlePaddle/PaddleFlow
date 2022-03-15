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

package errors

import "fmt"

const (
	CpuNotFound           = "CpuNotFound"
	MemoryNotFound        = "MemoryNotFound"
	QueueResourceNotMatch = "QueueResourceNotMatch"
	InvalidScaleResource  = "InvalidScaleResource" // 扩展资源类型不支持
)

type PFError struct {
	RequestID string `json:"requestID"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}

func (e *PFError) Error() string {
	return fmt.Sprintf("code %s, reason %s", e.Code, e.Message)
}

func CpuNotFoundError() error {
	return &PFError{
		Code:    CpuNotFound,
		Message: "cpu is not found",
	}
}

func MemoryNotFoundError() error {
	return &PFError{
		Code:    MemoryNotFound,
		Message: "memory is not found",
	}
}

func QueueResourceNotMatchError(cpu, mem string) error {
	return &PFError{
		Code:    QueueResourceNotMatch,
		Message: fmt.Sprintf("queue source[cpu:%s mem:%s] is invalid", cpu, mem),
	}
}

func InvalidScaleResourceError(resourceName string) error {
	return &PFError{
		Code:    InvalidScaleResource,
		Message: fmt.Sprintf("invalid scalar resource[%s]", resourceName),
	}
}

func EmptyUserNameError() error {
	return fmt.Errorf("empty user name")
}

func EmptyQueueNameError() error {
	return fmt.Errorf("empty queue name")
}

func EmptyClusterNameError() error {
	return fmt.Errorf("empty cluster name")
}

func EmptyFSIDError() error {
	return fmt.Errorf("empty fs id")
}

func EmptyJobTypeError() error {
	return fmt.Errorf("empty job type")
}

func InvalidJobTypeError(jobType string) error {
	return fmt.Errorf("invalid job type %s", jobType)
}

func EmptyFlavourError() error {
	return fmt.Errorf("empty flavour")
}

func InvalidFlavourError(flavour string) error {
	return fmt.Errorf("invalid flavour %s", flavour)
}

func EmptyJobModeError() error {
	return fmt.Errorf("empty job mode")
}

func InvalidJobModeError(mode string) error {
	return fmt.Errorf("invalid job mode %s", mode)
}

func EmptyJobNameError() error {
	return fmt.Errorf("empty job name")
}

func EmptyJobImageError() error {
	return fmt.Errorf("empty job image")
}

func EmptyJobCommandError() error {
	return fmt.Errorf("empty job command")
}

func EmptyReplicasError() error {
	return fmt.Errorf("empty job replicas")
}

func EmptyJobPortError() error {
	return fmt.Errorf("empty job port")
}

func EmptySparkMainFileError() error {
	return fmt.Errorf("empty spark main file path")
}

func InvalidJobPriorityError(priority string) error {
	return fmt.Errorf("invalid job priority %s, should be in [LOW, NORMAL, HIGH]", priority)
}

func JobFileNotFound(path string) error {
	return fmt.Errorf("get job file from path[%s] failed", path)
}

// UnSupportedOperate indicate action is not supported
func UnSupportedOperate(action string) error {
	return fmt.Errorf("unsupported operate to %s", action)
}

func JobIDNotFoundError(jobID string) error {
	return fmt.Errorf("jobID[%s] not found", jobID)
}
