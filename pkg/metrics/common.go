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
	"strings"
)

const (
	MetricJobTime    = "pf_metric_job_time"
	MetricQueueInfo  = "pf_metric_queue_info"
	MetricJobGPUInfo = "pf_metric_job_gpu_info"
)

func toHelp(name string) string {
	return strings.ReplaceAll(name, "_", " ")
}

const (
	JobIDLabel          = "jobID"
	GpuIdxLabel         = "id"
	StatusLabel         = "status"
	QueueIDLabel        = "queueID"
	FinishedStatusLabel = "finishedStatus"
	QueueNameLabel      = "queueName"
	UserNameLabel       = "userName"
	ResourceLabel       = "resource"
	TypeLabel           = "type"
	BaiduGpuIndexLabel  = "baidu_com_gpu_idx"
)
