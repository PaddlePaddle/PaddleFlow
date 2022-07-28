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

package monitor

const (
	QueryCPUUsageRateQl = "sum(rate(container_cpu_usage_seconds_total{image!=\"\", pod=~\"%s\"}[1m])) by (pod) / sum(container_spec_cpu_quota{image!=\"\", pod=~\"%s\"} / 100000) by (pod)"
	QueryMEMUsageRateQl = "sum(container_memory_working_set_bytes{image!=\"\", pod=~\"%s\"}) by (pod) / sum(container_spec_memory_limit_bytes{image!=\"\", pod=~\"%s\"}) by (pod)"
	QueryMEMUsageQl     = "sum(container_memory_working_set_bytes{image!=\"\", pod=~\"%s\"}) by (pod)"
	QueryNetReceiveQl   = "sum(rate(container_network_receive_bytes_total{image!=\"\", pod=~\"%s\"}[1m])) by (pod)"
	QueryNetTransmitQl  = "sum(rate(container_network_transmit_bytes_total{image!=\"\", pod=~\"%s\"}[1m])) by (pod)"
	QueryDiskUsageQl    = "sum(container_fs_usage_bytes{image!=\"\", pod=~\"%s\"}) by (pod)"
	QueryDiskReadQl     = "sum(rate(container_fs_reads_bytes_total{image!=\"\", pod=~\"%s\"}[1m])) by (pod)"
	QueryDiskWriteQl    = "sum(rate(container_fs_writes_bytes_total{image!=\"\", pod=~\"%s\"}[1m])) by (pod)"
	QueryGpuUtilQl      = "sum(rate(container_accelerator_duty_cycle{image!=\"\", pod=~\"%s\"}[1m])) by (pod)"
	QueryGpuMemUtilQl   = "sum(container_accelerator_memory_used_bytes{image!=\"\", pod=~\"%s\"}) by (pod) / sum(container_accelerator_memory_total_bytes{image!=\"\", pod=~\"%s\"}) by (pod)"
	QueryGpuMemUsageQl  = "sum(container_accelerator_memory_used_bytes{image!=\"\", pod=~\"%s\"}) by (pod)"
)
