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

/**
 * @Author: kiritoxkiriko
 * @Date: 2022/7/26
 * @Description:
 */

package job_perf

type JobTimePoint int

const (
	T1 JobTimePoint = iota
	T2
	T3
	T4
	T5
	T6
	T7
)

type JobStatus int

const (
	DBUpdating JobStatus = iota
	InQueue
	DeQueue
	Pending
	Creating
	Running
)
