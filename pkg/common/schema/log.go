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

package schema

type LogInfo struct {
	LogContent  string `json:"logContent"`
	HasNextPage bool   `json:"hasNextPage"`
	Truncated   bool   `json:"truncated"`
}

type JobLogInfo struct {
	JobID    string        `json:"jobID"`
	TaskList []TaskLogInfo `json:"taskList"`
}

type TaskLogInfo struct {
	TaskID string  `json:"taskID"`
	Info   LogInfo `json:"logInfo"`
}

type JobLogRequest struct {
	JobID           string `json:"jobID"`
	JobType         string `json:"jobType"`
	Namespace       string `json:"namespace"`
	LogFilePosition string `json:"logFilePosition"`
	LogPageSize     int    `json:"logPageSize"`
	LogPageNo       int    `json:"logPageNo"`
}

// MixedLogRequest can request job log or k8s pod/deploy events and log
type MixedLogRequest struct {
	Name         string
	Namespace    string
	ResourceType string
	Framework    string

	LineLimit      string
	SizeLimit      int64
	IsReadFromTail bool
	//ClusterInfo    model.ClusterInfo
}

type MixedLogResponse struct {
	ResourceName string        `json:"name"`
	Resourcetype string        `json:"type"`
	TaskList     []TaskLogInfo `json:"taskList"`
	Events       []string      `json:"eventList"`
}
