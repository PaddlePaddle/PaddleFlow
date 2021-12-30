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

// JobView is view of job info responded to user, while Job is for pipeline and job engine to process
type JobView struct {
	JobID      string            `json:"jobID"`
	JobName    string            `json:"name"`
	Command    string            `json:"command"`
	Parameters map[string]string `json:"parameters"`
	Env        map[string]string `json:"env"`
	StartTime  string            `json:"startTime"`
	EndTime    string            `json:"endTime"`
	Status     JobStatus         `json:"status"`
	Deps       string            `json:"deps"`
	Image      string            `json:"image"`
	Artifacts  Artifacts         `json:"artifacts"`
	JobMessage string            `json:"jobMessage"`
}

// RuntimeView is view of run responded to user, while workflowRuntime is for pipeline engine to process
type RuntimeView map[string]JobView

type LogRunCacheRequest struct {
	FirstFp     string `json:"firstFp"`
	SecondFp    string `json:"secondFp"`
	Source      string `json:"source"`
	RunID       string `json:"runID"`
	Step        string `json:"step"`
	FsID        string `json:"fsID"`
	FsName      string `json:"fsname"`
	UserName    string `json:"username"`
	ExpiredTime string `json:"expiredTime"`
	Strategy    string `json:"strategy"`
}

type LogRunArtifactRequest struct {
	Md5          string `json:"md5"`
	RunID        string `json:"runID"`
	FsID         string `json:"fsID"`
	FsName       string `json:"fsname"`
	UserName     string `json:"username"`
	ArtifactPath string `json:"artifactPath"`
	Step         string `json:"step"`
	Type         string `json:"type"`
	ArtifactName string `json:"artifactName"`
	Meta         string `json:"meta"`
}
