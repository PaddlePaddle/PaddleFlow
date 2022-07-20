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

type ComponentView interface {
	GetComponentName() string
	GetParentDagID() string
	GetStatus() JobStatus
	GetSeq() int
	GetDeps() string
	GetMsg() string
	GetName() string

	SetDeps(string)
}

// JobView is view of job info responded to user, while Job is for pipeline and job engine to process
type JobView struct {
	PK          int64             `json:"-"`
	JobID       string            `json:"jobID"`
	Name        string            `json:"name"`
	Type        string            `json:"type"`
	StepName    string            `json:"stepName"`
	ParentDagID string            `json:"parentDagID"`
	LoopSeq     int               `json:"-"`
	Command     string            `json:"command"`
	Parameters  map[string]string `json:"parameters"`
	Env         map[string]string `json:"env"`
	ExtraFS     []FsMount         `json:"extraFS"`
	StartTime   string            `json:"startTime"`
	EndTime     string            `json:"endTime"`
	Status      JobStatus         `json:"status"`
	Deps        string            `json:"deps"`
	DockerEnv   string            `json:"dockerEnv"`
	Artifacts   Artifacts         `json:"artifacts"`
	Cache       Cache             `json:"cache"`
	JobMessage  string            `json:"jobMessage"`
	CacheRunID  string            `json:"cacheRunID"`
	CacheJobID  string            `json:"cacheJobID"`
}

func (j JobView) GetComponentName() string {
	return j.StepName
}

func (j JobView) GetParentDagID() string {
	return j.ParentDagID
}

func (j *JobView) SetDeps(deps string) {
	j.Deps = deps
}

func (j JobView) GetDeps() string {
	return j.Deps
}

func (j JobView) GetStatus() JobStatus {
	return j.Status
}

func (j JobView) GetSeq() int {
	return j.LoopSeq
}

func (j JobView) GetMsg() string {
	return j.JobMessage
}

func (j JobView) GetName() string {
	return j.Name
}

type DagView struct {
	PK          int64                      `json:"-"`
	DagID       string                     `json:"id"`
	Name        string                     `json:"name"`
	Type        string                     `json:"type"`
	DagName     string                     `json:"dagName"`
	ParentDagID string                     `json:"parentDagID"`
	LoopSeq     int                        `json:"-"`
	Deps        string                     `json:"deps"`
	Parameters  map[string]string          `json:"parameters"`
	Artifacts   Artifacts                  `json:"artifacts"`
	StartTime   string                     `json:"startTime"`
	EndTime     string                     `json:"endTime"`
	Status      JobStatus                  `json:"status"`
	Message     string                     `json:"message"`
	EntryPoints map[string][]ComponentView `json:"entryPoints"`
}

func (d DagView) GetComponentName() string {
	return d.DagName
}

func (d DagView) GetParentDagID() string {
	return d.ParentDagID
}

func (d *DagView) SetDeps(deps string) {
	d.Deps = deps
}

func (d DagView) GetDeps() string {
	return d.Deps
}

func (d DagView) GetStatus() JobStatus {
	return d.Status
}

func (d DagView) GetSeq() int {
	return d.LoopSeq
}

func (d DagView) GetMsg() string {
	return d.Message
}

func (d DagView) GetName() string {
	return d.Name
}

// RuntimeView is view of run responded to user, while workflowRuntime is for pipeline engine to process
type RuntimeView map[string][]ComponentView

type PostProcessView map[string]*JobView

type LogRunCacheRequest struct {
	FirstFp     string `json:"firstFp"`
	SecondFp    string `json:"secondFp"`
	Source      string `json:"source"`
	RunID       string `json:"runID"`
	Step        string `json:"step"`
	JobID       string `json:"jobID"`
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
	JobID        string `json:"jobID"`
	Type         string `json:"type"`
	ArtifactName string `json:"artifactName"`
	Meta         string `json:"meta"`
}
