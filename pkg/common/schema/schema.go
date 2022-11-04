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

import (
	"encoding/json"
	"fmt"
)

type ComponentView interface {
	GetComponentName() string
	GetParentDagID() string
	GetStatus() JobStatus
	GetSeq() int
	GetDeps() string
	GetMsg() string
	GetName() string
	GetStartTime() string
	GetEndTime() string

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

func (j JobView) GetStartTime() string {
	return j.StartTime
}

func (j JobView) GetEndTime() string {
	return j.EndTime
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

func (d DagView) GetStartTime() string {
	return d.StartTime
}

func (d DagView) GetEndTime() string {
	return d.EndTime
}

// RuntimeView is view of run responded to user, while workflowRuntime is for pipeline engine to process
type RuntimeView map[string][]ComponentView

func initRuntime(compMap map[string]interface{}) (map[string][]ComponentView, error) {
	resMap := map[string][]ComponentView{}
	// 遍历compMap中的每个compList
	for name, comps := range compMap {
		compList, ok := comps.([]interface{})
		if !ok {
			return nil, fmt.Errorf("init Runtime of response failed: value of comp is not list")
		}

		resList := []ComponentView{}
		for _, comp := range compList {
			// 遍历compList中的每个comp
			compMap, ok := comp.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("init Runtime of response failed: comp should be map type")
			}

			// 通过是否有entryPoints判断是否是Dag
			if entryPoints, ok := compMap["entryPoints"]; ok {
				// 如果是Dag
				subCompMap, ok := entryPoints.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("init Runtime of response failed: entryPoints in dag[%s] should be map type", name)
				}

				// 先Marshal再Unmarshal回davView
				// 由于entryPoints中的内容无法直接Unmarshal到davView中，这里先删除，后续单独初始化
				delete(compMap, "entryPoints")
				compByte, err := json.Marshal(compMap)
				if err != nil {
					return nil, err
				}
				dagView := DagView{}
				if err := json.Unmarshal(compByte, &dagView); err != nil {
					return nil, err
				}

				// 递归调用initRuntime来初始化entryPoints子节点
				subComps, err := initRuntime(subCompMap)
				if err != nil {
					return nil, err
				}

				dagView.EntryPoints = subComps
				resList = append(resList, &dagView)
			} else {
				// 如果不是Dag，而是Job
				compByte, err := json.Marshal(compMap)
				if err != nil {
					return nil, err
				}
				jobView := JobView{}
				if err := json.Unmarshal(compByte, &jobView); err != nil {
					return nil, err
				}
				resList = append(resList, &jobView)
			}
		}
		resMap[name] = resList
	}
	return resMap, nil
}

func (rv *RuntimeView) UnmarshalJSON(data []byte) error {
	compMap := map[string]interface{}{}
	if err := json.Unmarshal(data, &compMap); err != nil {
		return err
	}

	var err error
	*rv, err = initRuntime(compMap)
	if err != nil {
		return err
	}
	return nil
}

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
