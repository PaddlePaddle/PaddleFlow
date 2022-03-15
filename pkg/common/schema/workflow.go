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
	"fmt"
	"strings"
)

const (
	ArtifactTypeInput  = "input"
	ArtifactTypeOutput = "output"
)

type Artifacts struct {
	Input      map[string]string `yaml:"input"`
	Output     map[string]string `yaml:"-"`
	OutputList []string          `yaml:"output"`
}

func (atf *Artifacts) ValidateOutputMapByList() error {
	if atf.Output == nil {
		atf.Output = make(map[string]string)
	}
	for _, outputName := range atf.OutputList {
		atf.Output[outputName] = ""
	}
	return nil
}

type WorkflowSourceStep struct {
	Parameters map[string]interface{} `yaml:"parameters"`
	Command    string                 `yaml:"command"`
	Deps       string                 `yaml:"deps"`
	Artifacts  Artifacts              `yaml:"artifacts"`
	Env        map[string]string      `yaml:"env"`
	Image      string                 `yaml:"image"` // 这个字段暂时不对用户暴露
}

func (s *WorkflowSourceStep) GetDeps() []string {
    // 获取依赖节点列表。添加前删除每个步骤名称前后的空格，只有空格的步骤名直接略过不添加
	deps := make([]string, 0)
	for _, dep := range strings.Split(s.Deps, ",") {
		dryDep := strings.TrimSpace(dep)
		if len(dryDep) <= 0 {
			continue
		}
		deps = append(deps, dryDep)
	}
	return deps
}

type Cache struct {
	Enable         bool   `yaml:"enable"`
	MaxExpiredTime string `yaml:"max_expired_time"` // seconds
	FsScope        string `yaml:"fs_scope"`         // seperated by ","
}

type WorkflowSource struct {
	Name        string                         `yaml:"name"`
	DockerEnv   string                         `yaml:"docker_env"`
	EntryPoints map[string]*WorkflowSourceStep `yaml:"entry_points"`
	Cache       Cache                          `yaml:"cache"`
	Parallelism int                            `yaml:"parallelism"`
}

// ValidateArtifacts - 该函数的作用是将WorkflowSource中的Slice类型的输出Artifact改为Map类型
//                     这样做的原因是：之前的run.yaml中（ver.1.3.2之前），输出Artifact为Map类型，而现在为了支持Cache的优化，改为Slice类型
//
// PARAMS:
//   无
// RETURNS:
//   nil: 正常执行则返回nil
//   error: 执行异常则返回错误信息
func (wfs *WorkflowSource) ValidateArtifacts() error {
	if wfs.EntryPoints == nil {
		wfs.EntryPoints = make(map[string]*WorkflowSourceStep)
	}

	for stepName, step := range wfs.EntryPoints {
		if err := step.Artifacts.ValidateOutputMapByList(); err != nil {
			return fmt.Errorf("validate artifacts failed")
		}
		wfs.EntryPoints[stepName] = step
	}
	return nil
}
