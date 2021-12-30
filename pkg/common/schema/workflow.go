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
	"strings"
)

const (
	ArtifactTypeInput  = "input"
	ArtifactTypeOutput = "output"
)

type WorkflowSourceStep struct {
	Parameters map[string]interface{} `yaml:"parameters"`
	Command    string                 `yaml:"command"`
	Deps       string                 `yaml:"deps"`
	Artifacts  Artifacts              `yaml:"artifacts"`
	Env        map[string]string      `yaml:"env"`
	Image      string                 `yaml:"image"` // 这个字段暂时不对用户暴露
}

type Artifacts struct {
	Input  map[string]string `yaml:"input"`
	Output map[string]string `yaml:"output"`
}

func (s *WorkflowSourceStep) GetDeps() []string {
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
