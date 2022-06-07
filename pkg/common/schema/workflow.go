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

	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	ArtifactTypeInput  = "input"
	ArtifactTypeOutput = "output"

	EntryPointsStr = "entry_points"

	CacheAttributeEnable         = "enable"
	CacheAttributeMaxExpiredTime = "max_expired_time"
	CacheAttributeFsScope        = "fs_scope"

	FailureStrategyFailFast = "fail_fast"
	FailureStrategyContinue = "continue"

	EnvDockerEnv = "dockerEnv"
)

type Artifacts struct {
	Input      map[string]string `yaml:"input"       json:"input"`
	Output     map[string]string `yaml:"-"           json:"output"`
	OutputList []string          `yaml:"output"      json:"-"`
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

type Component interface {
	GetDeps() []string
	GetArtifacts() Artifacts
	GetParameters() map[string]interface{}
	GetCondition() string
	GetLoopArgument() interface{}

	// 下面几个Update 函数在进行模板替换的时候会用到
	UpdateCondition(string)
	UpdateLoopArguemt(interface{})
}

type WorkflowSourceStep struct {
	LoopArgument interface{}            `yaml:"loop_argument"`
	Condition    string                 `yaml:"condition"`
	Parameters   map[string]interface{} `yaml:"parameters"`
	Command      string                 `yaml:"command"`
	Deps         string                 `yaml:"deps"`
	Artifacts    Artifacts              `yaml:"artifacts"`
	Env          map[string]string      `yaml:"env"`
	DockerEnv    string                 `yaml:"docker_env"`
	Cache        Cache                  `yaml:"cache"`
	Reference    string                 `yaml:"referenc"`
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

func (s *WorkflowSourceStep) GetArtifacts() Artifacts {
	return s.Artifacts
}

func (s *WorkflowSourceStep) GetParameters() map[string]interface{} {
	return s.Parameters
}

func (s *WorkflowSourceStep) GetCondition() string {
	return s.Condition
}

func (s *WorkflowSourceStep) GetLoopArgument() interface{} {
	return s.LoopArgument
}

func (s *WorkflowSourceStep) UpdateCondition(condition string) {
	s.Condition = condition
}

func (s *WorkflowSourceStep) UpdateLoopArguemt(loopArgument interface{}) {
	s.LoopArgument = loopArgument
}

type WorkflowSourceDag struct {
	LoopArgument interface{}            `yaml:"loop_argument"`
	Condition    string                 `yaml:"condition"`
	Parameters   map[string]interface{} `yaml:"parameters"`
	Deps         string                 `yaml:"deps"`
	Artifacts    Artifacts              `yaml:"artifacts"`
	EntryPoints  map[string]Component   `yaml:"entry_points"`
}

func (d *WorkflowSourceDag) GetDeps() []string {
	// 获取依赖节点列表。添加前删除每个步骤名称前后的空格，只有空格的步骤名直接略过不添加
	deps := make([]string, 0)
	for _, dep := range strings.Split(d.Deps, ",") {
		dryDep := strings.TrimSpace(dep)
		if len(dryDep) <= 0 {
			continue
		}
		deps = append(deps, dryDep)
	}
	return deps
}

func (d *WorkflowSourceDag) GetArtifacts() Artifacts {
	return d.Artifacts
}

func (d *WorkflowSourceDag) GetParameters() map[string]interface{} {
	return d.Parameters
}

func (d *WorkflowSourceDag) GetCondition() string {
	return d.Condition
}

func (d *WorkflowSourceDag) GetLoopArgument() interface{} {
	return d.LoopArgument
}

func (d *WorkflowSourceDag) UpdateCondition(condition string) {
	d.Condition = condition
}

func (d *WorkflowSourceDag) UpdateLoopArguemt(loopArgument interface{}) {
	d.LoopArgument = loopArgument
}

func (d *WorkflowSourceDag) GetSubComponet(subComponentName string) (Component, bool) {
	sc, ok := d.EntryPoints[subComponentName]
	return sc, ok
}

type Cache struct {
	Enable         bool   `yaml:"enable"           json:"enable"`
	MaxExpiredTime string `yaml:"max_expired_time" json:"maxExpiredTime"` // seconds
	FsScope        string `yaml:"fs_scope"         json:"fsScope"`        // seperated by ","
}

type FailureOptions struct {
	Strategy string `yaml:"strategy"`
}

type WorkflowSource struct {
	Name           string                         `yaml:"name"`
	DockerEnv      string                         `yaml:"docker_env"`
	EntryPoints    WorkflowSourceDag              `yaml:"entry_points"`
	Components     map[string]Component           `yaml:"components"`
	Cache          Cache                          `yaml:"cache"`
	Parallelism    int                            `yaml:"parallelism"`
	Disabled       string                         `yaml:"disabled"`
	FailureOptions FailureOptions                 `yaml:"failure_options"`
	PostProcess    map[string]*WorkflowSourceStep `yaml:"post_process"`
}

func (wfs *WorkflowSource) GetDisabled() []string {
	// 获取disabled节点列表。每个节点名称前后的空格会被删除，只有空格的步骤名直接略过不添加
	disabledSteps := make([]string, 0)
	for _, step := range strings.Split(wfs.Disabled, ",") {
		step := strings.TrimSpace(step)
		if len(step) <= 0 {
			continue
		}
		disabledSteps = append(disabledSteps, step)
	}
	return disabledSteps
}

func (wfs *WorkflowSource) IsDisabled(stepName string) (bool, error) {
	// 表示该节点是否disabled
	disabledSteps := wfs.GetDisabled()

	if !wfs.HasStep(stepName) {
		return false, fmt.Errorf("check disabled for step[%s] failed, step not existed!", stepName)
	}

	for _, disableStepName := range disabledSteps {
		if stepName == disableStepName {
			return true, nil
		}
	}
	return false, nil
}

func (wfs *WorkflowSource) HasStep(step string) bool {
	return true
}

// 该函数的作用是将WorkflowSource中的Slice类型的输出Artifact改为Map类型。
// 这样做的原因是：之前的run.yaml中（ver.1.3.2之前），输出Artifact为Map类型，而现在为了支持Cache的优化，改为Slice类型
func (wfs *WorkflowSource) validateArtifacts() error {
	return nil
}

func parseArtifactsOfSteps(steps map[string]*WorkflowSourceStep) (map[string]*WorkflowSourceStep, error) {
	res := map[string]*WorkflowSourceStep{}
	for stepName, step := range steps {
		if err := step.Artifacts.ValidateOutputMapByList(); err != nil {
			return map[string]*WorkflowSourceStep{}, fmt.Errorf("validate artifacts failed")
		}
		res[stepName] = step
	}
	return res, nil
}

// 将yaml解析为map
func runYaml2Map(runYaml []byte) (map[string]interface{}, error) {
	// Unstructured没有解析Yaml的方法，且无法使用官方yaml库Unmarshal后的结果，因此需要先转成Json
	jsonByte, err := k8syaml.ToJSON(runYaml)
	if err != nil {
		return nil, err
	}
	// 将Json转化成Map
	yamlUnstructured := unstructured.Unstructured{}
	if err := yamlUnstructured.UnmarshalJSON(jsonByte); err != nil && !runtime.IsMissingKind(err) {
		// MissingKindErr不影响Json的解析
		return nil, err
	}

	yamlMap := yamlUnstructured.UnstructuredContent()
	return yamlMap, nil
}

func (wfs *WorkflowSource) ValidateStepCacheByMap(runMap map[string]interface{}) error {
	return nil
}

func ParseWorkflowSource(runYaml []byte) (WorkflowSource, error) {
	wfs := WorkflowSource{
		FailureOptions: FailureOptions{Strategy: FailureStrategyFailFast},
	}
	if err := yaml.Unmarshal(runYaml, &wfs); err != nil {
		return WorkflowSource{}, err
	}
	// 将List格式的OutputArtifact，转换为Map格式
	if err := wfs.validateArtifacts(); err != nil {
		return WorkflowSource{}, err
	}

	// 为了判断用户是否设定节点级别的Cache，需要第二次Unmarshal
	yamlMap, err := runYaml2Map(runYaml)
	if err != nil {
		return WorkflowSource{}, err
	}

	// 检查节点级别的Cache设置，根据需要用Run级别的Cache进行覆盖
	if err := wfs.ValidateStepCacheByMap(yamlMap); err != nil {
		return WorkflowSource{}, err
	}
	return wfs, nil
}
