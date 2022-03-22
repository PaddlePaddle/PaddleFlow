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
	"reflect"
	"strings"

	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
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
	Image      string                 `yaml:"image"`
	Cache	   Cache				  `yaml:"cache"`
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
	Disabled 	string                         `yaml:"disabled"`
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
	_, ok := wfs.EntryPoints[step]
	if ok {
		return true
	} else {
		return false
	}
}

// ValidateArtifacts - 该函数的作用是将WorkflowSource中的Slice类型的输出Artifact改为Map类型
//                     这样做的原因是：之前的run.yaml中（ver.1.3.2之前），输出Artifact为Map类型，而现在为了支持Cache的优化，改为Slice类型
//
// PARAMS:
//   无
// RETURNS:
//   nil: 正常执行则返回nil
//   error: 执行异常则返回错误信息
func (wfs *WorkflowSource) validateArtifacts() error {
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

func (wfs *WorkflowSource) validateStepCacheByMap(yamlMap map[string]interface{}) error {
	for name, point := range wfs.EntryPoints {
		// 先将全局的Cache设置赋值给该节点的Cache，下面再根据Map进行替换
		point.Cache = wfs.Cache

		// 检查用户是否有设置节点级别的Cache
		cache, ok, err := unstructured.NestedFieldCopy(yamlMap, "entry_points", name, "cache")
		if err != nil {
			return err
		}
		if ok {
			cacheMap := cache.(map[string]interface{})
			// 给Cache的每个字段赋值，覆盖掉全局的Cache设置
			for i := 0; i < reflect.TypeOf(point.Cache).NumField(); i++ {
				attrName := reflect.TypeOf(point.Cache).Field(i).Tag.Get("yaml")
				if attrValue, ok := cacheMap[attrName]; ok {
					reflect.ValueOf(&point.Cache).Elem().Field(i).Set(reflect.ValueOf(attrValue))
				}
			}
		}
	}
	return nil
}

func ParseWorkflowSource(runYaml []byte) (WorkflowSource, error) {
	var wfs WorkflowSource
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
	if err := wfs.validateStepCacheByMap(yamlMap); err != nil {
		return WorkflowSource{}, err
	}
	return wfs, nil
}
