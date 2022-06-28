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
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
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

type WorkflowSourceStep struct {
	Parameters map[string]interface{} `yaml:"parameters"    json:"parameters"`
	Command    string                 `yaml:"command"       json:"command"`
	Deps       string                 `yaml:"deps"          json:"deps"`
	Artifacts  Artifacts              `yaml:"artifacts"     json:"artifacts"`
	Env        map[string]string      `yaml:"env"           json:"env"`
	DockerEnv  string                 `yaml:"docker_env"    json:"dockerEnv"`
	Cache      Cache                  `yaml:"cache"         json:"cache"`
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
	Enable         bool   `yaml:"enable"           json:"enable"`
	MaxExpiredTime string `yaml:"max_expired_time" json:"maxExpiredTime"` // seconds
	FsScope        string `yaml:"fs_scope"         json:"fsScope"`        // seperated by ","
}

type FailureOptions struct {
	Strategy string `yaml:"strategy"     json:"strategy"`
}

type WorkflowSource struct {
	Name           string                         `yaml:"name"            json:"name"`
	DockerEnv      string                         `yaml:"docker_env"      json:"dockerEnv"`
	EntryPoints    map[string]*WorkflowSourceStep `yaml:"entry_points"    json:"entryPoints"`
	Cache          Cache                          `yaml:"cache"           json:"cache"`
	Parallelism    int                            `yaml:"parallelism"     json:"parallelism"`
	Disabled       string                         `yaml:"disabled"        json:"disabled"`
	FailureOptions FailureOptions                 `yaml:"failure_options" json:"failureOptions"`
	PostProcess    map[string]*WorkflowSourceStep `yaml:"post_process"    json:"postProcess"`
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
	_, ok1 := wfs.EntryPoints[step]
	_, ok2 := wfs.PostProcess[step]
	ok := ok1 || ok2
	if ok {
		return true
	} else {
		return false
	}
}

// 该函数的作用是将WorkflowSource中的Slice类型的输出Artifact改为Map类型。
// 这样做的原因是：之前的run.yaml中（ver.1.3.2之前），输出Artifact为Map类型，而现在为了支持Cache的优化，改为Slice类型
func (wfs *WorkflowSource) validateArtifacts() error {
	steps, err := parseArtifactsOfSteps(wfs.EntryPoints)
	if err != nil {
		return err
	}
	wfs.EntryPoints = steps

	steps, err = parseArtifactsOfSteps(wfs.PostProcess)
	if err != nil {
		return err
	}
	wfs.PostProcess = steps
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
	for name, point := range wfs.EntryPoints {
		// 先将全局的Cache设置赋值给该节点的Cache，下面再根据Map进行替换
		point.Cache = wfs.Cache

		// 检查用户是否有设置节点级别的Cache
		cache, ok, err := unstructured.NestedFieldCopy(runMap, EntryPointsStr, name, "cache")
		if err != nil {
			return err
		}
		if ok {
			cacheMap := cache.(map[string]interface{})
			// Enable字段赋值
			if value, ok := cacheMap[CacheAttributeEnable]; ok {
				switch value := value.(type) {
				case bool:
					point.Cache.Enable = value
				default:
					return fmt.Errorf("cannot assign cache attribute [%s] by value[%v] with type [%s]",
						CacheAttributeEnable, value, reflect.TypeOf(value).Name())
				}
			}
			// MaxExpiredTime字段赋值
			if value, ok := cacheMap[CacheAttributeMaxExpiredTime]; ok {
				switch value := value.(type) {
				case int64:
					point.Cache.MaxExpiredTime = strconv.FormatInt(value, 10)
				case string:
					point.Cache.MaxExpiredTime = value
				default:
					return fmt.Errorf("cannot assign cache attribute [%s] by value[%v] with type [%s]",
						CacheAttributeMaxExpiredTime, value, reflect.TypeOf(value).Name())
				}
			}
			// FsScope字段赋值
			if value, ok := cacheMap[CacheAttributeFsScope]; ok {
				switch value := value.(type) {
				case string:
					point.Cache.FsScope = value
				default:
					return fmt.Errorf("cannot assign cache attribute [%s] by value[%v] with type [%s]",
						CacheAttributeFsScope, value, reflect.TypeOf(value).Name())
				}
			}
		}
	}
	return nil
}

func ParseWorkflowSource(runYaml []byte) (WorkflowSource, error) {
	wfs := WorkflowSource{
		FailureOptions: FailureOptions{Strategy: FailureStrategyFailFast},
	}
	if err := yaml.Unmarshal(runYaml, &wfs); err != nil {
		return WorkflowSource{}, err
	}

	// 为了判断用户是否设定节点级别的Cache，需要第二次Unmarshal
	yamlMap, err := runYaml2Map(runYaml)
	if err != nil {
		return WorkflowSource{}, err
	}

	// 将List格式的OutputArtifact，转换为Map格式
	if err := wfs.validateArtifacts(); err != nil {
		return WorkflowSource{}, err
	}

	// 检查节点级别的Cache设置，根据需要用Run级别的Cache进行覆盖
	if err := wfs.ValidateStepCacheByMap(yamlMap); err != nil {
		return WorkflowSource{}, err
	}
	return wfs, nil
}

func (wfs *WorkflowSource) TransToRunYamlRaw() (runYamlRaw string, err error) {
	runYaml, err := yaml.Marshal(wfs)
	if err != nil {
		return "", err
	}

	defer func() {
		if info := recover(); info != nil {
			err = fmt.Errorf("trans Pipeline to YamlRaw failed: %v", info)
		}
	}()

	runYamlRaw = base64.StdEncoding.EncodeToString(runYaml)
	return
}
