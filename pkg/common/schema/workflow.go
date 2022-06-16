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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
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
	GetType() string

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

func (s *WorkflowSourceStep) GetType() string {
	return "step"
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

func (d *WorkflowSourceDag) GetType() string {
	return "dag"
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

func (wfs *WorkflowSource) IsDisabled(componentName string) (bool, error) {
	// 表示该节点是否disabled
	disabledComponents := wfs.GetDisabled()
	postComponents := map[string]Component{}
	for k, v := range wfs.PostProcess {
		postComponents[k] = v
	}
	if !wfs.HasStep(wfs.EntryPoints.EntryPoints, componentName) && !wfs.HasStep(postComponents, componentName) {
		return false, fmt.Errorf("check disabled for component[%s] failed, component not existed!", componentName)
	}

	for _, disableStepName := range disabledComponents {
		if componentName == disableStepName {
			return true, nil
		}
	}
	return false, nil
}

// 递归的检查absoluteName对应的Component是否存在
func (wfs *WorkflowSource) HasStep(components map[string]Component, absoluteName string) bool {
	nameList := strings.SplitN(absoluteName, ".", 2)
	if len(nameList) > 1 {
		if component, ok := components[nameList[0]]; ok {
			if dag, ok := component.(*WorkflowSourceDag); ok {
				return wfs.HasStep(dag.EntryPoints, nameList[1])
			} else if step, ok := component.(*WorkflowSourceStep); ok {
				// 如果为step，检查是否有引用Source.Components中的节点
				reference := step.Reference
				for {
					if referedComponent, ok := wfs.Components[reference]; ok {
						if dag, ok := referedComponent.(*WorkflowSourceDag); ok {
							// 检查Source.Components中的节点，如果它是一个dag，那就继续向下遍历子节点
							return wfs.HasStep(dag.EntryPoints, nameList[1])
						} else if step, ok := referedComponent.(*WorkflowSourceStep); ok {
							// 如果是step，那就看是否继续ref了其他component
							reference = step.Reference
							continue
						} else {
							logger.Logger().Errorf("a component not dag or step")
							return false
						}
					} else {
						return false
					}
				}
			} else {
				logger.Logger().Errorf("a component not dag or step")
				return false
			}
		} else {
			return false
		}
	} else {
		_, ok := components[absoluteName]
		return ok
	}
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
func RunYaml2Map(runYaml []byte) (map[string]interface{}, error) {
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

// 该函数除了将yaml解析为wfs，还进行了全局参数替换操作
func GetWorkflowSource(runYaml []byte) (WorkflowSource, error) {
	wfs := WorkflowSource{
		FailureOptions: FailureOptions{Strategy: FailureStrategyFailFast},
	}
	p := Parser{}
	yamlMap, err := RunYaml2Map(runYaml)
	if err != nil {
		logger.Logger().Errorf(err.Error())
		return WorkflowSource{}, err
	}

	if err := p.ParseWorkflowSource(yamlMap, &wfs); err != nil {
		logger.Logger().Errorf(err.Error())
		return WorkflowSource{}, err
	}

	// 全局参数替换
	entryPoints, ok := yamlMap["entry_points"]
	if !ok {
		return WorkflowSource{}, fmt.Errorf("get entry_points failed")
	}
	entryPointsMap, ok := entryPoints.(map[string]interface{})
	if !ok {
		return WorkflowSource{}, fmt.Errorf("get entry_points map failed")
	}
	if err := wfs.ValidateRunNodes(wfs.EntryPoints.EntryPoints, yamlMap, entryPointsMap); err != nil {
		return WorkflowSource{}, err
	}

	postComponentsMap := map[string]Component{}
	for k, v := range wfs.PostProcess {
		postComponentsMap[k] = v
	}
	postProcess, ok := yamlMap["post_process"]
	if !ok {
		return WorkflowSource{}, fmt.Errorf("get post_process failed")
	}
	postProcessMap, ok := postProcess.(map[string]interface{})
	if !ok {
		return WorkflowSource{}, fmt.Errorf("get entry_points map failed")
	}
	if err := wfs.ValidateRunNodes(postComponentsMap, yamlMap, postProcessMap); err != nil {
		return WorkflowSource{}, err
	}

	return wfs, nil
}

// 对Step的DockerEnv、Cache进行全局替换
func (wfs *WorkflowSource) ValidateRunNodes(components map[string]Component, yamlMap map[string]interface{}, componentsMap map[string]interface{}) error {
	for name, component := range components {
		if dag, ok := component.(*WorkflowSourceDag); ok {
			subComponent, ok, err := unstructured.NestedFieldCopy(componentsMap, name, "entry_points")
			if err != nil || !ok {
				return fmt.Errorf("get subComponent failed")
			}
			subComponentMap, ok := subComponent.(map[string]interface{})
			if !ok {
				return fmt.Errorf("get subComponentMap failed")
			}
			if err := wfs.ValidateRunNodes(dag.EntryPoints, yamlMap, subComponentMap); err != nil {
				return err
			}
		} else if step, ok := component.(*WorkflowSourceStep); ok {
			if step.Env == nil {
				step.Env = map[string]string{}
			}
			if step.Parameters == nil {
				step.Parameters = map[string]interface{}{}
			}

			// Reference节点不用替换
			if step.Reference == "" {
				// DockerEnv字段替换检查
				if step.DockerEnv == "" {
					step.DockerEnv = wfs.DockerEnv
				}

				// 检查是否需要全局Cache替换（节点Cache字段优先级大于全局Cache字段）
				componentCache, ok, err := unstructured.NestedFieldCopy(componentsMap, name, "cache")
				if err != nil || !ok {
					return fmt.Errorf("get componentCache failed")
				}
				componentCacheMap, ok := componentCache.(map[string]interface{})
				if !ok {
					return fmt.Errorf("get componentCacheMap failed")
				}
				globalCache, ok, err := unstructured.NestedFieldCopy(yamlMap, "cache")
				if err != nil || !ok {
					return fmt.Errorf("get globalCache failed")
				}
				globalCacheMap, ok := globalCache.(map[string]interface{})
				if !ok {
					return fmt.Errorf("get globalCacheMap failed")
				}
				if err := ValidateStepCacheByMap(&step.Cache, globalCacheMap, componentCacheMap); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func ValidateStepCacheByMap(cache *Cache, globalCacheMap map[string]interface{}, componentCacheMap map[string]interface{}) error {
	// 节点级别的Cache字段的优先级大于全局Cache字段
	parser := Parser{}
	// 先将节点的Cache，用全局的Cache赋值，这样如果下面节点级别的Cache字段没有再次覆盖，那节点级别的Cache字段就会采用全局的值
	if err := parser.ParseCache(globalCacheMap, cache); err != nil {
		return err
	}
	if err := parser.ParseCache(componentCacheMap, cache); err != nil {
		return err
	}
	return nil
}

func setCacheByMap(cache *Cache, cacheMap map[string]interface{}) {

}
