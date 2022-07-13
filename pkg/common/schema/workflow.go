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
	"strings"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
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

	FsPrefix = "fs-"
)

func ID(userName, fsName string) string {
	return FsPrefix + userName + "-" + fsName
}

type Artifacts struct {
	Input  map[string]string `yaml:"input"       json:"input"`
	Output map[string]string `yaml:"output"      json:"output"`
}

func (art *Artifacts) ValidateOutputMapByList() error {
	if art.Output == nil {
		art.Output = make(map[string]string)
	}
	for _, outputName := range art.Output {
		art.Output[outputName] = ""
	}
	return nil
}

func (art *Artifacts) DeepCopy() *Artifacts {
	input := map[string]string{}
	output := map[string]string{}

	for name, value := range art.Input {
		input[name] = value
	}

	for name, value := range art.Output {
		output[name] = value
	}

	nArt := &Artifacts{
		Input:  input,
		Output: output,
	}
	return nArt
}

func getLoopArgumentLength(lp interface{}) int {
	if lp == nil {
		return 0
	}

	t := reflect.TypeOf(lp)
	if t.Kind() != reflect.Slice {
		return 0
	} else {
		v := reflect.ValueOf(lp)
		return v.Len()
	}
}

// Component包括Dag和Step，有Struct WorkflowSourceStep 和 WorkflowSourceDag实现了该接口
type Component interface {
	GetDeps() []string
	GetArtifacts() Artifacts
	GetArtifactPath(artName string) (string, error)
	GetInputArtifactPath(artName string) (string, error)
	GetOutputArtifactPath(artName string) (string, error)
	GetParameters() map[string]interface{}
	GetParameterValue(paramName string) (interface{}, error)
	GetCondition() string
	GetLoopArgument() interface{}
	GetLoopArgumentLength() int
	GetType() string
	GetName() string

	// 下面几个Update 函数在进行模板替换的时候会用到
	UpdateCondition(string)
	UpdateLoopArguemt(interface{})
	UpdateName(name string)
	UpdateDeps(deps string)

	InitInputArtifacts()
	InitOutputArtifacts()
	InitParameters()

	// 用于 deepCopy, 避免复用时出现问题
	DeepCopy() Component
}

type WorkflowSourceStep struct {
	Name         string                 `yaml:"-"`
	LoopArgument interface{}            `yaml:"loop_argument"`
	Condition    string                 `yaml:"condition"`
	Parameters   map[string]interface{} `yaml:"parameters"`
	Command      string                 `yaml:"command"`
	Deps         string                 `yaml:"deps"`
	Artifacts    Artifacts              `yaml:"artifacts"`
	Env          map[string]string      `yaml:"env"`
	DockerEnv    string                 `yaml:"docker_env"`
	Cache        Cache                  `yaml:"cache"`
	Reference    Reference              `yaml:"reference"`
	FsMount      []FsMount              `yaml:"fs_mount"`
}

func (s *WorkflowSourceStep) GetName() string {
	return s.Name
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

func (s *WorkflowSourceStep) UpdateName(name string) {
	s.Name = name
}

func (s *WorkflowSourceStep) UpdateLoopArguemt(loopArgument interface{}) {
	s.LoopArgument = loopArgument
}

func (s *WorkflowSourceStep) UpdateDeps(deps string) {
	s.Deps = deps
}

func (s *WorkflowSourceStep) InitOutputArtifacts() {
	s.Artifacts.Output = map[string]string{}
}

func (s *WorkflowSourceStep) InitInputArtifacts() {
	s.Artifacts.Input = map[string]string{}
}

func (s *WorkflowSourceStep) InitParameters() {
	s.Parameters = map[string]interface{}{}
}

// 获取 artifact 的路径
func (s *WorkflowSourceStep) GetArtifactPath(artName string) (string, error) {
	path, err := s.GetInputArtifactPath(artName)
	if err == nil {
		return path, err
	}

	path, err = s.GetOutputArtifactPath(artName)
	if err == nil {
		return path, err
	}

	err = fmt.Errorf("there is no artifact named [%s] in this component", artName)
	return "", err
}

// 获取指定 parameter 的值
func (s *WorkflowSourceStep) GetParameterValue(paramName string) (interface{}, error) {
	value, ok := s.Parameters[paramName]
	if !ok {
		err := fmt.Errorf("there is no parameter named [%s] in this component", paramName)
		return nil, err
	}

	return value, nil
}

// 获取 输入artifact的存储路径
func (s *WorkflowSourceStep) GetInputArtifactPath(artName string) (string, error) {
	path, ok := s.Artifacts.Input[artName]
	if !ok {
		err := fmt.Errorf("there is no input artifact named [%s] in this component", artName)
		return "", err
	}

	return path, nil
}

// 获取输出artifact的存储路径
func (s *WorkflowSourceStep) GetOutputArtifactPath(artName string) (string, error) {
	path, ok := s.Artifacts.Output[artName]
	if !ok {
		err := fmt.Errorf("there is no output artifact named [%s] in this component", artName)
		return "", err
	}

	return path, nil
}

func (s *WorkflowSourceStep) GetLoopArgumentLength() int {
	return getLoopArgumentLength(s.GetLoopArgument())
}

func (s *WorkflowSourceStep) DeepCopy() Component {
	params := map[string]interface{}{}
	for name, value := range s.Parameters {
		params[name] = value
	}

	env := map[string]string{}
	for name, value := range s.Env {
		env[name] = value
	}

	fsMount := append(s.FsMount, []FsMount{}...)

	ns := &WorkflowSourceStep{
		Name:         s.Name,
		LoopArgument: s.LoopArgument,
		Condition:    s.Condition,
		Parameters:   params,
		Command:      s.Command,
		Deps:         s.Deps,
		Env:          env,
		Artifacts:    *s.Artifacts.DeepCopy(),
		DockerEnv:    s.DockerEnv,
		Cache:        s.Cache,
		Reference:    s.Reference,
		FsMount:      fsMount,
	}

	return ns
}

type WorkflowSourceDag struct {
	Name         string                 `yaml:"-"`
	Type         string                 `yaml:"-"`
	LoopArgument interface{}            `yaml:"loop_argument"`
	Condition    string                 `yaml:"condition"`
	Parameters   map[string]interface{} `yaml:"parameters"`
	Deps         string                 `yaml:"deps"`
	Artifacts    Artifacts              `yaml:"artifacts"`
	EntryPoints  map[string]Component   `yaml:"entry_points"`
}

func (d *WorkflowSourceDag) GetName() string {
	return d.Name
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

func (d *WorkflowSourceDag) GetLoopArgumentLength() int {
	return getLoopArgumentLength(d.GetLoopArgument())
}

func (d *WorkflowSourceDag) UpdateName(name string) {
	d.Name = name
}

func (d *WorkflowSourceDag) UpdateCondition(condition string) {
	d.Condition = condition
}

func (d *WorkflowSourceDag) UpdateLoopArguemt(loopArgument interface{}) {
	d.LoopArgument = loopArgument
}

func (d *WorkflowSourceDag) UpdateDeps(deps string) {
	d.Deps = deps
}

func (d *WorkflowSourceDag) GetSubComponet(subComponentName string) (Component, bool) {
	sc, ok := d.EntryPoints[subComponentName]
	return sc, ok
}

// 获取 artifact 的路径
func (d *WorkflowSourceDag) GetArtifactPath(artName string) (string, error) {
	path, err := d.GetInputArtifactPath(artName)
	if err == nil {
		return path, err
	}

	path, err = d.GetOutputArtifactPath(artName)
	if err == nil {
		return path, err
	}

	err = fmt.Errorf("there is no artifact named [%s] in this component", artName)
	return "", err
}

// 获取指定 parameter 的值
func (d *WorkflowSourceDag) GetParameterValue(paramName string) (interface{}, error) {
	value, ok := d.Parameters[paramName]
	if !ok {
		err := fmt.Errorf("there is no parameter named [%s] in this component", paramName)
		return nil, err
	}

	return value, nil
}

// 获取 输入artifact的存储路径
func (d *WorkflowSourceDag) GetInputArtifactPath(artName string) (string, error) {
	path, ok := d.Artifacts.Input[artName]
	if !ok {
		err := fmt.Errorf("there is no input artifact named [%s] in this component", artName)
		return "", err
	}

	return path, nil
}

// 获取输出artifact的存储路径
func (d *WorkflowSourceDag) GetOutputArtifactPath(artName string) (string, error) {
	path, ok := d.Artifacts.Output[artName]
	if !ok {
		err := fmt.Errorf("there is no output artifact named [%s] in this component", artName)
		return "", err
	}

	return path, nil
}

func (d *WorkflowSourceDag) InitOutputArtifacts() {
	d.Artifacts.Output = map[string]string{}
}

func (d *WorkflowSourceDag) InitInputArtifacts() {
	d.Artifacts.Input = map[string]string{}
}

func (d *WorkflowSourceDag) InitParameters() {
	d.Parameters = map[string]interface{}{}
}

func (d *WorkflowSourceDag) DeepCopy() Component {
	params := map[string]interface{}{}
	for name, value := range d.Parameters {
		params[name] = value
	}

	ep := map[string]Component{}
	for name, value := range d.EntryPoints {
		ep[name] = value.DeepCopy()
	}

	nd := &WorkflowSourceDag{
		Name:         d.Name,
		LoopArgument: d.LoopArgument,
		Condition:    d.Condition,
		Parameters:   params,
		Deps:         d.Deps,
		Artifacts:    *d.Artifacts.DeepCopy(),
		EntryPoints:  ep,
	}

	return nd
}

type Reference struct {
	Component string
}

type Cache struct {
	Enable         bool      `yaml:"enable"           json:"enable"`
	MaxExpiredTime string    `yaml:"max_expired_time" json:"maxExpiredTime"` // seconds
	FsScope        []FsScope `yaml:"fs_scope"         json:"fsScope"`        // seperated by ","
}

type FsScope struct {
	FsName string `yaml:"fs_name"       json:"fsName"`
	FsID   string `yaml:"-"             json:"fsID"`
	Path   string `yaml:"path"          json:"path"`
}

type FailureOptions struct {
	Strategy string `yaml:"strategy"     json:"strategy"`
}

type FsOptions struct {
	GlobalFsName string    `yaml:"global_fs_name"    json:"globalFsName"`
	FsMount      []FsMount `yaml:"fs_mount"     json:"fsMount"`
}

type FsMount struct {
	FsID      string `yaml:"-"             json:"fsID"`
	FsName    string `yaml:"fs_name"       json:"fsName"`
	MountPath string `yaml:"mount_path"    json:"mountPath"`
	SubPath   string `yaml:"sub_path"      json:"subPath"`
	Readonly  bool   `yaml:"readonly"      json:"readonly"`
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
	FsOptions      FsOptions                      `yaml:"fs_options"`
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
	_, _, ok1 := wfs.GetComponent(wfs.EntryPoints.EntryPoints, componentName)
	_, _, ok2 := wfs.GetComponent(postComponents, componentName)
	if !ok1 && !ok2 {
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
func (wfs *WorkflowSource) GetComponent(components map[string]Component, absoluteName string) (map[string]Component, string, bool) {
	nameList := strings.SplitN(absoluteName, ".", 2)
	if len(nameList) > 1 {
		if component, ok := components[nameList[0]]; ok {
			if dag, ok := component.(*WorkflowSourceDag); ok {
				return wfs.GetComponent(dag.EntryPoints, nameList[1])
			} else if step, ok := component.(*WorkflowSourceStep); ok {
				// 如果为step，检查是否有引用Source.Components中的节点
				referComp := step.Reference.Component
				return wfs.componentsHasStep(referComp, nameList[1])
			} else {
				logger.Logger().Errorf("a component not dag or step")
				return nil, "", false
			}
		} else {
			return nil, "", false
		}
	} else {
		_, ok := components[absoluteName]
		return components, absoluteName, ok
	}
}

func (wfs *WorkflowSource) componentsHasStep(referComp string, subNames string) (map[string]Component, string, bool) {
	// 在前面已经校验过递归reference因此不会有递归出现，但后续可能会允许递归
	// TODO: 如果有递归reference的情况，这里会出现死循环，需要升级
	for {
		if referedComponent, ok := wfs.Components[referComp]; ok {
			if dag, ok := referedComponent.(*WorkflowSourceDag); ok {
				// 检查Source.Components中的节点，如果它是一个dag，那就继续向下遍历子节点
				return wfs.GetComponent(dag.EntryPoints, subNames)
			} else if step, ok := referedComponent.(*WorkflowSourceStep); ok {
				// 如果是step，那就看是否继续ref了其他component
				referComp = step.Reference.Component
				continue
			} else {
				logger.Logger().Errorf("a component not dag or step")
				return nil, "", false
			}
		} else {
			return nil, "", false
		}
	}
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
	yamlMap, err := RunYaml2Map(runYaml)
	if err != nil {
		logger.Logger().Errorf(err.Error())
		return WorkflowSource{}, err
	}

	return GetWorkflowSourceByMap(yamlMap)
}

// 由Map解析得到一个Wfs，该Map中的key需要是下划线格式
func GetWorkflowSourceByMap(yamlMap map[string]interface{}) (WorkflowSource, error) {
	wfs := WorkflowSource{
		FailureOptions: FailureOptions{Strategy: FailureStrategyFailFast},
	}
	p := Parser{}
	if err := p.ParseWorkflowSource(yamlMap, &wfs); err != nil {
		logger.Logger().Errorf(err.Error())
		return WorkflowSource{}, err
	}

	// 全局参数替换
	entryPointsMap, ok := yamlMap["entry_points"].(map[string]interface{})
	if !ok {
		return WorkflowSource{}, fmt.Errorf("get entry_points map failed")
	}
	if err := wfs.ProcessRuntimeComponents(wfs.EntryPoints.EntryPoints, yamlMap, entryPointsMap); err != nil {
		return WorkflowSource{}, err
	}

	postComponentsMap := map[string]Component{}
	for k, v := range wfs.PostProcess {
		postComponentsMap[k] = v
	}
	postProcessMap, ok := yamlMap["post_process"].(map[string]interface{})
	if ok {
		if err := wfs.ProcessRuntimeComponents(postComponentsMap, yamlMap, postProcessMap); err != nil {
			return WorkflowSource{}, err
		}
	}

	componentMap, ok := yamlMap["components"].(map[string]interface{})
	if ok {
		if err := wfs.ProcessRuntimeComponents(wfs.Components, yamlMap, componentMap); err != nil {
			return WorkflowSource{}, err
		}
	}
	return wfs, nil
}

// 对Step的DockerEnv、Cache进行全局替换
func (wfs *WorkflowSource) ProcessRuntimeComponents(components map[string]Component, yamlMap map[string]interface{}, componentsMap map[string]interface{}) error {
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
			if err := wfs.ProcessRuntimeComponents(dag.EntryPoints, yamlMap, subComponentMap); err != nil {
				return err
			}
		} else if step, ok := component.(*WorkflowSourceStep); ok {
			if step.Env == nil {
				step.Env = map[string]string{}
			}

			// Reference节点不用替换
			if step.Reference.Component == "" {
				// DockerEnv字段替换检查
				if step.DockerEnv == "" {
					step.DockerEnv = wfs.DockerEnv
				}

				// 检查是否需要全局Cache替换（节点Cache字段优先级大于全局Cache字段）
				// 获取节点Cache
				componentCache, ok, err := unstructured.NestedFieldCopy(componentsMap, name, "cache")
				if err != nil {
					return fmt.Errorf("check componentCache failed")
				}
				componentCacheMap := map[string]interface{}{}
				if ok {
					componentCacheMap, ok = componentCache.(map[string]interface{})
					if !ok {
						return fmt.Errorf("get componentCacheMap failed")
					}
				}

				// 获取全局Cache
				globalCache, ok, err := unstructured.NestedFieldCopy(yamlMap, "cache")
				if err != nil {
					return fmt.Errorf("check globalCache failed")
				}
				globalCacheMap := map[string]interface{}{}
				if ok {
					globalCacheMap, ok = globalCache.(map[string]interface{})
					if !ok {
						return fmt.Errorf("get globalCacheMap failed")
					}
				}

				// Cache替换处理
				if err := ProcessStepCacheByMap(&step.Cache, globalCacheMap, componentCacheMap); err != nil {
					return err
				}

				// 合并全局 fs_mount 和节点 fs_mount
				// 获取全局 fs_mount
				globalFsMount, ok, err := unstructured.NestedFieldCopy(yamlMap, "fs_options", "fs_mount")
				if err != nil {
					return fmt.Errorf("check globalFsMount failed")
				}
				globalFsMountList := []interface{}{}
				if ok {
					globalFsMountList, ok = globalFsMount.([]interface{})
					if !ok {
						return fmt.Errorf("get globalFsMountList failed")
					}
				}

				// fs_mount 合并
				if err := ProcessStepFsMount(&step.FsMount, globalFsMountList); err != nil {
					return err
				}
			}
		}

		if component.GetArtifacts().Input == nil {
			component.InitInputArtifacts()
		}
		if component.GetArtifacts().Output == nil {
			component.InitOutputArtifacts()
		}
		if component.GetParameters() == nil {
			component.InitParameters()
		}
	}
	return nil
}

func ProcessStepCacheByMap(cache *Cache, globalCacheMap map[string]interface{}, componentCacheMap map[string]interface{}) error {
	// 节点级别的Cache字段的优先级大于全局Cache字段
	parser := Parser{}

	// 由于ParseCache中会将节点级别的FsScope再次添加进当前节点的FsScope，因此这里先清空，避免重复
	cache.FsScope = []FsScope{}

	// 先将节点的Cache，用全局的Cache赋值，这样如果下面节点级别的Cache字段没有再次覆盖，那节点级别的Cache字段就会采用全局的值
	if err := parser.ParseCache(globalCacheMap, cache); err != nil {
		return err
	}
	if err := parser.ParseCache(componentCacheMap, cache); err != nil {
		return err
	}
	return nil
}

func ProcessStepFsMount(fsMountList *[]FsMount, globalFsMountList []interface{}) error {
	parser := Parser{}
	for _, m := range globalFsMountList {
		fsMountMap, ok := m.(map[string]interface{})
		if !ok {
			return fmt.Errorf("each global mount info in [fs_mount] list should be map type")
		}
		fsMount := FsMount{}
		if err := parser.ParseFsMount(fsMountMap, &fsMount); err != nil {
			return fmt.Errorf("parse global fsMount failed, error: %s", err.Error())
		}
		*fsMountList = append(*fsMountList, fsMount)
	}
	return nil
}

func (wfs *WorkflowSource) GetComponentByFullName(fullName string) (Component, error) {
	names := strings.Split(fullName, ".")
	comp1, err1 := getComponentRecursively(wfs.EntryPoints.EntryPoints, names)
	postComps := map[string]Component{}
	for k, v := range wfs.PostProcess {
		postComps[k] = v
	}
	comp2, err2 := getComponentRecursively(postComps, names)
	if err2 == nil {
		return comp2, nil
	}
	if err1 == nil {
		return comp1, nil
	}

	return nil, fmt.Errorf("no component has fullName[%s]", fullName)
}

func getComponentRecursively(components map[string]Component, names []string) (Component, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("need names")
	} else {
		subComp, ok := components[names[0]]
		if !ok {
			return nil, fmt.Errorf("no component named [%s]", names[0])
		}
		if len(names) == 1 {
			return subComp, nil
		} else {
			if dag, ok := subComp.(*WorkflowSourceDag); ok {
				return getComponentRecursively(dag.EntryPoints, names[1:])
			} else {
				return nil, fmt.Errorf("invalid fullName")
			}
		}
	}
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

// 给所有Step的fsMount和fsScope的fsID赋值
func (wfs *WorkflowSource) ProcessFsAndGetAllIDs(userName string, globalFsName string) ([]string, error) {
	// 用map记录所有需要返回的ID，去重
	fsIDMap := map[string]int{}

	logger.Logger().Infof("debug: begin process FsID")
	if err := wfs.processFsByUserName(wfs.EntryPoints.EntryPoints, userName, fsIDMap, globalFsName); err != nil {
		return []string{}, err
	}

	if err := wfs.processFsByUserName(wfs.Components, userName, fsIDMap, globalFsName); err != nil {
		return []string{}, err
	}

	postMap := map[string]Component{}
	for k, v := range wfs.PostProcess {
		postMap[k] = v
	}
	if err := wfs.processFsByUserName(postMap, userName, fsIDMap, globalFsName); err != nil {
		return []string{}, err
	}

	resFsIDList := []string{}
	for id := range fsIDMap {
		resFsIDList = append(resFsIDList, id)
	}

	return resFsIDList, nil
}

func (wfs *WorkflowSource) processFsByUserName(compMap map[string]Component, userName string, fsIDMap map[string]int, globalFsName string) error {
	for _, comp := range compMap {
		if dag, ok := comp.(*WorkflowSourceDag); ok {
			if err := wfs.processFsByUserName(dag.EntryPoints, userName, fsIDMap, globalFsName); err != nil {
				return err
			}
		} else if step, ok := comp.(*WorkflowSourceStep); ok {
			// fsNameSet用来检查FsScope中的FsName是否都在FsMount中，或者是global_fs_name
			fsNameSet := map[string]int{globalFsName: 1}
			fsNameSet[wfs.FsOptions.GlobalFsName] = 1

			for i, mount := range step.FsMount {
				if mount.FsName == "" {
					return fmt.Errorf("[fs_name] in fs_mount must be set")
				}
				mount.FsID = ID(userName, mount.FsName)

				fsNameSet[mount.FsName] = 1
				fsIDMap[mount.FsID] = 1
				step.FsMount[i] = mount
				logger.Logger().Infof("debug: after process,  FsID is %s", mount.FsID)
			}
			logger.Logger().Infof("debug: after process,  step fsMount is %v", step.FsMount)
			for i, scope := range step.Cache.FsScope {
				if scope.FsName == "" {
					return fmt.Errorf("[fs_name] in fs_scope must be set")
				}
				scope.FsID = ID(userName, scope.FsName)

				// 检查FsScope中的FsName是否都在FsMount中
				if _, ok := fsNameSet[scope.FsName]; !ok {
					return fmt.Errorf("fs_name [%s] in fs_scope must also be in fs_mount", scope.FsName)
				}
				step.Cache.FsScope[i] = scope
			}
		} else {
			return fmt.Errorf("component not dag or step")
		}
	}
	return nil
}
