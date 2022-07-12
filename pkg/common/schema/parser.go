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
	"strconv"
	"strings"
)

type Parser struct {
}

// 该函数将请求体解析成WorkflowSource，
// 该函数未完成全局替换操作
func (p *Parser) ParseWorkflowSource(bodyMap map[string]interface{}, wfs *WorkflowSource) error {
	for key, value := range bodyMap {
		switch key {
		case "name":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[name] of workflow should be string type")
			}
			wfs.Name = value
		case "docker_env":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[docker_env] of workflow should be string type")
			}
			wfs.DockerEnv = value
		case "entry_points":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[entry_points] of workflow should be map[string]interface{} type")
			}
			entryPointsMap, err := p.ParseComponents(value)
			if err != nil {
				return fmt.Errorf("parse [entry_points] failed, error: %s", err.Error())
			}
			entryPoints := WorkflowSourceDag{
				EntryPoints: entryPointsMap,
			}
			wfs.EntryPoints = entryPoints
		case "components":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[components] of workflow should be map[string]interface{} type")
			}
			componentsMap, err := p.ParseComponents(value)
			if err != nil {
				return fmt.Errorf("parse [components] failed, error: %s", err.Error())
			}
			wfs.Components = componentsMap
		case "cache":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[cache] of workflow should be map[string]interface{} type")
			}
			cache := Cache{}
			if err := p.ParseCache(value, &cache); err != nil {
				return fmt.Errorf("parse [cache] in workflow failed, error: %s", err.Error())
			}
			wfs.Cache = cache
		case "parallelism":
			value, ok := value.(int64)
			if !ok {
				return fmt.Errorf("[parallelism] of workflow should be int type")
			}
			wfs.Parallelism = int(value)
		case "disabled":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[disabled] of workflow should be string type")
			}
			wfs.Disabled = value
		case "failure_options":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[failure_options] of workflow should be map[string]interface{} type")
			}
			options := FailureOptions{}
			for optKey, optValue := range value {
				switch optKey {
				case "strategy":
					optValue, ok := optValue.(string)
					if !ok {
						return fmt.Errorf("[failure_options.strategy] of workflow should be string type")
					}
					options.Strategy = optValue
				default:
					return fmt.Errorf("[failure_options] has no attribute [%s]", optKey)
				}
			}
			wfs.FailureOptions = options
		case "post_process":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[post_process] of workflow should be map[string]interface{} type")
			}
			postMap, err := p.ParseComponents(value)
			if err != nil {
				return fmt.Errorf("parse [post_process] failed, error: %s", err.Error())
			}
			wfs.PostProcess = map[string]*WorkflowSourceStep{}
			for postkey, postValue := range postMap {
				postValue, ok := postValue.(*WorkflowSourceStep)
				if !ok {
					return fmt.Errorf("[post_process] can only have step")
				}
				wfs.PostProcess[postkey] = postValue
			}
		case "fs_options":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[fs_options] of workflow should be map[string]interface{} type")
			}
			fsOptions := FsOptions{}
			if err := p.ParseFsOptions(value, &fsOptions); err != nil {
				return err
			}
			wfs.FsOptions = fsOptions
		default:
			return fmt.Errorf("workflow has no attribute [%s]", key)
		}
	}
	return nil
}

func (p *Parser) ParseComponents(entryPoints map[string]interface{}) (map[string]Component, error) {
	components := map[string]Component{}
	for name, component := range entryPoints {
		compMap, ok := component.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("component should be map type")
		}
		if p.IsDag(compMap) {
			dagComp := WorkflowSourceDag{}
			if err := p.ParseDag(compMap, &dagComp); err != nil {
				return nil, fmt.Errorf("parse dag[%s] failed, %v", name, err)
			}
			dagComp.Name = name
			components[name] = &dagComp
		} else {
			stepComp := WorkflowSourceStep{}
			if err := p.ParseStep(compMap, &stepComp); err != nil {
				return nil, fmt.Errorf("parse step[%s] failed, %v", name, err)
			}
			stepComp.Name = name
			components[name] = &stepComp
		}
	}
	return components, nil
}

func (p *Parser) ParseStep(params map[string]interface{}, step *WorkflowSourceStep) error {
	for key, value := range params {
		switch key {
		case "loop_argument":
			step.LoopArgument = value
		case "condition":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[condition] in step should be string type")
			}
			step.Condition = value
		case "parameters":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[parameters] in step should be map type")
			}
			step.Parameters = value
		case "command":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[command] in step should be string type")
			}
			step.Command = value
		case "deps":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[deps] in step should be string type")
			}
			step.Deps = value
		case "artifacts":
			artifacts := Artifacts{}
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[artifacts] in step should be map type")
			}
			for atfKey, atfValue := range value {
				switch atfKey {
				case "output":
					atfMap := map[string]string{}
					switch atfValue := atfValue.(type) {
					case []interface{}:
						for _, atfName := range atfValue {
							atfName, ok := atfName.(string)
							if !ok {
								return fmt.Errorf("[artifacts.output] in step should be list of string type")
							}
							atfMap[atfName] = ""
						}
					case map[string]interface{}:
						// 这个Case除了给用户提供灵活的写法外，更主要的目的是适配Json接口，因为Json转Yaml会得到map格式的artifacts.output
						for atfName, atfPath := range atfValue {
							atfPath, ok := atfPath.(string)
							if !ok || atfPath != "" {
								return fmt.Errorf("[artifacts.output] with map type must use empty string(\"\") value, list type is recomended")
							}
							atfMap[atfName] = ""
						}
					default:
						return fmt.Errorf("[artifact.output] should be map type with empty string(\"\") value or list type")
					}
					artifacts.Output = atfMap
				case "input":
					atfValue, ok := atfValue.(map[string]interface{})
					if !ok {
						return fmt.Errorf("[artifacts.input] in step should be map[string]string type")
					}
					atfMap := map[string]string{}
					for k, v := range atfValue {
						v, ok := v.(string)
						if !ok {
							return fmt.Errorf("[artifacts.input] in step should be map[string]string type")
						}
						atfMap[k] = v
					}
					artifacts.Input = atfMap
				default:
					return fmt.Errorf("[artifacts] of step has no attribute [%s]", atfKey)
				}
			}
			step.Artifacts = artifacts
		case "env":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[env] in step should be map type")
			}
			if step.Env == nil {
				step.Env = map[string]string{}
			}
			// 设置在env里的变量优先级最高，通过其他字段设置的env变量，在这里会被覆盖值
			for envKey, envValue := range value {
				resEnv := ""
				switch envValue := envValue.(type) {
				case string:
					resEnv = envValue
				case int64:
					resEnv = strconv.FormatInt(envValue, 10)
				case float64:
					resEnv = strings.TrimRight(strconv.FormatFloat(envValue, 'f', 8, 64), "0")
				default:
					return fmt.Errorf("values in [env] should be string type")
				}
				step.Env[envKey] = resEnv
			}
		case "docker_env":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[docker_env] in step should be string type")
			}
			step.DockerEnv = value
		case "cache":
			cache := Cache{}
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[cache] in step should be map[string]interface type")
			}
			if err := p.ParseCache(value, &cache); err != nil {
				return fmt.Errorf("parse cache in step failed, error: %s", err.Error())
			}
			step.Cache = cache
		case "reference":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[reference] in step should be map type")
			}
			reference := Reference{}
			for refKey, refValue := range value {
				switch refKey {
				case "component":
					refValue, ok := refValue.(string)
					if !ok {
						return fmt.Errorf("[reference.component] in step should be string type")
					}
					reference.Component = refValue
				default:
					return fmt.Errorf("[reference] of step has no attribute [%s]", refKey)
				}
			}
			step.Reference = reference
		case "fs_mount":
			value, ok := value.([]interface{})
			if !ok {
				return fmt.Errorf("[fs_mount] should be list type")
			}
			for _, m := range value {
				mapValue, ok := m.(map[string]interface{})
				if !ok {
					return fmt.Errorf("mount info in [fs_mount] should be map type")
				}
				fsMount := FsMount{}
				if err := p.ParseFsMount(mapValue, &fsMount); err != nil {
					return fmt.Errorf("parse [fs_mount] in step failed, error: %s", err.Error())
				}
				step.FsMount = append(step.FsMount, fsMount)
			}
		case "type":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[type] of step should be string type")
			}
			valueLower := strings.ToLower(value)
			if valueLower != "step" {
				return fmt.Errorf("set [type] as [%s] in step", value)
			}
		default:
			return fmt.Errorf("step has no attribute [%s]", key)
		}
	}
	return nil
}

// 该函数用于给生成给WorkflowSourceDag的各个字段赋值，但不会进行默认值填充，不会进行全局参数对局部参数的替换
func (p *Parser) ParseDag(params map[string]interface{}, dagComp *WorkflowSourceDag) error {
	for key, value := range params {
		switch key {
		case "loop_argument":
			dagComp.LoopArgument = value
		case "condition":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[condition] in dag should be string type")
			}
			dagComp.Condition = value
		case "parameters":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[parameters] in dag should be map type")
			}
			dagComp.Parameters = value
		case "deps":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[deps] in dag should be string type")
			}
			dagComp.Deps = value
		case "artifacts":
			artifacts := Artifacts{}
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[artifacts] in dag should be map type")
			}
			for atfKey, atfValue := range value {
				switch atfKey {
				case "output":
					atfValue, ok := atfValue.(map[string]interface{})
					if !ok {
						return fmt.Errorf("[artifacts.output] in dag should be map[string]string type")
					}
					atfMap := map[string]string{}
					for k, v := range atfValue {
						v, ok := v.(string)
						if !ok {
							return fmt.Errorf("[artifacts.output] in dag should be map[string]string type")
						}
						atfMap[k] = v
					}
					artifacts.Output = atfMap
				case "input":
					atfValue, ok := atfValue.(map[string]interface{})
					if !ok {
						return fmt.Errorf("[artifacts.output] in dag should be map[string]string type")
					}
					atfMap := map[string]string{}
					for k, v := range atfValue {
						v, ok := v.(string)
						if !ok {
							return fmt.Errorf("[artifacts.output] in dag should be map[string]string type")
						}
						atfMap[k] = v
					}
					artifacts.Input = atfMap
				default:
					return fmt.Errorf("[artifacts] of dag has no attribute [%s]", atfKey)
				}
			}
			dagComp.Artifacts = artifacts
		case "entry_points":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[entry_points] of dag should be map type")
			}
			entryPoints, err := p.ParseComponents(value)
			if err != nil {
				return err
			}
			dagComp.EntryPoints = entryPoints
		case "type":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[type] of dag should be string type")
			}
			valueLower := strings.ToLower(value)
			if valueLower != "dag" {
				return fmt.Errorf("set [type] as [%s] in dag", value)
			}
		default:
			return fmt.Errorf("dag has no attribute [%s]", key)
		}
	}
	return nil
}

func (p *Parser) ParseCache(cacheMap map[string]interface{}, cache *Cache) error {
	for cacheKey, cacheValue := range cacheMap {
		switch cacheKey {
		case "enable":
			cacheValue, ok := cacheValue.(bool)
			if !ok {
				return fmt.Errorf("[cache.enable] should be bool type")
			}
			cache.Enable = cacheValue
		case "max_expired_time":
			switch cacheValue := cacheValue.(type) {
			case string:
				cache.MaxExpiredTime = cacheValue
			case int64:
				cache.MaxExpiredTime = strconv.FormatInt(cacheValue, 10)
			default:
				return fmt.Errorf("[cache.max_expired_time] should be string/int64 type")
			}
		case "fs_scope":
			cacheValue, ok := cacheValue.([]interface{})
			if !ok {
				return fmt.Errorf("[cache.fs_scope] should be list type")
			}
			fsScopeList := []FsScope{}
			for _, m := range cacheValue {
				fsScopeMap, ok := m.(map[string]interface{})
				if !ok {
					return fmt.Errorf("each info in [fs_scope] should be map type")
				}
				fsScope := FsScope{}
				if err := p.ParseFsScope(fsScopeMap, &fsScope); err != nil {
					return fmt.Errorf("parse fs_scope in [cache] failed, error: %s", err.Error())
				}

				fsScopeList = append(fsScopeList, fsScope)
			}
			// 这里这样使用append是为了让后解析的FsScope列表中的元素，排在前面
			cache.FsScope = append(fsScopeList, cache.FsScope...)
		default:
			return fmt.Errorf("[cache] has no attribute [%s]", cacheKey)
		}
	}
	return nil
}

func (p *Parser) ParseFsScope(fsMap map[string]interface{}, fs *FsScope) error {
	for key, value := range fsMap {
		switch key {
		case "fs_name":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[fs_name] should be string type")
			}
			fs.FsName = value
		case "path":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[path] should be string type")
			}
			fs.Path = value
		default:
			return fmt.Errorf("[fs_scope] has no attribute [%s]", key)
		}
	}
	return nil
}

func (p *Parser) ParseFsOptions(fsMap map[string]interface{}, fs *FsOptions) error {
	for key, value := range fsMap {
		switch key {
		case "global_fs_name":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[fs_options.global_fs_name] should be string type")
			}
			fs.GlobalFsName = value
		case "fs_mount":
			value, ok := value.([]interface{})
			if !ok {
				return fmt.Errorf("[fs_options.fs_mount] should be list type")
			}
			for _, m := range value {
				mapValue, ok := m.(map[string]interface{})
				if !ok {
					return fmt.Errorf("each mount info in [fs_options.fs_mount] should be map type")
				}
				fsMount := FsMount{}
				if err := p.ParseFsMount(mapValue, &fsMount); err != nil {
					return fmt.Errorf("parse fs_mount in [fs_options] failed, error: %s", err.Error())
				}
				fs.FsMount = append(fs.FsMount, fsMount)
			}
		default:
			return fmt.Errorf("[fs_options] has no attribute [%s]", key)
		}
	}
	return nil
}

func (p *Parser) ParseFsMount(fsMap map[string]interface{}, fs *FsMount) error {
	for key, value := range fsMap {
		switch key {
		case "fs_name":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[fs_name] should be string type")
			}
			fs.FsName = value
		case "mount_path":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[mount_path] should be string type")
			}
			fs.MountPath = value
		case "sub_path":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[sub_path] should be string type")
			}
			fs.SubPath = value
		case "readonly":
			value, ok := value.(bool)
			if !ok {
				return fmt.Errorf("[readonly] should be bool type")
			}
			fs.Readonly = value
		default:
			return fmt.Errorf("[fs_mount] has no attribute [%s]", key)
		}
	}
	return nil
}

func (p *Parser) IsDag(comp map[string]interface{}) bool {
	if _, ok := comp["entry_points"]; ok {
		return true
	}
	return false
}

func (p *Parser) TransJsonMap2Yaml(jsonMap map[string]interface{}) error {
	for key, value := range jsonMap {
		switch key {
		case "dockerEnv":
			jsonMap["docker_env"] = value
			delete(jsonMap, "dockerEnv")
		case "entryPoints":
			if err := p.transJsonSubComp2Yaml(value, "entryPoints"); err != nil {
				return err
			}
			jsonMap["entry_points"] = value
			delete(jsonMap, "entryPoints")
		case "failureOptions":
			jsonMap["failure_options"] = value
			delete(jsonMap, "failureOptions")
		case "postProcess":
			if err := p.transJsonSubComp2Yaml(value, "postProcess"); err != nil {
				return err
			}
			jsonMap["post_process"] = value
			delete(jsonMap, "postProcess")
		case "loopArgument":
			jsonMap["loop_argument"] = value
			delete(jsonMap, "loopArgument")
		case "components":
			if err := p.transJsonSubComp2Yaml(value, "components"); err != nil {
				return err
			}
		case "fsMount":
			if err := p.transJsonFsMount2Yaml(value); err != nil {
				return err
			}
			jsonMap["fs_mount"] = value
			delete(jsonMap, "fsMount")
		case "cache":
			if err := p.transJsonCache2Yaml(value); err != nil {
				return err
			}
			jsonMap["cache"] = value
		case "fsOptions":
			if err := p.transJsonFsOptions2Yaml(value); err != nil {
				return err
			}
			jsonMap["fs_options"] = value
			delete(jsonMap, "fsOptions")
		}
	}
	return nil
}

func (p *Parser) transJsonCache2Yaml(value interface{}) error {
	cacheMap, ok := value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("[cache] should be map type")
	}
	for cacheKey, cacheValue := range cacheMap {
		switch cacheKey {
		case "maxExpiredTime":
			cacheMap["max_expired_time"] = cacheValue
			delete(cacheMap, "maxExpiredTime")
		case "fsScope":
			scopeList, ok := cacheValue.([]interface{})
			if !ok {
				return fmt.Errorf("fsScope should be list type")
			}
			for i, scope := range scopeList {
				scopeMap, ok := scope.(map[string]interface{})
				if !ok {
					return fmt.Errorf("each scope in [fsScope] should be map type")
				}
				for scopeKey, scopeValue := range scopeMap {
					switch scopeKey {
					case "fsName":
						scopeMap["fs_name"] = scopeValue
						delete(scopeMap, "fsName")
					}
				}
				scopeList[i] = scopeMap
			}

			cacheMap["fs_scope"] = scopeList
			delete(cacheMap, "fsScope")
		}
	}
	return nil
}

func (p *Parser) transJsonFsMount2Yaml(value interface{}) error {
	mountList, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("[fsMount] should be list type")
	}
	for i, mount := range mountList {
		mountMap, ok := mount.(map[string]interface{})
		if !ok {
			return fmt.Errorf("each mount info in [fsMount] should map type")
		}
		for mountKey, mountValue := range mountMap {
			switch mountKey {
			case "fsName":
				mountMap["fs_name"] = mountValue
				delete(mountMap, "fsName")
			case "mountPath":
				mountMap["mount_path"] = mountValue
				delete(mountMap, "mountPath")
			case "subPath":
				mountMap["sub_path"] = mountValue
				delete(mountMap, "subPath")
			}
		}
		mountList[i] = mountMap
	}
	return nil
}

func (p *Parser) transJsonSubComp2Yaml(value interface{}, filedType string) error {
	compsMap, ok := value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("[%s] should be map type", filedType)
	}
	for _, comp := range compsMap {
		compMap, ok := comp.(map[string]interface{})
		if !ok {
			return fmt.Errorf("each components in [%s] should be map type", filedType)
		}
		if err := p.TransJsonMap2Yaml(compMap); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) transJsonFsOptions2Yaml(value interface{}) error {
	fsOptMap, ok := value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("[fsOptions] should be map type")
	}
	for key, value := range fsOptMap {
		switch key {
		case "fsMount":
			if err := p.transJsonFsMount2Yaml(value); err != nil {
				return err
			}
			fsOptMap["fs_mount"] = value
			delete(fsOptMap, "fsMount")
		case "globalFsName":
			fsOptMap["global_fs_name"] = value
			delete(fsOptMap, "globalFsName")
		}
	}
	return nil
}
