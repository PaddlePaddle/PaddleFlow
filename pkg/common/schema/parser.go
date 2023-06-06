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
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"strconv"
	"strings"
	"unsafe"
)

type Parser struct {
}

// 该函数将请求体解析成WorkflowSource，
// 该函数未完成全局替换操作
func (p *Parser) ParseWorkflowSource(bodyMap map[string]interface{}, wfs *WorkflowSource) error {
	for key, value := range bodyMap {
		if value == nil {
			continue
		}
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
			value1, ok1 := value.(int64)
			value2, ok2 := value.(float64) // 这里是为了兼容一个由json.Unmarshal得到的parallelism值
			if ok1 {
				wfs.Parallelism = int(value1)
			} else if ok2 {
				wfs.Parallelism = int(value2)
			} else {
				return fmt.Errorf("[parallelism] of workflow should be int type")
			}

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
			logger.Logger().Infof("Parse Step Result: %v", stepComp)
			stepComp.Name = name
			components[name] = &stepComp
		}
	}
	return components, nil
}

func (p *Parser) ParseStep(params map[string]interface{}, step *WorkflowSourceStep) error {

	for key, value := range params {
		if value == nil {
			continue
		}
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
		case "distributed_jobs":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[distributed_jobs] in step should be map type")
			}

			distJobs := DistributedJob{}
			// parse framework
			framework, ok := value["framework"].(string)
			if !ok {
				return fmt.Errorf("extract framework from [distributed_jobs] failed")
			}
			distJobs.Framework = Framework(framework)

			// parse members
			members, ok, err := unstructured.NestedSlice(value, "members")
			if !ok {
				return fmt.Errorf("extract members from [distributed_jobs] failed because [%v]", err)
			}
			distJobs.Members = make([]Member, 0)

			for index, member := range members {
				mem := Member{}
				memberMap, ok := member.(map[string]interface{})
				if !ok {
					return fmt.Errorf("the member %v defined in [distributed_jobs] should be map type", index)
				}

				for memberKey, memberValue := range memberMap {
					switch memberKey {
					case "role":
						refValue, ok := memberValue.(string)
						if !ok {
							return fmt.Errorf("[role] defined in member %v should be string type", index)
						}
						mem.Role = MemberRole(refValue)
					case "command":
						refValue, ok := memberValue.(string)
						if !ok {
							return fmt.Errorf("[command] defined in member %v should be string type", index)
						}
						mem.Command = refValue
					case "replicas":
						refValue, ok := memberValue.(int64)
						if !ok {
							return fmt.Errorf("[replicas] defined in member %v should be int type", index)
						}
						mem.Replicas = *(*int)(unsafe.Pointer(&refValue))
					case "image":
						refValue, ok := memberValue.(string)
						if !ok {
							return fmt.Errorf("[image] defined in member %v should be string type", index)
						}
						mem.Image = refValue
					case "port":
						refValue, ok := memberValue.(int64)
						if !ok {
							return fmt.Errorf("[port] defined in member %v should be int type", index)
						}
						mem.Port = *(*int)(unsafe.Pointer(&refValue))
					case "queue":
						refValue, ok := memberValue.(string)
						if !ok {
							return fmt.Errorf("[queue] defined in member %v should be string type", index)
						}
						mem.QueueName = refValue
					case "flavour":
						refValue, ok := memberValue.(map[string]interface{})
						flavour := Flavour{}
						if !ok {
							return fmt.Errorf("[flavour] defined in member %v should be map type", index)
						}

						if flavour, err = MapToFlavour(refValue); err != nil {
							return fmt.Errorf("[scalarResources] resolve failed in member %v", index)
						}
						mem.Flavour = flavour
					}
				}
				distJobs.Members = append(distJobs.Members, mem)
			}
			step.DistributedJobs = distJobs
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
		case "extra_fs":
			value, ok := value.([]interface{})
			if !ok {
				return fmt.Errorf("[extra_fs] should be list type")
			}
			for _, m := range value {
				mapValue, ok := m.(map[string]interface{})
				if !ok {
					return fmt.Errorf("mount info in [extra_fs] should be map type")
				}
				fsMount := FsMount{}
				if err := p.ParseFsMount(mapValue, &fsMount); err != nil {
					return fmt.Errorf("parse [extra_fs] in step failed, error: %s", err.Error())
				}
				step.ExtraFS = append(step.ExtraFS, fsMount)
			}
		case "type":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[type] of step should be string type")
			}
			valueLower := strings.ToLower(value)
			if valueLower != "" && valueLower != "step" {
				return fmt.Errorf("set [type] as [%s] in step", value)
			}
		case "name":
			// 该字段不暴露给用户
			continue
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
			logger.Logger().Infof("Entry Points Parse: %v", entryPoints["train"].(*WorkflowSourceStep))
			dagComp.EntryPoints = entryPoints
		case "type":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[type] of dag should be string type")
			}
			valueLower := strings.ToLower(value)
			if valueLower != "" && valueLower != "dag" {
				return fmt.Errorf("set [type] as [%s] in dag", value)
			}
		case "name":
			// 该字段不暴露给用户
			continue
		default:
			return fmt.Errorf("dag has no attribute [%s]", key)
		}
	}
	return nil
}

func (p *Parser) ParseCache(cacheMap map[string]interface{}, cache *Cache) error {
	for cacheKey, cacheValue := range cacheMap {
		if cacheValue == nil {
			continue
		}
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
		case "name":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[name] should be string type")
			}
			fs.Name = value
		case "path":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[path] should be string type")
			}
			fs.Path = value
		case "id":
			// 该字段不暴露给用户
			continue
		default:
			return fmt.Errorf("[fs_scope] has no attribute [%s]", key)
		}
	}
	return nil
}

func (p *Parser) ParseFsOptions(fsMap map[string]interface{}, fs *FsOptions) error {
	for key, value := range fsMap {
		switch key {
		case "main_fs":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[fs_options.main_fs] should be map type")
			}
			fsMount := FsMount{}
			if err := p.ParseFsMount(value, &fsMount); err != nil {
				return fmt.Errorf("parse [fs_options.main_fs] failed, error: %s", err.Error())
			}
			fs.MainFS = fsMount
		case "extra_fs":
			value, ok := value.([]interface{})
			if !ok {
				return fmt.Errorf("[fs_options.extra_fs] should be list type")
			}
			for _, m := range value {
				mapValue, ok := m.(map[string]interface{})
				if !ok {
					return fmt.Errorf("each mount info in [fs_options.extra_fs] should be map type")
				}
				fsMount := FsMount{}
				if err := p.ParseFsMount(mapValue, &fsMount); err != nil {
					return fmt.Errorf("parse [fs_options.extra_fs] failed, error: %s", err.Error())
				}
				fs.ExtraFS = append(fs.ExtraFS, fsMount)
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
		case "name":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[name] should be string type")
			}
			fs.Name = value
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
		case "read_only":
			value, ok := value.(bool)
			if !ok {
				return fmt.Errorf("[read_only] should be bool type")
			}
			fs.ReadOnly = value
		case "id":
			// 该字段不暴露给用户
			continue
		default:
			return fmt.Errorf("[main_fs] or each mount info in [extra_fs] has no attribute [%s]", key)
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
		case "extraFS":
			if err := p.transJsonExtraFS2Yaml(value); err != nil {
				return err
			}
			jsonMap["extra_fs"] = value
			delete(jsonMap, "extraFS")
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
			cacheMap["fs_scope"] = cacheValue
			delete(cacheMap, "fsScope")
		}
	}
	return nil
}

func (p *Parser) transJsonExtraFS2Yaml(value interface{}) error {
	if value == nil {
		return nil
	}
	mountList, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("[extraFS] should be list type")
	}
	for i, mount := range mountList {
		if err := p.transJsonFsMount2Yaml(mount); err != nil {
			return err
		}
		mountList[i] = mount
	}
	return nil
}

func (p *Parser) transJsonFsMount2Yaml(value interface{}) error {
	mountMap, ok := value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("[mainFS] or each mount info in [extraFS] should map type")
	}
	for mountKey, mountValue := range mountMap {
		switch mountKey {
		case "mountPath":
			mountMap["mount_path"] = mountValue
			delete(mountMap, "mountPath")
		case "subPath":
			mountMap["sub_path"] = mountValue
			delete(mountMap, "subPath")
		case "readOnly":
			mountMap["read_only"] = mountValue
			delete(mountMap, "readOnly")
		}
	}
	return nil
}

func (p *Parser) transJsonSubComp2Yaml(value interface{}, filedType string) error {
	if value == nil {
		return nil
	}
	compsMap, ok := value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("[%s] should be map type", filedType)
	}
	for _, comp := range compsMap {
		if comp == nil {
			continue
		}
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
		case "extraFS":
			if err := p.transJsonExtraFS2Yaml(value); err != nil {
				return err
			}
			fsOptMap["extra_fs"] = value
			delete(fsOptMap, "extraFS")
		case "mainFS":
			fsOptMap["main_fs"] = value
			if err := p.transJsonFsMount2Yaml(value); err != nil {
				return err
			}
			delete(fsOptMap, "mainFS")
		}
	}
	return nil
}
