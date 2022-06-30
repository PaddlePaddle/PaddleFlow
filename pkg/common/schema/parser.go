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
)

type Parser struct {
}

// 该函数将请求体解析成WorkflowSource，
// 同时该函数完成了将JobType、Queue、Flavour填充到环境变量的工作，以及将Output Artifact从List转为Map，
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
		case "dockerEnv":
			fallthrough
		case "docker_env":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[docker_env/dockerEnv] of workflow should be string type")
			}
			wfs.DockerEnv = value
		case "entryPoints":
			fallthrough
		case "entry_points":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[entry_points/entryPoints] of workflow should be map[string]interface{} type")
			}
			entryPointsMap, err := p.ParseComponents(value)
			if err != nil {
				return fmt.Errorf("parse [entry_points/entryPoints] failed, error: %s", err.Error())
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
		case "failureOptions":
			fallthrough
		case "failure_options":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[failure_options/failureOptions] of workflow should be map[string]interface{} type")
			}
			options := FailureOptions{}
			for optKey, optValue := range value {
				switch optKey {
				case "strategy":
					optValue, ok := optValue.(string)
					if !ok {
						return fmt.Errorf("[failure_options/failureOptions.strategy] of workflow should be string type")
					}
					options.Strategy = optValue
				default:
					return fmt.Errorf("[failure_options/failureOptions] has no attribute [%s]", optKey)
				}
			}
		case "postProcess":
			fallthrough
		case "post_process":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[post_process/postProcess] of workflow should be map[string]interface{} type")
			}
			postMap, err := p.ParseComponents(value)
			if err != nil {
				return fmt.Errorf("parse [post_process/postProcess] failed, error: %s", err.Error())
			}
			wfs.PostProcess = map[string]*WorkflowSourceStep{}
			for postkey, postValue := range postMap {
				postValue, ok := postValue.(*WorkflowSourceStep)
				if !ok {
					return fmt.Errorf("[post_process/postProcess] can only have step")
				}
				wfs.PostProcess[postkey] = postValue
			}
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
				return nil, err
			}
			dagComp.Name = name
			components[name] = &dagComp
		} else {
			stepComp := WorkflowSourceStep{}
			if err := p.ParseStep(compMap, &stepComp); err != nil {
				return nil, err
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
		case "loopArgument":
			fallthrough
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
					atfValue, ok := atfValue.([]interface{})
					if !ok {
						return fmt.Errorf("[artifacts.output] in step should be list of string type")
					}
					atfMap := map[string]string{}
					for _, atfName := range atfValue {
						atfName, ok := atfName.(string)
						if !ok {
							return fmt.Errorf("[artifacts.output] in step should be list of string type")
						}
						atfMap[atfName] = ""
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
			// 设置在env里的变量优先级最高，如果在Step里设置了如queue、flavour等需要填充到env的字段，会直接被env中对应的值覆盖
			for envKey, envValue := range value {
				resEnv := ""
				switch envValue := envValue.(type) {
				case string:
					resEnv = envValue
				case int64:
					resEnv = strconv.FormatInt(envValue, 10)
				default:
					return fmt.Errorf("values in [env] should be string type")
				}
				step.Env[envKey] = resEnv
			}
		case "dockerEnv":
			fallthrough
		case "docker_env":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[docker_env/dockerEnv] in step should be string type")
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
		case "type":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[type] of step should be string type")
			}
			if value != "step" {
				return fmt.Errorf("set [type] as [%s] in step", value)
			}
		case "flavour":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[flavour] of step should be string type")
			}
			if step.Env == nil {
				step.Env = map[string]string{}
			}
			if _, ok := step.Env[EnvJobFlavour]; !ok {
				step.Env[EnvJobFlavour] = value
			}
		case "jobType":
			fallthrough
		case "job_type":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[job_type/jobType] of step should be string type")
			}
			if step.Env == nil {
				step.Env = map[string]string{}
			}
			if _, ok := step.Env[EnvJobType]; !ok {
				step.Env[EnvJobType] = value
			}
		case "queue":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[queue] of step should be string type")
			}
			if step.Env == nil {
				step.Env = map[string]string{}
			}
			if _, ok := step.Env[EnvJobQueueName]; !ok {
				step.Env[EnvJobQueueName] = value
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
		case "loopArgument":
			fallthrough
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
		case "entryPoints":
			fallthrough
		case "entry_points":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[entry_points/entryPoints] of dag should be map type")
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
			if value != "dag" {
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
		case "maxExpiredTime":
			fallthrough
		case "max_expired_time":
			switch cacheValue := cacheValue.(type) {
			case string:
				cache.MaxExpiredTime = cacheValue
			case int64:
				cache.MaxExpiredTime = strconv.FormatInt(cacheValue, 10)
			default:
				return fmt.Errorf("[cache.max_expired_time/maxExpiredTime] should be string/int64 type")
			}
		case "fsScope":
			fallthrough
		case "fs_scope":
			cacheValue, ok := cacheValue.(string)
			if !ok {
				return fmt.Errorf("[cache.fs_scope/fsScope] should be string type")
			}
			cache.FsScope = cacheValue
		default:
			return fmt.Errorf("[cache] has no attribute [%s]", cacheKey)
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
			if err := p.transJsonSubMap2Yaml(value, "entryPoints"); err != nil {
				return err
			}
			jsonMap["entry_points"] = value
			delete(jsonMap, "entryPoints")
		case "failureOptions":
			jsonMap["failure_options"] = value
			delete(jsonMap, "failureOptions")
		case "postProcess":
			if err := p.transJsonSubMap2Yaml(value, "postProcess"); err != nil {
				return err
			}
			jsonMap["post_process"] = value
			delete(jsonMap, "postProcess")
		case "loopArgument":
			jsonMap["loop_argument"] = value
			delete(jsonMap, "loopArgument")
		case "component":
			if err := p.transJsonSubMap2Yaml(value, "component"); err != nil {
				return err
			}
		case "cache":
			cacheMap, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[cache] should be map type")
			}
			cacheMap["max_expired_time"] = cacheMap["maxExpiredTime"]
			delete(cacheMap, "maxExpiredTime")

			cacheMap["fs_scope"] = cacheMap["fsScope"]
			delete(cacheMap, "fsScope")
		}
	}
	return nil
}

func (p *Parser) transJsonSubMap2Yaml(value interface{}, filedType string) error {
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
