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
			entryPointsMap, err := p.ParseNodes(value)
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
			componentsMap, err := p.ParseNodes(value)
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
			value, ok := value.(int)
			if !ok {
				return fmt.Errorf("[parallelism] of workflow should be int type")
			}
			wfs.Parallelism = value
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
			postMap, err := p.ParseNodes(value)
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
		}
	}
	return nil
}

func (p *Parser) ParseNodes(entryPoints map[string]interface{}) (map[string]Component, error) {
	nodes := map[string]Component{}
	for name, node := range entryPoints {
		nodeMap := node.(map[string]interface{})
		if p.IsDag(nodeMap) {
			dagNode := WorkflowSourceDag{}
			if err := p.ParseDag(nodeMap, &dagNode); err != nil {
				return nil, err
			}
			nodes[name] = &dagNode
		} else {
			stepNode := WorkflowSourceStep{}
			if err := p.ParseStep(nodeMap, &stepNode); err != nil {
				return nil, err
			}
			nodes[name] = &stepNode
		}
	}
	return nodes, nil
}

func (p *Parser) ParseStep(params map[string]interface{}, stepNode *WorkflowSourceStep) error {
	for key, value := range params {
		switch key {
		case "loopArgument":
			fallthrough
		case "loop_argument":
			stepNode.LoopArgument = value
		case "conditon":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[condition] in step should be string type")
			}
			stepNode.Condition = value
		case "parameters":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[parameters] in step should be map type")
			}
			stepNode.Parameters = value
		case "command":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[command] in step should be string type")
			}
			stepNode.Command = value
		case "deps":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[deps] in step should be string type")
			}
			stepNode.Deps = value
		case "artifacts":
			artifacts := Artifacts{}
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[artifacts] in step should be map type")
			}
			for atfKey, atfValue := range value {
				switch atfKey {
				case "output":
					atfValue, ok := atfValue.([]string)
					if !ok {
						return fmt.Errorf("[artifacts.output] in step should be list of string type")
					}
					atfMap := map[string]string{}
					for _, atfName := range atfValue {
						atfMap[atfName] = ""
					}
					artifacts.Output = atfMap
				case "input":
					atfValue, ok := atfValue.(map[string]string)
					if !ok {
						return fmt.Errorf("[artifacts.output] in step should be map[string]string type")
					}
					artifacts.Input = atfValue
				default:
					return fmt.Errorf("[artifacts] of step has no attribute [%s]", atfKey)
				}
			}
			stepNode.Artifacts = artifacts
		case "env":
			value, ok := value.(map[string]string)
			if !ok {
				return fmt.Errorf("[artifacts] in step should be map type")
			}
			if stepNode.Env == nil {
				stepNode.Env = map[string]string{}
			}
			// 设置在env里的变量优先级最高，如果在Step里设置了如queue、flavour等需要填充到env的字段，会直接被env中对应的值覆盖
			for envKey, envValue := range value {
				stepNode.Env[envKey] = envValue
			}
		case "dockerEnv":
			fallthrough
		case "docker_env":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[docker_env/dockerEnv] in step should be string type")
			}
			stepNode.DockerEnv = value
		case "cache":
			cache := Cache{}
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[cache] in step should be map[string]interface type")
			}
			if err := p.ParseCache(value, &cache); err != nil {
				return fmt.Errorf("parse cache in step failed, error: %s", err.Error())
			}
			stepNode.Cache = cache
		case "reference":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[reference] in step should be string type")
			}
			stepNode.Reference = value
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
			if stepNode.Env == nil {
				stepNode.Env = map[string]string{}
			}
			if _, ok := stepNode.Env[EnvJobFlavour]; !ok {
				stepNode.Env[EnvJobFlavour] = value
			}
		case "jobType":
			fallthrough
		case "job_type":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[job_type/jobType] of step should be string type")
			}
			if stepNode.Env == nil {
				stepNode.Env = map[string]string{}
			}
			if _, ok := stepNode.Env[EnvJobType]; !ok {
				stepNode.Env[EnvJobType] = value
			}
		case "queue":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[queue] of step should be string type")
			}
			if stepNode.Env == nil {
				stepNode.Env = map[string]string{}
			}
			if _, ok := stepNode.Env[EnvJobQueueName]; !ok {
				stepNode.Env[EnvJobQueueName] = value
			}
		default:
			return fmt.Errorf("step has no attribute [%s]", key)
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

// 该函数用于给生成给WorkflowSourceDag的各个字段赋值，但不会进行默认值填充，不会进行全局参数对局部参数的替换
func (p *Parser) ParseDag(params map[string]interface{}, dagNode *WorkflowSourceDag) error {
	for key, value := range params {
		switch key {
		case "loopArgument":
			fallthrough
		case "loop_argument":
			dagNode.LoopArgument = value
		case "condition":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[condition] in dag should be string type")
			}
			dagNode.Condition = value
		case "parameters":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[parameters] in dag should be map type")
			}
			dagNode.Parameters = value
		case "deps":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[deps] in dag should be string type")
			}
			dagNode.Deps = value
		case "artifacts":
			artifacts := Artifacts{}
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[artifacts] in dag should be map type")
			}
			for atfKey, atfValue := range value {
				switch atfKey {
				case "output":
					atfValue, ok := atfValue.(map[string]string)
					if !ok {
						return fmt.Errorf("[artifacts.output] in dag should be map[string]string type")
					}
					artifacts.Output = atfValue
				case "input":
					atfValue, ok := atfValue.(map[string]string)
					if !ok {
						return fmt.Errorf("[artifacts.output] in dag should be map[string]string type")
					}
					artifacts.Input = atfValue
				default:
					return fmt.Errorf("[artifacts] of dag has no attribute [%s]", atfKey)
				}
			}
			dagNode.Artifacts = artifacts
		case "entryPoints":
			fallthrough
		case "entry_points":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[entry_points/entryPoints] of dag should be map type")
			}
			entryPoints, err := p.ParseNodes(value)
			if err != nil {
				return err
			}
			dagNode.EntryPoints = entryPoints
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

func (p *Parser) IsDag(node map[string]interface{}) bool {
	if _, ok := node["entry_points"]; ok {
		return true
	}
	return false
}
