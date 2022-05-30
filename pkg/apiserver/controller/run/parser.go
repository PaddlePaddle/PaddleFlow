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

package run

import (
	"fmt"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type Parser struct {
}

func loopArgumentParser(param interface{}) interface{} {
	return param
}

func (p *Parser) ParseNodes(entryPoints map[string]interface{}) (map[string]interface{}, error) {
	nodes := map[string]interface{}{}
	for name, node := range entryPoints {
		nodeMap := node.(map[string]interface{})
		if p.isDag(nodeMap) {
			dagNode := schema.WorkflowSourceDag{}
			if err := p.ParseDag(nodeMap, &dagNode); err != nil {
				return nil, err
			}
			nodes[name] = &dagNode
		} else {
			stepNode := schema.WorkflowSourceStep{}
			if err := p.ParseStep(nodeMap, &stepNode); err != nil {
				return nil, err
			}
			nodes[name] = &stepNode
		}
	}
	return nodes, nil
}

func (p *Parser) ParseStep(params map[string]interface{}, stepNode *schema.WorkflowSourceStep) error {
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
			artifacts := schema.Artifacts{}
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
					artifacts.OutputList = atfValue
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
			stepNode.Env = value
		case "dockerEnv":
			fallthrough
		case "docker_env":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[docker_env/dockerEnv] in step should be string type")
			}
			stepNode.DockerEnv = value
		case "cache":
			cache := schema.Cache{}
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[cache] in step should be map[string]interface type")
			}
			for cacheKey, cacheValue := range value {
				switch cacheKey {
				case "enable":
					cacheValue, ok := cacheValue.(bool)
					if !ok {
						return fmt.Errorf("[cache.enable] in step should be bool type")
					}
					cache.Enable = cacheValue
				case "maxExpiredTime":
					fallthrough
				case "max_expired_time":
					cacheValue, ok := cacheValue.(string)
					if !ok {
						return fmt.Errorf("[cache.max_expired_time/maxExpiredTime] in step should be string type")
					}
					cache.MaxExpiredTime = cacheValue
				case "fsScope":
					fallthrough
				case "fs_scope":
					cacheValue, ok := cacheValue.(string)
					if !ok {
						return fmt.Errorf("[cache.fs_scope/fsScope] in step should be string type")
					}
					cache.FsScope = cacheValue
				default:
					return fmt.Errorf("[cache] of step has no attribute [%s]", cacheKey)
				}
			}
			stepNode.Cache = cache
		case "reference":
			value, ok := value.(string)
			if !ok {
				return fmt.Errorf("[reference] in step should be string type")
			}
			stepNode.Reference = value
		default:
			return fmt.Errorf("step has no attribute [%s]", key)
		}
	}
	return nil
}

// 该函数用于给生成给WorkflowSourceDag的各个字段赋值，但不会进行默认值填充，不会进行全局参数对局部参数的替换
func (p *Parser) ParseDag(params map[string]interface{}, dagNode *schema.WorkflowSourceDag) error {
	for key, value := range params {
		switch key {
		case "loopArgument":
			fallthrough
		case "loop_argument":
			dagNode.LoopArgument = value
		case "conditon":
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
			artifacts := schema.Artifacts{}
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

func (p *Parser) isDag(node map[string]interface{}) bool {
	if _, ok := node["entry_points"]; ok {
		return true
	}
	return false
}
