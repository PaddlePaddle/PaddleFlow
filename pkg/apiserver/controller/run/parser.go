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

<<<<<<< HEAD
import (
	"fmt"
	"paddleflow/pkg/common/schema"
)
=======
import "paddleflow/pkg/common/schema"
>>>>>>> e68096c4e1996506bcf6e66af49eb98166bc5864

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
<<<<<<< HEAD
			if err := p.ParseDag(nodeMap, &dagNode); err != nil {
				return nil, err
			}
			nodes[name] = &dagNode
		} else {
			stepNode := schema.WorkflowSourceStep{}
		}

=======
			// loop_argument
			dagNode.LoopArgument = nodeMap["loop_argument"]
			condition, ok := nodeMap["condition"].(string)
		}
>>>>>>> e68096c4e1996506bcf6e66af49eb98166bc5864
	}
	return nodes, nil
}

<<<<<<< HEAD
func (p *Parser) TransCacheJson2Yaml()

func (p *Parser) ParseStep(params map[string]interface{}, stepNode *schema.WorkflowSourceStep) error {
	for key, value := range params {
		switch key {
		case "loop_argument":

		}
	}
	return nil
}

// 该函数用于给生成给WorkflowSourceDag的各个字段赋值，但不会进行默认值填充，不会进行全局参数对局部参数的替换
func (p *Parser) ParseDag(params map[string]interface{}, dagNode *schema.WorkflowSourceDag) error {
	for key, value := range params {
		switch key {
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
		case "entry_points":
			value, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("[entry_points] of dag should be map type")
			}
			entryPoints, err := p.ParseNodes(value)
			if err != nil {
				return err
			}
			dagNode.EntryPoints = entryPoints
		default:
			return fmt.Errorf("dag has no attribute [%s]", key)
		}
	}
	return nil
=======
func (p *Parser) ParseDagParam(key string, value interface{}, dagNode *schema.WorkflowSourceDag) (interface{}, error) {
	switch key {
	case "loop_argument":
		return
	}
>>>>>>> e68096c4e1996506bcf6e66af49eb98166bc5864
}

func (p *Parser) isDag(node map[string]interface{}) bool {
	if _, ok := node["entry_points"]; ok {
		return true
	}
	return false
}
