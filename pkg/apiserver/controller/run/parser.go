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

import "paddleflow/pkg/common/schema"

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
			// loop_argument
			dagNode.LoopArgument = nodeMap["loop_argument"]
			condition, ok := nodeMap["condition"].(string)
		}
	}
	return nodes, nil
}

func (p *Parser) ParseDagParam(key string, value interface{}, dagNode *schema.WorkflowSourceDag) (interface{}, error) {
	switch key {
	case "loop_argument":
		return
	}
}

func (p *Parser) isDag(node map[string]interface{}) bool {
	if _, ok := node["entry_points"]; ok {
		return true
	}
	return false
}
