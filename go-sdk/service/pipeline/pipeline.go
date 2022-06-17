/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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

package pipeline

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v2"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type Pipeline = schema.WorkflowSource

type Step = schema.WorkflowSourceStep

func NewPipelineFromYamlFile(filepath string) (pipeline *Pipeline, err error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	defer file.Close()
	pipeline, err = NewPipelineFromReader(file)
	return
}

func NewPipelineFromReader(reader io.Reader) (pipeline *Pipeline, err error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	pipeline, err = NewPipelineFromYamlBytes(content)
	return
}

func NewPipelineFromYamlBytes(content []byte) (pipeline *Pipeline, err error) {
	ppl, err := schema.ParseWorkflowSourceWithOutTransOutputArtifact(content)
	if err != nil {
		return nil, err
	}

	pipeline = &ppl

	err = validatePipeline(pipeline)
	if err != nil {
		return nil, err
	}

	return
}

func validatePipeline(pipeline *schema.WorkflowSource) (err error) {
	// TODO: 进行更为详细的校验
	name := pipeline.Name
	if name != "" && !schema.CheckReg(name, common.RegPatternPipelineName) {
		err = fmt.Errorf("validate pipeline name[%s] with pattern[%s] failed", pipeline.Name, common.RegPatternPipelineName)
	}

	return
}

func TransPipelineToYamlRaw(p *Pipeline) (runYamlRaw string, err error) {
	runYaml, err := yaml.Marshal(*p)
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
