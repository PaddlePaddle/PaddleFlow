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

package v1

import (
	"net/http"
	"os"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/pipeline"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
)

func TestCreatePipeline(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	var err error

	pplUrl := baseUrl + "/pipeline"
	createPplReq := pipeline.CreatePipelineRequest{
		FsName:   "mockFsName",
		UserName: "",
		YamlPath: "../../../../example/wide_and_deep/run.yaml",
		Name:     "mockPplName",
	}

	pipeline.ValidateWorkflowForPipeline = func(ppl models.Pipeline) error { return nil }
	handler.ReadFileFromFs = func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) { return os.ReadFile(runYamlPath) }

	result, err := PerformPostRequest(router, pplUrl, createPplReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, result.Code)
	createPplRsp := pipeline.CreatePipelineResponse{}
	err = ParseBody(result.Body, &createPplRsp)
	assert.Nil(t, err)
	assert.Equal(t, createPplRsp.Name, createPplReq.Name)
	assert.True(t, strings.Contains(createPplRsp.ID, "ppl-"))
}
