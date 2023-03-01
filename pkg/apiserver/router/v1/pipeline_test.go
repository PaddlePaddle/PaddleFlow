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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/pipeline"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	pkgPipeline "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
)

func TestCreatePipelineRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	config.GlobalServerConfig.Metrics.Enable = true
	var err error

	pplUrl := baseUrl + "/pipeline"
	createPplReq := pipeline.CreatePipelineRequest{
		FsName:   "mockFsName",
		UserName: "",
		YamlPath: "../../../../example/wide_and_deep/run.yaml",
	}

	patch := gomonkey.ApplyFunc(pkgPipeline.NewWorkflow, func(wfSource schema.WorkflowSource, runID string, params map[string]interface{}, extra map[string]string,
		callbacks pkgPipeline.WorkflowCallbacks) (*pkgPipeline.Workflow, error) {
		return &pkgPipeline.Workflow{}, nil
	})
	defer patch.Reset()

	patch1 := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return os.ReadFile(runYamlPath)
	})
	defer patch1.Reset()

	patch2 := gomonkey.ApplyFunc(pipeline.CheckFsAndGetID, func(*logger.RequestContext, string, string) (string, error) {
		return "", nil
	})

	defer patch2.Reset()

	result, err := PerformPostRequest(router, pplUrl, createPplReq)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, result.Code)
	createPplRsp := pipeline.CreatePipelineResponse{}
	err = ParseBody(result.Body, &createPplRsp)
	assert.Nil(t, err)
	assert.True(t, strings.Contains(createPplRsp.PipelineID, "ppl-"))

	b, _ := json.Marshal(createPplRsp)
	println("")
	fmt.Printf("%s\n", b)

	patch3 := gomonkey.ApplyFunc(common.BindJSON, func(*http.Request, interface{}) error {
		return fmt.Errorf("patch3 error")
	})
	defer patch3.Reset()

	res, _ := PerformPostRequest(router, pplUrl, createPplReq)
	assert.Equal(t, http.StatusBadRequest, res.Code)
}

func TestListPipelineRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	config.GlobalServerConfig.Metrics.Enable = true
	var err error
	pplUrl := baseUrl + "/pipeline"

	patch := gomonkey.ApplyFunc(pipeline.ListPipeline, func(ctx *logger.RequestContext, marker string, maxKeys int, userFilter []string, nameFilter []string) (pipeline.ListPipelineResponse, error) {
		return pipeline.ListPipelineResponse{}, nil
	})
	defer patch.Reset()

	_, err = PerformGetRequest(router, pplUrl)
	assert.Nil(t, err)

}

func TestUpdatePipelineRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	config.GlobalServerConfig.Metrics.Enable = true
	var err error
	pplUrl := baseUrl + "/pipeline/ppl-01"

	req := pipeline.UpdatePipelineRequest{
		FsName:   "mockFsName",
		UserName: "",
		YamlPath: "../../../../example/wide_and_deep/run.yaml",
	}

	patch := gomonkey.ApplyFunc(pipeline.UpdatePipeline, func(ctx *logger.RequestContext, request pipeline.CreatePipelineRequest, pipelineID string) (pipeline.UpdatePipelineResponse, error) {
		return pipeline.UpdatePipelineResponse{}, nil
	})
	defer patch.Reset()

	_, err = PerformPostRequest(router, pplUrl, req)
	assert.Nil(t, err)
}

func TestGetPipelineRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	config.GlobalServerConfig.Metrics.Enable = true
	var err error
	pplUrl := baseUrl + "/pipeline"
	pplID := "ppl-001"

	patch := gomonkey.ApplyFunc(pipeline.GetPipeline, func(ctx *logger.RequestContext, pipelineID string, marker string, maxKeys int, fsFilter []string) (pipeline.GetPipelineResponse, error) {
		return pipeline.GetPipelineResponse{}, nil
	})
	defer patch.Reset()

	_, err = PerformGetRequest(router, pplUrl+"/"+pplID)
	assert.Nil(t, err)
}

func TestDeletePipelineRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	config.GlobalServerConfig.Metrics.Enable = true
	var err error
	pplUrl := baseUrl + "/pipeline"
	pplID := "ppl-001"

	patch := gomonkey.ApplyFunc(pipeline.DeletePipeline, func(ctx *logger.RequestContext, pipelineID string) error {
		return nil
	})
	defer patch.Reset()

	_, err = PerformDeleteRequest(router, pplUrl+"/"+pplID)
	assert.Nil(t, err)
}

func TestGetPipelineVersionRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	config.GlobalServerConfig.Metrics.Enable = true
	var err error
	pplUrl := baseUrl + "/pipeline/ppl-01/01"

	patch := gomonkey.ApplyFunc(pipeline.GetPipelineVersion, func(ctx *logger.RequestContext, pipelineID string, pipelineVersionID string) (pipeline.GetPipelineVersionResponse, error) {
		return pipeline.GetPipelineVersionResponse{}, nil
	})
	defer patch.Reset()

	_, err = PerformGetRequest(router, pplUrl)
	assert.Nil(t, err)
}

func TestDeletePipelineVersionRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	config.GlobalServerConfig.Metrics.Enable = true
	var err error
	pplUrl := baseUrl + "/pipeline/ppl01/01"

	patch := gomonkey.ApplyFunc(pipeline.DeletePipelineVersion, func(ctx *logger.RequestContext, pipelineID string, pipelineVersionID string) error {
		return nil
	})
	defer patch.Reset()

	_, err = PerformDeleteRequest(router, pplUrl)
	assert.Nil(t, err)
}
