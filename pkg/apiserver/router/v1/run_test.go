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
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/pipeline"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

func getMockRun1() models.Run {
	run1 := models.Run{
		ID:       MockRunID1,
		Name:     MockRunName1,
		UserName: MockRootUser,
		FsName:   MockFsName1,
		FsID:     common.ID(MockRootUser, MockFsName1),
		Status:   common.StatusRunPending,
	}
	return run1
}

func getMockRun1_3() models.Run {
	run1 := models.Run{
		ID:       MockRunID3,
		Name:     "",
		UserName: MockRootUser,
		FsName:   MockFsName1,
		FsID:     common.ID(MockRootUser, MockFsName1),
		Status:   common.StatusRunPending,
	}
	return run1
}

func getMockRun2() models.Run {
	run2 := models.Run{
		ID:       MockRunID2,
		Name:     MockRunName2,
		UserName: MockNormalUser,
		FsName:   MockFsName2,
		FsID:     common.ID(MockNormalUser, MockFsName2),
		Status:   common.StatusRunPending,
	}
	return run2
}

func loadCase(casePath string) []byte {
	data, err := ioutil.ReadFile(casePath)
	if err != nil {
		fmt.Println("File reading error", err)
		return []byte{}
	}
	return data
}

func TestGetRunRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	var err error

	ctxroot := &logger.RequestContext{UserName: MockRootUser}
	run1 := getMockRun1()
	run1.ID, err = models.CreateRun(ctxroot.Logging(), &run1)
	assert.Nil(t, err)

	runTemp := &models.Run{}
	p1 := gomonkey.ApplyPrivateMethod(reflect.TypeOf(runTemp), "decode", func() error {
		return nil
	})
	defer p1.Reset()
	url := baseUrl + "/run/" + run1.ID
	result, err := PerformGetRequest(router, url)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)
	runRsp := models.Run{}
	err = ParseBody(result.Body, &runRsp)
	assert.Nil(t, err)
	assert.Equal(t, run1.ID, runRsp.ID)
	assert.Equal(t, MockRootUser, runRsp.UserName)
}

func TestListRunRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	var err error

	runUrl := baseUrl + "/run"
	ctxroot := &logger.RequestContext{UserName: MockRootUser}
	ctx2 := &logger.RequestContext{UserName: MockNormalUser}

	run1 := getMockRun1()
	run1.ID, err = models.CreateRun(ctxroot.Logging(), &run1)
	run2 := getMockRun2()
	run2.ID, err = models.CreateRun(ctx2.Logging(), &run2)
	run3UnderUser1 := getMockRun1_3()
	run1.ID, err = models.CreateRun(ctxroot.Logging(), &run3UnderUser1)

	runTemp := &models.Run{}
	p1 := gomonkey.ApplyPrivateMethod(reflect.TypeOf(runTemp), "decode", func() error {
		return nil
	})
	defer p1.Reset()

	result, err := PerformGetRequest(router, runUrl)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)
	runRsp := pipeline.ListRunResponse{}
	err = ParseBody(result.Body, &runRsp)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(runRsp.RunList))

	// with filters
	filters := "?" + util.QueryKeyFsFilter + "=" + MockFsName1
	result, err = PerformGetRequest(router, runUrl+filters)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)
	err = ParseBody(result.Body, &runRsp)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(runRsp.RunList))
	assert.Equal(t, MockFsName1, runRsp.RunList[0].FsName)
}

func TestCreateRunRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)

	runUrl := baseUrl + "/run"
	req := pipeline.CreateRunRequest{}

	patch := gomonkey.ApplyFunc(common.BindJSON, func(r *http.Request, data interface{}) error {
		data = &pipeline.CreateRunRequest{}
		return nil
	})
	defer patch.Reset()

	patch1_1 := gomonkey.ApplyFunc(pipeline.CreateRun,
		func(ctx *logger.RequestContext, request *pipeline.CreateRunRequest, extra map[string]string) (pipeline.CreateRunResponse, error) {
			return pipeline.CreateRunResponse{}, nil
		})
	defer patch1_1.Reset()

	res, _ := PerformPostRequest(router, runUrl, req)
	assert.Equal(t, res.Code, http.StatusCreated)

	patch1 := gomonkey.ApplyFunc(pipeline.CreateRun,
		func(ctx *logger.RequestContext, request *pipeline.CreateRunRequest, extra map[string]string) (pipeline.CreateRunResponse, error) {
			ctx.ErrorCode = common.InvalidPipeline
			return pipeline.CreateRunResponse{}, fmt.Errorf("patch error")
		})
	defer patch1.Reset()

	res, _ = PerformPostRequest(router, runUrl, req)
	assert.Equal(t, res.Code, http.StatusBadRequest)

	patch2 := gomonkey.ApplyFunc(pipeline.CreateRun,
		func(ctx *logger.RequestContext, request *pipeline.CreateRunRequest, extra map[string]string) (pipeline.CreateRunResponse, error) {
			ctx.ErrorCode = common.InvalidPipeline
			return pipeline.CreateRunResponse{}, fmt.Errorf("patch2 error")
		})
	defer patch2.Reset()
	res, _ = PerformPostRequest(router, runUrl, req)
	assert.Equal(t, res.Code, http.StatusBadRequest)

	patch3 := gomonkey.ApplyFunc(common.BindJSON, func(*http.Request, interface{}) error {
		return fmt.Errorf("patch3 error")
	})
	defer patch3.Reset()

	res, _ = PerformPostRequest(router, runUrl, req)
	assert.Equal(t, http.StatusBadRequest, res.Code)
}

func TestCreateRunByJsonRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	jsonPath := "../../controller/pipeline/testcase/run_dag.json"
	jsonByte := loadCase(jsonPath)

	runUrl := baseUrl + "/runjson"

	patch3 := gomonkey.ApplyFunc(json.Valid, func([]byte) bool {
		return false
	})
	defer patch3.Reset()

	res, _ := PerformPostRequest(router, runUrl, jsonByte)
	assert.Equal(t, res.Code, http.StatusBadRequest)

	patch4 := gomonkey.ApplyFunc(json.Valid, func([]byte) bool {
		return true
	})
	defer patch4.Reset()

	var us *unstructured.Unstructured
	patch5 := gomonkey.ApplyMethod(reflect.TypeOf(us), "UnmarshalJSON", func(*unstructured.Unstructured, []byte) error {
		return fmt.Errorf("patch5 error")
	})
	defer patch5.Reset()

	res, _ = PerformPostRequest(router, runUrl, jsonByte)
	assert.Equal(t, res.Code, http.StatusBadRequest)

	patch6 := gomonkey.ApplyMethod(reflect.TypeOf(us), "UnmarshalJSON", func(*unstructured.Unstructured, []byte) error {
		return nil
	})
	defer patch6.Reset()

	patch7 := gomonkey.ApplyFunc(pipeline.CreateRunByJson,
		func(ctx *logger.RequestContext, bodyMap map[string]interface{}) (pipeline.CreateRunResponse, error) {
			ctx.ErrorCode = common.InvalidPipeline
			return pipeline.CreateRunResponse{}, fmt.Errorf("patch7 error")
		})
	defer patch7.Reset()
	res, _ = PerformPostRequest(router, runUrl, map[string]string{})
	assert.Equal(t, res.Code, http.StatusBadRequest)

	patch8 := gomonkey.ApplyFunc(pipeline.CreateRunByJson,
		func(ctx *logger.RequestContext, bodyMap map[string]interface{}) (pipeline.CreateRunResponse, error) {
			return pipeline.CreateRunResponse{}, nil
		})
	defer patch8.Reset()

	res, _ = PerformPostRequest(router, runUrl, map[string]string{})
	assert.Equal(t, res.Code, http.StatusCreated)

	patch9 := gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
		return []byte{}, fmt.Errorf("patch9 error")
	})
	defer patch9.Reset()
	res, _ = PerformPostRequest(router, runUrl, map[string]string{})
	assert.Equal(t, res.Code, http.StatusBadRequest)
}

func TestUpdateRunRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	var err error

	ctxroot := &logger.RequestContext{UserName: MockRootUser}
	run1 := getMockRun1()
	run1.ID, err = models.CreateRun(ctxroot.Logging(), &run1)
	assert.Nil(t, err)

	url := baseUrl + "/run/" + run1.ID + "?action=stop23"
	res, _ := PerformPutRequest(router, url, nil)
	assert.Equal(t, res.Code, http.StatusBadRequest)

	url = baseUrl + "/run/" + run1.ID + "?action=stop"
	patch := gomonkey.ApplyFunc(pipeline.StopRun, func(ctx *logger.RequestContext, userName, runID string, request pipeline.UpdateRunRequest) error {
		ctx.ErrorCode = common.InvalidArguments
		return fmt.Errorf("patch error")
	})
	defer patch.Reset()

	res, _ = PerformPutRequest(router, url, nil)
	assert.Equal(t, res.Code, http.StatusBadRequest)

	patch2 := gomonkey.ApplyFunc(pipeline.StopRun, func(ctx *logger.RequestContext, userName, runID string, request pipeline.UpdateRunRequest) error {
		ctx.ErrorCode = ""
		return nil
	})
	defer patch2.Reset()
	res, _ = PerformPutRequest(router, url, nil)
	assert.Equal(t, res.Code, http.StatusOK)

	res, _ = PerformPutRequest(router, url, "abcdefe")
	assert.Equal(t, res.Code, http.StatusBadRequest)

	url = baseUrl + "/run/" + run1.ID + "?action=retry"
	patch3 := gomonkey.ApplyFunc(pipeline.RetryRun, func(ctx *logger.RequestContext, runID string) (string, error) {
		ctx.ErrorCode = common.InvalidArguments
		return "run-01", fmt.Errorf("patch3 error")
	})
	defer patch3.Reset()
	res, _ = PerformPutRequest(router, url, nil)
	assert.Equal(t, res.Code, http.StatusBadRequest)

	patch4 := gomonkey.ApplyFunc(pipeline.RetryRun, func(ctx *logger.RequestContext, runID string) (string, error) {
		return "run-01", nil
	})
	defer patch4.Reset()
	res, _ = PerformPutRequest(router, url, nil)
	assert.Equal(t, res.Code, http.StatusOK)

	patch9 := gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
		return []byte{}, fmt.Errorf("patch9 error")
	})
	defer patch9.Reset()
	res, _ = PerformPutRequest(router, url, nil)
	assert.Equal(t, res.Code, http.StatusBadRequest)

}
