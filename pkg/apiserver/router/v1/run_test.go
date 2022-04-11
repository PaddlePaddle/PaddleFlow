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
	"testing"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/controller/run"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/apiserver/router/util"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/database/dbinit"
	"paddleflow/pkg/common/logger"
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

func prepareDBAndAPI(t *testing.T) (*chi.Mux, string) {
	chiRouter := NewApiTest()
	baseUrl := util.PaddleflowRouterPrefix + util.PaddleflowRouterVersionV1

	config.GlobalServerConfig = &config.ServerConfig{
		ApiServer: config.ApiServerConfig{
			TokenExpirationHour: -1,
		},
	}

	dbinit.InitMockDB()
	rootCtx := &logger.RequestContext{UserName: MockRootUser}

	token, err := CreateTestUser(rootCtx, MockRootUser, MockPassword)
	assert.Nil(t, err)
	setToken(token)

	return chiRouter, baseUrl
}

func TestGetRunRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	var err error

	ctxroot := &logger.RequestContext{UserName: MockRootUser}
	run1 := getMockRun1()
	run1.ID, err = models.CreateRun(ctxroot.Logging(), &run1)
	assert.Nil(t, err)

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

	result, err := PerformGetRequest(router, runUrl)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)
	runRsp := run.ListRunResponse{}
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
