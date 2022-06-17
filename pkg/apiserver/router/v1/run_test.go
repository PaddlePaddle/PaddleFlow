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
	"github.com/PaddlePaddle/PaddleFlow/pkg/service/db_service"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/run"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

func getMockRun1() db_service.Run {
	run1 := db_service.Run{
		ID:       MockRunID1,
		Name:     MockRunName1,
		UserName: MockRootUser,
		FsName:   MockFsName1,
		FsID:     common.ID(MockRootUser, MockFsName1),
		Status:   common.StatusRunPending,
	}
	return run1
}

func getMockRun1_3() db_service.Run {
	run1 := db_service.Run{
		ID:       MockRunID3,
		Name:     "",
		UserName: MockRootUser,
		FsName:   MockFsName1,
		FsID:     common.ID(MockRootUser, MockFsName1),
		Status:   common.StatusRunPending,
	}
	return run1
}

func getMockRun2() db_service.Run {
	run2 := db_service.Run{
		ID:       MockRunID2,
		Name:     MockRunName2,
		UserName: MockNormalUser,
		FsName:   MockFsName2,
		FsID:     common.ID(MockNormalUser, MockFsName2),
		Status:   common.StatusRunPending,
	}
	return run2
}

func TestGetRunRouter(t *testing.T) {
	router, baseUrl := prepareDBAndAPI(t)
	var err error

	ctxroot := &logger.RequestContext{UserName: MockRootUser}
	run1 := getMockRun1()
	run1.ID, err = db_service.CreateRun(ctxroot.Logging(), &run1)
	assert.Nil(t, err)

	url := baseUrl + "/run/" + run1.ID
	result, err := PerformGetRequest(router, url)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, result.Code)
	runRsp := db_service.Run{}
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
	run1.ID, err = db_service.CreateRun(ctxroot.Logging(), &run1)
	run2 := getMockRun2()
	run2.ID, err = db_service.CreateRun(ctx2.Logging(), &run2)
	run3UnderUser1 := getMockRun1_3()
	run1.ID, err = db_service.CreateRun(ctxroot.Logging(), &run3UnderUser1)

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
