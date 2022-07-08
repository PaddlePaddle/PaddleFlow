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

package v1

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/statistics"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
)

type StatisticsRouter struct{}

func (sr *StatisticsRouter) Name() string {
	return "StatisticsRouter"
}

func (sr *StatisticsRouter) AddRouter(r chi.Router) {
	log.Info("add statistics router")

	r.Get("/statistics/job/{jobID}", sr.getJobStatistics)
	r.Get("/statistics/jobDetail/{jobID}", sr.getJobDetailStatistics)

}

func (sr *StatisticsRouter) getJobStatistics(writer http.ResponseWriter, request *http.Request) {
	ctx := common.GetRequestContext(request)
	jobID := chi.URLParam(request, util.ParamKeyJobID)
	response, err := statistics.GetJobStatistics(&ctx, jobID)
	if err != nil {
		ctx.Logging().Errorf("jobID[%s] get statistics data failed. error:%s.", jobID, err.Error())
		common.RenderErrWithMessage(writer, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(writer, http.StatusOK, response)
}

func (sr *StatisticsRouter) getJobDetailStatistics(writer http.ResponseWriter, request *http.Request) {
	ctx := common.GetRequestContext(request)
	jobID := chi.URLParam(request, util.ParamKeyJobID)
	startStr := request.URL.Query().Get(util.ParamKeyStart)
	endStr := request.URL.Query().Get(util.ParamKeyEnd)
	stepStr := request.URL.Query().Get(util.ParamKeyStep)
	var start, end, step int64
	var err error
	if startStr == "" {
		start = 0
	} else {
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			ctx.Logging().Errorf("invalid request param start, error:%s.", err.Error())
			common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
			return
		}
	}
	if endStr == "" {
		end = 0
	} else {
		end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			ctx.Logging().Errorf("invalid request param end, error:%s.", err.Error())
			common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
			return
		}
	}
	if stepStr == "" {
		step = 60
	} else {
		step, err = strconv.ParseInt(stepStr, 10, 64)
		if err != nil {
			ctx.Logging().Errorf("invalid request param step, error:%s.", err.Error())
			common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
			return
		}
	}
	err = validateStatisticsParam(start, end, step)
	if err != nil {
		ctx.Logging().Errorf("invalid request param, error:%s.", err.Error())
		common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}
	response, err := statistics.GetJobDetailStatistics(&ctx, jobID, start, end, step)
	if err != nil {
		ctx.Logging().Errorf("jobID[%s] get detail statistics data failed. error:%s.", jobID, err.Error())
		common.RenderErrWithMessage(writer, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(writer, http.StatusOK, response)
}

func validateStatisticsParam(start, end, step int64) error {
	if start > end {
		return common.InvalidStartEndParams()
	}
	if step <= 0 {
		return common.InvalidStatisticsParams("step")
	}
	if start < 0 {
		return common.InvalidStatisticsParams("start")
	}
	if end < 0 {
		return common.InvalidStatisticsParams("end")
	}
	return nil
}
