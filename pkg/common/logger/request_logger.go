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

package logger

import (
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
)

type RequestContext struct {
	RequestID    string
	UserID       string
	UserName     string
	GrpcCode     codes.Code
	ErrorCode    string
	ErrorMessage string
}

func (ctx *RequestContext) Logging() *log.Entry {
	return log.WithFields(log.Fields{
		"RequestID": ctx.RequestID,
		"UserName":  ctx.UserName,
	})
}

func LoggerForRequest(ctx *RequestContext) *log.Entry {
	return log.WithFields(log.Fields{
		"RequestID": ctx.RequestID,
		"UserName":  ctx.UserName,
	})
}

func LoggerForJob(jobID string) *log.Entry {
	return log.WithFields(log.Fields{
		"JobID": jobID,
	})
}

func LoggerForRun(runID string) *log.Entry {
	return log.WithFields(log.Fields{
		"RunID": runID,
	})
}

func LoggerForMetric(metricName string) *log.Entry {
	return log.WithFields(log.Fields{
		"metricName": metricName,
	})
}

func Logger() *log.Entry {
	return log.WithFields(log.Fields{})
}
