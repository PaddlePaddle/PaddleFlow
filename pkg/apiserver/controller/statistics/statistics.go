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

package statistics

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/consts"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/monitor"
)

var metricNameList = [...]string{consts.MetricCpuUsageRate, consts.MetricMemoryUsage, consts.MetricDiskUsage,
	consts.MetricNetReceiveBytes, consts.MetricNetSendBytes, consts.MetricDiskReadRate,
	consts.MetricDiskWriteRate, consts.MetricGpuUtil, consts.MetricGpuMemoryUtil}

type JobStatisticsResponse struct {
	CpuUsageRate    float64 `json:"cpu_usage_rate"`
	MemoryUsage     string  `json:"memory_usage"`
	NetReceiveBytes float64 `json:"net_receive_bytes"`
	NetSendBytes    float64 `json:"net_send_bytes"`
	DiskUsageBytes  float64 `json:"disk_usage_bytes"`
	DiskReadRate    float64 `json:"disk_read_rate"`
	DiskWriteRate   float64 `json:"disk_write_rate"`
	GpuUtil         float64 `json:"gpu_util"`
	GpuMemoryUtil   float64 `json:"gpu_memory_util"`
}

type JobDetailStatisticsResponse struct {
	Result      []TaskStatistics `json:"result"`
	TaskNameMap map[string]int   `json:"-"`
}

type TaskStatistics struct {
	TaskName string       `json:"taskName"`
	TaskInfo []MetricInfo `json:"taskInfo"`
}

type MetricInfo struct {
	MetricName string       `json:"metric"`
	Values     [][2]float64 `json:"values"`
}

func GetJobStatistics(ctx *logger.RequestContext, jobID string) (*JobStatisticsResponse, error) {
	response := &JobStatisticsResponse{}
	_, err := models.GetJobByID(jobID)
	if err != nil {
		ctx.ErrorCode = common.JobNotFound
		ctx.Logging().Errorln(err.Error())
		return nil, common.NotFoundError(common.ResourceTypeJob, jobID)
	}
	ctxP, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, value := range metricNameList {
		result, err := queryResourceMetric(ctxP, jobID, value)
		if err != nil {
			ctx.Logging().Errorf("query metric[%s] failed, error: %s", value, err.Error())
			return nil, err
		}
		err = convertResultToResponse(result, response)
		if err != nil {
			ctx.Logging().Errorf("convert metric[%s] result to response failed, error: %s", value, err.Error())
			return nil, err
		}
	}

	return response, nil
}

func GetJobDetailStatistics(ctx *logger.RequestContext, jobID string, start, end, step int64) (*JobDetailStatisticsResponse, error) {
	response := &JobDetailStatisticsResponse{
		Result:      make([]TaskStatistics, 1),
		TaskNameMap: make(map[string]int),
	}

	job, err := models.GetJobByID(jobID)
	if err != nil {
		ctx.ErrorCode = common.JobNotFound
		ctx.Logging().Errorln(err.Error())
		return nil, common.NotFoundError(common.ResourceTypeJob, jobID)
	}

	if start == 0 {
		if job.ActivatedAt.Valid {
			start = job.UpdatedAt.Unix()
		}
	}
	if end == 0 {
		if schema.IsImmutableJobStatus(job.Status) {
			end = job.UpdatedAt.Unix()
		} else {
			end = start + 60*10
		}
	}
	ctxP, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, value := range metricNameList {
		result, err := queryRangeResourceMetric(ctxP, jobID, value, start, end, step)
		if err != nil {
			ctx.Logging().Errorf("query range metric[%s] failed, error: %s", value, err.Error())
			return nil, err
		}
		err = convertResultToDetailResponse(result, response)
		if err != nil {
			ctx.Logging().Errorf("convert metric[%s] result to detail response failed, error: %s", value, err.Error())
			return nil, err
		}
	}
	return response, nil
}

func convertResultToDetailResponse(result model.Value, response *JobDetailStatisticsResponse) error {
	data := result.(model.Matrix)
	for _, value := range data {
		taskValues := make([][2]float64, 1)
		for _, rangeValue := range value.Values {
			taskValues = append(taskValues, [2]float64{float64(rangeValue.Timestamp.Unix()), float64(rangeValue.Value)})
		}
		taskName := string(value.Metric[common.PodID])
		metricInfo := MetricInfo{
			MetricName: string(value.Metric[model.MetricNameLabel]),
			Values:     taskValues,
		}
		if _, ok := response.TaskNameMap[taskName]; ok {
			response.Result[response.TaskNameMap[taskName]].TaskInfo = append(response.Result[response.TaskNameMap[taskName]].TaskInfo, metricInfo)
		} else {
			task := TaskStatistics{
				TaskName: taskName,
				TaskInfo: make([]MetricInfo, 1),
			}
			task.TaskInfo = append(task.TaskInfo, metricInfo)
			response.Result = append(response.Result, task)
			response.TaskNameMap[taskName] = len(response.Result)
		}
	}
	return nil
}

func queryResourceMetric(ctx context.Context, jobID, metricName string) (model.Value, error) {
	queryPromql := fmt.Sprintf("avg(%s{jobID=\"%s\"}) without (podID)", metricName, jobID)
	result, _, err := monitor.PrometheusClientAPI.Query(ctx, queryPromql, time.Now())
	if err != nil {
		log.Errorf("job[%s] prometheus query api error %s", jobID, err.Error())
		return nil, err
	}
	return result, nil
}

func queryRangeResourceMetric(ctx context.Context, jobID, metricName string, start, end, step int64) (model.Value, error) {
	queryPromql := fmt.Sprintf("%s{jobID=\"%s\"}", metricName, jobID)
	r := v1.Range{
		Start: time.Unix(start, 0),
		End:   time.Unix(end, 0),
		Step:  time.Duration(step) * time.Second,
	}
	result, _, err := monitor.PrometheusClientAPI.QueryRange(ctx, queryPromql, r)
	if err != nil {
		log.Errorf("job[%s] prometheus query range api error %s", jobID, err.Error())
		return nil, err
	}
	return result, nil
}

func convertResultToResponse(result model.Value, response *JobStatisticsResponse) error {
	data := result.(model.Vector)
	if len(data) > 0 {
		switch data[0].Metric[model.MetricNameLabel] {
		case consts.MetricCpuUsageRate:
			response.CpuUsageRate = float64(data[0].Value)
		case consts.MetricMemoryUsage:
			response.MemoryUsage = data[0].Value.String()
		case consts.MetricDiskUsage:
			response.DiskUsageBytes = float64(data[0].Value)
		case consts.MetricNetReceiveBytes:
			response.NetReceiveBytes = float64(data[0].Value)
		case consts.MetricNetSendBytes:
			response.NetSendBytes = float64(data[0].Value)
		case consts.MetricDiskReadRate:
			response.DiskReadRate = float64(data[0].Value)
		case consts.MetricDiskWriteRate:
			response.DiskWriteRate = float64(data[0].Value)
		case consts.MetricGpuUtil:
			response.GpuUtil = float64(data[0].Value)
		case consts.MetricGpuMemoryUtil:
			response.GpuMemoryUtil = float64(data[0].Value)
		}
	}
	return nil

}
