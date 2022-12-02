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
	"fmt"

	prometheusModel "github.com/prometheus/common/model"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/consts"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/monitor"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

var metricNameList = [...]string{
	consts.MetricCpuUsageRate, consts.MetricMemoryUsageRate,
	consts.MetricMemoryUsage, consts.MetricDiskUsage,
	consts.MetricNetReceiveBytes, consts.MetricNetSendBytes,
	consts.MetricDiskReadRate, consts.MetricDiskWriteRate,
	consts.MetricGpuUtil, consts.MetricGpuMemoryUtil,
	consts.MetricGpuMemoryUsage}

type JobStatisticsResponse struct {
	MetricsInfo map[string]string `json:"metricsInfo"`
}

type JobDetailStatisticsResponse struct {
	Result      []TaskStatistics `json:"result"`
	TaskNameMap map[string]int   `json:"-"`
	Truncated   bool             `json:"truncated"`
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
	response := &JobStatisticsResponse{
		MetricsInfo: make(map[string]string),
	}
	clusterType, _, err := getClusterTypeByJob(ctx, jobID)
	if err != nil {
		ctx.Logging().Errorf("get metric type failed, error: %s", err.Error())
		return nil, err
	}
	metric, err := getMetricByType(clusterType)
	if err != nil {
		ctx.Logging().Errorf("get metric by type[%s] failed, error: %s", clusterType, err.Error())
		return nil, err
	}

	for _, value := range metricNameList {
		result, err := metric.GetJobAvgMetrics(value, jobID)
		if err != nil {
			ctx.Logging().Errorf("query metric[%s] failed, error: %s", value, err.Error())
			return nil, err
		}
		convertResultToResponse(response, result, value)
	}

	return response, nil
}

func GetJobDetailStatistics(ctx *logger.RequestContext, jobID string, start, end, step int64) (*JobDetailStatisticsResponse, error) {
	response := &JobDetailStatisticsResponse{
		Result:      make([]TaskStatistics, 0),
		TaskNameMap: make(map[string]int),
	}

	clusterType, job, err := getClusterTypeByJob(ctx, jobID)
	if err != nil {
		ctx.Logging().Errorf("get metric type failed, error: %s", err.Error())
		return nil, err
	}

	if start == 0 {
		if job.ActivatedAt.Valid {
			start = job.ActivatedAt.Time.Unix()
		} else {
			return response, nil
		}
	}
	if end == 0 {
		if schema.IsImmutableJobStatus(job.Status) {
			end = job.UpdatedAt.Unix()
		} else {
			end = start + 60*10
		}
	}

	metric, err := getMetricByType(clusterType)
	if err != nil {
		ctx.Logging().Errorf("get metric by type[%s] failed, error: %s", clusterType, err.Error())
		return nil, err
	}

	for _, value := range metricNameList {
		result, err := metric.GetJobSequenceMetrics(value, jobID, start, end, step)
		if err != nil {
			ctx.Logging().Errorf("query range metric[%s] failed, error: %s", value, err.Error())
			return nil, err
		}
		err = convertResultToDetailResponse(ctx, result, response, value)
		if err != nil {
			ctx.Logging().Errorf("convert metric[%s] result to detail response failed, error: %s", value, err.Error())
			return nil, err
		}
	}
	return response, nil
}

func getMetricByType(metricType string) (monitor.MetricInterface, error) {
	var metric monitor.MetricInterface
	switch metricType {
	case schema.KubernetesType:
		metric = monitor.NewKubernetesMetric(monitor.PrometheusClientAPI)
	default:
		return nil, fmt.Errorf("metric type[%s] is not support", metricType)
	}
	return metric, nil
}

func getClusterTypeByJob(ctx *logger.RequestContext, jobID string) (string, *model.Job, error) {
	job, err := storage.Job.GetJobByID(jobID)
	if err != nil {
		ctx.ErrorCode = common.JobNotFound
		ctx.Logging().Errorln(err.Error())
		return "", nil, common.NotFoundError(common.ResourceTypeJob, jobID)
	}
	if ok := checkJobPermission(ctx, &job); !ok {
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorf("get the job[%s] auth failed.", jobID)
		return "", nil, common.NoAccessError(ctx.UserName, common.ResourceTypeJob, jobID)
	}

	queue, err := storage.Queue.GetQueueByID(job.QueueID)
	if err != nil {
		ctx.ErrorCode = common.QueueNameNotFound
		ctx.Logging().Errorln(err.Error())
		return "", nil, common.NotFoundError(common.ResourceTypeQueue, job.QueueID)
	}
	cluster, err := storage.Cluster.GetClusterById(queue.ClusterId)
	if err != nil {
		ctx.ErrorCode = common.ClusterNotFound
		ctx.Logging().Errorln(err.Error())
		return "", nil, common.NotFoundError(common.ResourceTypeCluster, queue.ClusterId)
	}
	return cluster.ClusterType, &job, nil
}

func convertResultToDetailResponse(ctx *logger.RequestContext, result prometheusModel.Value, response *JobDetailStatisticsResponse, metricName string) error {
	data, ok := result.(prometheusModel.Matrix)
	if !ok {
		ctx.Logging().Errorf("convert result to matrix failed")
		return fmt.Errorf("convert result to matrix failed")
	}
	for _, value := range data {
		taskValues := make([][2]float64, 0)
		if len(value.Values) > common.StsMaxSeqData {
			value.Values = value.Values[:common.StsMaxSeqData]
			response.Truncated = true
		}
		for _, rangeValue := range value.Values {
			taskValues = append(taskValues, [2]float64{float64(rangeValue.Timestamp.Unix()), float64(rangeValue.Value)})
		}
		taskName := string(value.Metric[common.Pod])
		metricInfo := MetricInfo{
			MetricName: metricName,
			Values:     taskValues,
		}
		if _, ok := response.TaskNameMap[taskName]; ok {
			response.Result[response.TaskNameMap[taskName]].TaskInfo = append(response.Result[response.TaskNameMap[taskName]].TaskInfo, metricInfo)
		} else {
			task := TaskStatistics{
				TaskName: taskName,
				TaskInfo: make([]MetricInfo, 0),
			}
			task.TaskInfo = append(task.TaskInfo, metricInfo)
			response.TaskNameMap[taskName] = len(response.Result)
			response.Result = append(response.Result, task)
		}
	}
	return nil
}

func convertResultToResponse(response *JobStatisticsResponse, result float64, metricName string) {
	switch metricName {
	case consts.MetricCpuUsageRate, consts.MetricMemoryUsageRate, consts.MetricGpuUtil, consts.MetricGpuMemoryUtil:
		response.MetricsInfo[metricName] = fmt.Sprintf("%.2f%%", result*100)
	case consts.MetricNetReceiveBytes, consts.MetricNetSendBytes, consts.MetricDiskReadRate, consts.MetricDiskWriteRate:
		response.MetricsInfo[metricName] = fmt.Sprintf("%.2f(B/s)", result)
	case consts.MetricDiskUsage, consts.MetricMemoryUsage, consts.MetricGpuMemoryUsage:
		response.MetricsInfo[metricName] = fmt.Sprintf("%.2f(Bytes)", result)
	}
}

func checkJobPermission(ctx *logger.RequestContext, job *model.Job) bool {
	return common.IsRootUser(ctx.UserName) || ctx.UserName == job.UserName
}
